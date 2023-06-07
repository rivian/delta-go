// Copyright 2023 Rivian Automotive, Inc.
// Licensed under the Apache License, Version 2.0 (the “License”);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an “AS IS” BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package delta

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/rivian/delta-go/lock"
	"github.com/rivian/delta-go/state"
	"github.com/rivian/delta-go/storage"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/maps"
)

const DELTA_CLIENT_VERSION = "alpha-0.0.0"

var (
	ErrorDeltaTable                  error = errors.New("failed to apply transaction log")
	ErrorRetrieveLockBytes           error = errors.New("failed to retrieve bytes from lock")
	ErrorLockDataEmpty               error = errors.New("lock data is empty")
	ErrorExceededCommitRetryAttempts error = errors.New("exceeded commit retry attempts")
	ErrorNotATable                   error = errors.New("not a Delta table")
	ErrorInvalidVersion              error = errors.New("invalid version")
	ErrorLockFailed                  error = errors.New("lock failed unexpectedly without an error")
	ErrorNotImplemented              error = errors.New("not implemented")
)

var (
	commitFileRegex         *regexp.Regexp = regexp.MustCompile(`(?P<Version>\d{20}).json`)
	checkpointRegex         *regexp.Regexp = regexp.MustCompile(`(?P<Version>\d{20})\.checkpoint\.parquet$`)
	checkpointPartsRegex    *regexp.Regexp = regexp.MustCompile(`(?P<Version>\d{20})\.checkpoint\.(?P<CurrentPart>\d{10})\.(?P<Parts>\d{10})\.parquet$`)
	commitOrCheckpointRegex *regexp.Regexp = regexp.MustCompile(`(\d{20})\.((json)|(checkpoint))`)
)

type DeltaTable[RowType any, PartitionType any] struct {
	// The state of the table as of the most recent loaded Delta log entry.
	State DeltaTableState[RowType, PartitionType]
	// The remote store of the state of the table as of the most recent loaded Delta log entry.
	StateStore state.StateStore
	// object store to access log and data files
	Store storage.ObjectStore
	// Locking client to ensure optimistic locked commits from distributed workers
	LockClient lock.Locker
	// // file metadata for latest checkpoint
	LastCheckPoint *CheckPoint
	// table versions associated with timestamps
	VersionTimestamp map[DeltaDataTypeVersion]time.Time
}

// Create a new Delta Table struct without loading any data from backing storage.
//
// NOTE: This is for advanced users. If you don't know why you need to use this method, please
// call one of the `open_table` helper methods instead.
func NewDeltaTable[RowType any, PartitionType any](store storage.ObjectStore, lock lock.Locker, stateStore state.StateStore) *DeltaTable[RowType, PartitionType] {
	table := new(DeltaTable[RowType, PartitionType])
	table.Store = store
	table.StateStore = stateStore
	table.LockClient = lock
	table.LastCheckPoint = nil
	table.State = *NewDeltaTableState[RowType, PartitionType](-1)
	return table
}

// Creates a new DeltaTransaction for the DeltaTable.
// The transaction holds a mutable reference to the DeltaTable, preventing other references
// until the transaction is dropped.
func (table *DeltaTable[RowType, PartitionType]) CreateTransaction(options *DeltaTransactionOptions) *DeltaTransaction[RowType, PartitionType] {
	return NewDeltaTransaction(table, options)
}

// / Return the uri of commit version.
func (table *DeltaTable[RowType, PartitionType]) CommitUriFromVersion(version state.DeltaDataTypeVersion) *storage.Path {
	str := fmt.Sprintf("%020d.json", version)
	path := storage.PathFromIter([]string{"_delta_log", str})
	return &path
}

// / The base path of commit uri's
func BaseCommitUri() *storage.Path {
	return storage.NewPath("_delta_log/")
}

// / Return true if URI is a valid commit filename (not a checkpoint file, and not a temp commit)
func IsValidCommitUri(path *storage.Path) bool {
	match := commitFileRegex.MatchString(path.Base())
	return match
}

// / Return true plus the version if the URI is a valid commit filename
func CommitVersionFromUri(path *storage.Path) (bool, state.DeltaDataTypeVersion) {
	groups := commitFileRegex.FindStringSubmatch(path.Base())
	if len(groups) == 2 {
		version, err := strconv.ParseInt(groups[1], 10, 64)
		if err == nil {
			return true, state.DeltaDataTypeVersion(version)
		}
	}
	return false, 0
}

// / Return true plus the version if the URI is a valid commit or checkpoint filename
func CommitOrCheckpointVersionFromUri(path *storage.Path) (bool, state.DeltaDataTypeVersion) {
	groups := commitOrCheckpointRegex.FindStringSubmatch(path.Base())
	if len(groups) == 5 {
		version, err := strconv.ParseInt(groups[1], 10, 64)
		if err == nil {
			return true, state.DeltaDataTypeVersion(version)
		}
	}
	return false, 0
}

// / Create a DeltaTable with version 0 given the provided MetaData, Protocol, and CommitInfo
func (table *DeltaTable[RowType, PartitionType]) Create(metadata DeltaTableMetaData, protocol Protocol, commitInfo CommitInfo, addActions []Add[RowType, PartitionType]) error {
	meta := metadata.ToMetaData()

	// delta-rs commit info will include the delta-rs version and timestamp as of now
	enrichedCommitInfo := maps.Clone(commitInfo)
	enrichedCommitInfo["clientVersion"] = fmt.Sprintf("delta-go.%s", DELTA_CLIENT_VERSION)
	enrichedCommitInfo["timestamp"] = time.Now().UnixMilli()

	actions := []Action{
		enrichedCommitInfo,
		protocol,
		meta,
	}

	for _, add := range addActions {
		actions = append(actions, add)
	}

	transaction := table.CreateTransaction(nil)
	transaction.AddActions(actions)

	preparedCommit, err := transaction.PrepareCommit(nil, nil)
	if err != nil {
		return err
	}
	//Set StateStore Version=-1 synced with the table State Version
	zeroState := state.CommitState{
		Version: table.State.Version,
	}
	transaction.DeltaTable.StateStore.Put(zeroState)
	err = transaction.TryCommit(&preparedCommit)
	if err != nil {
		return err
	}

	// Merge state from new commit version
	newState, err := NewDeltaTableStateFromCommit(table, table.State.Version)
	if err != nil {
		return err
	}
	table.State.merge(newState)
	return nil
}

// / Exists checks if a DeltaTable with version 0 exists in the object store.
func (table *DeltaTable[RowType, PartitionType]) Exists() (bool, error) {
	path := table.CommitUriFromVersion(0)

	meta, err := table.Store.Head(path)
	if errors.Is(err, storage.ErrorObjectDoesNotExist) {
		// Fallback: check for other variants of the version
		basePath := BaseCommitUri()
		// Do not use paged result; if we aren't seeing a version file in the
		// first page of results, it's very unlikely that this is a commit log folder
		results, err := table.Store.List(basePath, nil)
		if err != nil {
			return false, err
		}
		for _, result := range results.Objects {
			// Check each result to see if it is a version file
			isValidCommitUri := IsValidCommitUri(&result.Location)

			if isValidCommitUri {
				return true, nil
			}
		}
		return false, nil
	}

	// Object should have size
	if meta.Size > 0 {
		return true, nil
	}

	// other errors --> object does not exist
	if err != nil {
		return false, err
	}

	// no errors -> assume object exists
	return true, nil
}

// / Read a commit log and return the actions from the log
func (table *DeltaTable[RowType, PartitionType]) ReadCommitVersion(version state.DeltaDataTypeVersion) ([]Action, error) {
	path := table.CommitUriFromVersion(0)
	return ReadCommitLog[RowType, PartitionType](table.Store, path)
}

// / Load the table state at the latest version
func (table *DeltaTable[RowType, PartitionType]) Load() error {
	return table.LoadVersion(nil)
}

// / Load the table state at the specified version
func (table *DeltaTable[RowType, PartitionType]) LoadVersion(version *state.DeltaDataTypeVersion) error {
	table.LastCheckPoint = nil
	table.State = *NewDeltaTableState[RowType, PartitionType](-1)

	var err error
	if version != nil {
		commitURI := table.CommitUriFromVersion(*version)
		_, err := table.Store.Head(commitURI)
		if errors.Is(err, storage.ErrorObjectDoesNotExist) {
			return ErrorInvalidVersion
		}
		if err != nil {
			return err
		}
	}
	checkpoints, allReturned, err := table.findLatestCheckpointsForVersion(version)
	if err != nil {
		return err
	}

	// Attempt to load the most recent checkpoint; fall back as needed
	for {
		if len(checkpoints) == 0 {
			break
		}

		// Checkpoints are sorted ascending
		checkpointIndex := len(checkpoints) - 1
		err = table.restoreCheckpoint(&checkpoints[checkpointIndex])
		if err == nil {
			break
		}

		if allReturned {
			// Pop the last checkpoint
			checkpoints = checkpoints[:checkpointIndex]
		} else {
			// We didn't retrieve all checkpoints, so look for any earlier than the one that just failed
			prevVersion := checkpoints[checkpointIndex].Version - 1
			if prevVersion > 0 {
				checkpoints, allReturned, err = table.findLatestCheckpointsForVersion(&prevVersion)
				if err != nil {
					return err
				}
			}
		}
	}

	return table.updateIncremental(version)
}

// / Find the most recent checkpoint(s) at or before the given version
// / If we are returning all checkpoints at or before the version, allReturned will be true, otherwise it will be false
// / If we are able to use the _last_checkpoint to retrieve the checkpoint then we will just return that one, and set allReturned to false
// / If we need to search through the directory for checkpoints, then allReturned will be true if the listing is ordered and false otherwise
func (table *DeltaTable[RowType, PartitionType]) findLatestCheckpointsForVersion(version *state.DeltaDataTypeVersion) (checkpoints []CheckPoint, allReturned bool, err error) {
	// First check if _last_checkpoint exists and is prior to the desired version
	var errReadingLastCheckpoint error
	path := lastCheckpointPath()
	lastCheckpointBytes, err := table.Store.Get(path)
	if err == nil {
		checkpoint, err := checkpointFromBytes(lastCheckpointBytes)
		if err != nil {
			// If we were unable to read the _last_checkpoint file, do not return immediately - search for any checkpoint file to try to recover
			// Save the error to return if we don't find a fallback checkpoint
			errReadingLastCheckpoint = err
		} else {
			if version == nil || checkpoint.Version <= *version {
				return []CheckPoint{*checkpoint}, false, nil
			}
		}
	} else if !errors.Is(err, storage.ErrorObjectDoesNotExist) {
		return nil, false, err
	}

	logIterator := storage.NewListIterator(BaseCommitUri(), table.Store)

	foundCheckpoints := make([]CheckPoint, 0, 20)
	listResultsAreOrdered := table.Store.IsListOrdered()

	for {
		meta, err := logIterator.Next()

		if errors.Is(err, storage.ErrorObjectDoesNotExist) {
			break
		}
		if err != nil {
			return nil, false, err
		}
		checkpoint, _, err := checkpointInfoFromURI(storage.NewPath(meta.Location.Raw))
		if err != nil {
			return nil, false, err
		}
		if checkpoint != nil {
			if version != nil && checkpoint.Version > *version {
				// If list results are returned in order, and our search has passed the max version, stop looking
				if listResultsAreOrdered {
					break
				}
				continue
			}
			// For multi-part checkpoint, verify that all parts are present before using it
			isCompleteCheckpoint := true
			if checkpoint.Parts > 0 {
				isCompleteCheckpoint, err = doesCheckpointVersionExist(table.Store, DeltaDataTypeVersion(checkpoint.Version), true)
				if err != nil {
					return nil, false, err
				}
			}
			if !isCompleteCheckpoint {
				continue
			}
			// This checkpoint is valid so save it
			foundCheckpoints = append(foundCheckpoints, *checkpoint)
		}

		// Finally, if list results are ordered, check if this is a regular commit and the version is greater
		// than the max version
		if listResultsAreOrdered && version != nil {
			isCommit, checkpointVersion := CommitVersionFromUri(&meta.Location)
			if isCommit && checkpointVersion > *version {
				break
			}
		}

	}

	if len(foundCheckpoints) == 0 && errReadingLastCheckpoint != nil {
		// Here, if we had an error reading the _last_checkpoint file and we did not subsequently find an appropriate
		// checkpoint file, return the earlier error
		return nil, false, errReadingLastCheckpoint
	}

	// If list results were ordered, then found checkpoints are already ordered.  Otherwise sort them.
	if !listResultsAreOrdered {
		sort.Slice(foundCheckpoints, func(i, j int) bool {
			return foundCheckpoints[i].Version < foundCheckpoints[j].Version
		})
	}
	return foundCheckpoints, true, nil
}

func (table *DeltaTable[RowType, PartitionType]) GetCheckpointDataPaths(checkpoint *CheckPoint) []storage.Path {
	paths := make([]storage.Path, 0, 10)
	prefix := fmt.Sprintf("%020d", checkpoint.Version)
	if checkpoint.Parts == 0 {
		paths = append(paths, storage.PathFromIter([]string{"_delta_log", prefix + ".checkpoint.parquet"}))
	} else {
		for i := DeltaDataTypeInt(0); i < checkpoint.Parts; i++ {
			part := fmt.Sprintf("%s.checkpoint.%010d.%010d.parquet", prefix, i+1, checkpoint.Parts)
			paths = append(paths, storage.PathFromIter([]string{"_delta_log", part}))
		}
	}
	return paths
}

// / Update the table state from the given checkpoint
func (table *DeltaTable[RowType, PartitionType]) restoreCheckpoint(checkpoint *CheckPoint) error {
	state, err := stateFromCheckpoint(table, checkpoint)
	if err != nil {
		return err
	}
	table.State = *state
	return nil
}

// / Updates the DeltaTable to the latest version by incrementally applying newer versions.
// / It assumes that the table is already updated to the current version `self.version`.
// / This function does not look for checkpoints
func (table *DeltaTable[RowType, PartitionType]) updateIncremental(maxVersion *state.DeltaDataTypeVersion) error {
	for {
		if maxVersion != nil && table.State.Version == *maxVersion {
			return nil
		}

		nextCommitVersion, nextCommitActions, noMoreCommits, err := table.nextCommitDetails()
		if err != nil {
			return err
		}
		if noMoreCommits {
			break
		}
		newState, err := NewDeltaTableStateFromActions[RowType, PartitionType](nextCommitActions, nextCommitVersion)
		if err != nil {
			return err
		}
		err = table.State.merge(newState)
		if err != nil {
			return err
		}
	}

	if table.State.Version == -1 {
		return ErrorInvalidVersion
	}
	return nil
}

// / Get the actions inside the next commit log if it exists and return the next commit's version and its actions
// / If the next commit doesn't exist, returns false in the third return parameter
func (table *DeltaTable[RowType, PartitionType]) nextCommitDetails() (state.DeltaDataTypeVersion, []Action, bool, error) {
	nextVersion := table.State.Version + 1
	nextCommitURI := table.CommitUriFromVersion(nextVersion)
	noMoreCommits := false
	actions, err := ReadCommitLog[RowType, PartitionType](table.Store, nextCommitURI)
	if errors.Is(err, storage.ErrorObjectDoesNotExist) {
		noMoreCommits = true
		err = nil
	}
	return nextVersion, actions, noMoreCommits, err
}

// / Create a checkpoint for this table at the given version
// / The existing table state will not be used or modified; a new table instance will be opened at the checkpoint version
// / Returns whether the checkpoint was created and any error
// / If the lock cannot be obtained, does not retry
func (table *DeltaTable[RowType, PartitionType]) CreateCheckpoint(checkpointLock lock.Locker, checkpointConfiguration *CheckpointConfiguration, version state.DeltaDataTypeVersion) (bool, error) {
	return CreateCheckpoint[RowType, PartitionType](table.Store, checkpointLock, checkpointConfiguration, version)
}

// / Create a checkpoint for a table located at the store for the given version
// / If expired log cleanup is enabled on this table, then after a successful checkpoint, run the cleanup to delete expired logs
// / Returns whether the checkpoint was created and any error
// / If the lock cannot be obtained, does not retry - if other processes are checkpointing there's no need to duplicate the effort
func CreateCheckpoint[RowType any, PartitionType any](store storage.ObjectStore, checkpointLock lock.Locker, checkpointConfiguration *CheckpointConfiguration, version state.DeltaDataTypeVersion) (checkpointed bool, err error) {
	// The table doesn't need a commit lock or state store as we are not going to perform any commits
	table, err := OpenTableWithVersion[RowType, PartitionType](store, nil, nil, version)
	if err != nil {
		return false, err
	}
	locked, err := checkpointLock.TryLock()
	if err != nil {
		return false, err
	}
	if !locked {
		// This is unexpected
		return false, ErrorLockFailed
	}
	defer func() {
		// Defer the unlock and overwrite any errors if unlock fails
		if unlockErr := checkpointLock.Unlock(); unlockErr != nil {
			log.Debugf("delta-go: Unlock attempt failed. %v", unlockErr)
			err = unlockErr
		}
	}()
	err = createCheckpointFor(&table.State, table.Store, checkpointConfiguration)
	if err != nil {
		return false, err
	}
	if table.State.EnableExpiredLogCleanup {
		err = validateCheckpointAndCleanup(table, table.Store, version)
		if err != nil {
			return true, err
		}
	}
	return true, err
}

// / Cleanup expired logs before the given checkpoint version, after confirming there is a readable checkpoint
func validateCheckpointAndCleanup[RowType any, PartitionType any](table *DeltaTable[RowType, PartitionType], store storage.ObjectStore, checkpointVersion state.DeltaDataTypeVersion) error {
	// First confirm there is a valid checkpoint at the given version
	checkpoints, _, err := table.findLatestCheckpointsForVersion(&checkpointVersion)
	if err != nil {
		return err
	}
	if len(checkpoints) == 0 || checkpoints[len(checkpoints)-1].Version != checkpointVersion {
		return ErrorReadingCheckpoint
	}
	checkpoint := checkpoints[len(checkpoints)-1]
	err = table.restoreCheckpoint(&checkpoint)
	if err != nil {
		return err
	}
	if table.State.Version != checkpointVersion {
		return ErrorReadingCheckpoint
	}

	// Now remove expired logs before the checkpoint
	_, err = removeExpiredLogsAndCheckpoints(checkpointVersion, time.Now().Add(-table.State.LogRetention), store)
	return err
}

// / Read a commit log and return the actions inside it
func ReadCommitLog[RowType any, PartitionType any](store storage.ObjectStore, location *storage.Path) ([]Action, error) {
	commitData, err := store.Get(location)
	if err != nil {
		return nil, err
	}

	actions, err := ActionsFromLogEntries[RowType, PartitionType](commitData)
	if err != nil {
		return nil, err
	}
	return actions, nil
}

// The URI of the underlying data
// func (table *DeltaTable) TableUri() string {
// 	return table.Store.RootURI()
// }

// Delta table metadata
type DeltaTableMetaData struct {
	// Unique identifier for this table
	Id uuid.UUID
	/// User-provided identifier for this table
	Name string
	/// User-provided description for this table
	Description string
	/// Specification of the encoding for the files stored in the table
	Format Format
	/// Schema of the table
	Schema Schema
	/// An array containing the names of columns by which the data should be partitioned
	PartitionColumns []string
	/// The time when this metadata action is created, in milliseconds since the Unix epoch
	CreatedTime time.Time
	/// table properties
	Configuration map[string]string
}

// / Create metadata for a DeltaTable from scratch
func NewDeltaTableMetaData(name string, description string, format Format, schema Schema, partitionColumns []string, configuration map[string]string) *DeltaTableMetaData {
	// Reference implementation uses uuid v4 to create GUID:
	// https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala#L350
	metaData := new(DeltaTableMetaData)
	metaData.Id = uuid.New()
	metaData.Name = name
	metaData.Description = description
	metaData.Format = format
	metaData.Schema = schema
	metaData.PartitionColumns = partitionColumns
	metaData.CreatedTime = time.Now()
	metaData.Configuration = configuration
	return metaData

}

// DeltaTableMetaData.ToMetaData() converts a DeltaTableMetaData to MetaData
func (dtmd *DeltaTableMetaData) ToMetaData() MetaData {

	metadata := MetaData{
		Id:               dtmd.Id,
		IdAsString:       dtmd.Id.String(),
		Name:             dtmd.Name,
		Description:      dtmd.Description,
		Format:           dtmd.Format,
		SchemaString:     string(dtmd.Schema.Json()),
		PartitionColumns: dtmd.PartitionColumns,
		CreatedTime:      DeltaDataTypeTimestamp(dtmd.CreatedTime.UnixMilli()),
		Configuration:    dtmd.Configuration,
	}
	return metadata
}

func (dtmd *DeltaTableMetaData) GetPartitionColDataTypes() map[string]SchemaDataType {
	partitionColumnsWithType := make(map[string]SchemaDataType, len(dtmd.PartitionColumns))
	for _, v := range dtmd.Schema.Fields {
		for _, p := range dtmd.PartitionColumns {
			if p == v.Name {
				partitionColumnsWithType[p] = v.Type
			}
		}
	}
	return partitionColumnsWithType
}

// / Object representing a delta transaction.
// / Clients that do not need to mutate action content in case a transaction conflict is encountered
// / may use the `commit` method and rely on optimistic concurrency to determine the
// / appropriate Delta version number for a commit. A good example of this type of client is an
// / append only client that does not need to maintain transaction state with external systems.
// / Clients that may need to do conflict resolution if the Delta version changes should use
// / the `prepare_commit` and `try_commit_transaction` methods and manage the Delta version
// / themselves so that they can resolve data conflicts that may occur between Delta versions.
// /
// / Please not that in case of non-retryable error the temporary commit file such as
// / `_delta_log/_commit_<uuid>.json` will orphaned in storage.
type DeltaTransaction[RowType any, PartitionType any] struct {
	DeltaTable *DeltaTable[RowType, PartitionType]
	Actions    []Action
	Options    *DeltaTransactionOptions
}

// / Creates a new delta transaction.
// / Holds a mutable reference to the delta table to prevent outside mutation while a transaction commit is in progress.
// / Transaction behavior may be customized by passing an instance of `DeltaTransactionOptions`.
func NewDeltaTransaction[RowType any, PartitionType any](deltaTable *DeltaTable[RowType, PartitionType], options *DeltaTransactionOptions) *DeltaTransaction[RowType, PartitionType] {
	transaction := new(DeltaTransaction[RowType, PartitionType])
	transaction.DeltaTable = deltaTable
	transaction.Options = options
	return transaction
}

// / Add an arbitrary "action" to the actions associated with this transaction
func (transaction *DeltaTransaction[RowType, PartitionType]) AddAction(action Action) {
	transaction.Actions = append(transaction.Actions, action)
}

// / Add an arbitrary number of actions to the actions associated with this transaction
func (transaction *DeltaTransaction[RowType, PartitionType]) AddActions(actions []Action) {
	transaction.Actions = append(transaction.Actions, actions...)
}

// Commits the given actions to the delta log.
// This method will retry the transaction commit based on the value of `max_retry_commit_attempts` set in `DeltaTransactionOptions`.
func (transaction *DeltaTransaction[RowType, PartitionType]) Commit(operation DeltaOperation, appMetadata map[string]any) (state.DeltaDataTypeVersion, error) {
	// TODO: stubbing `operation` parameter (which will be necessary for writing the CommitInfo action),
	// but leaving it unused for now. `CommitInfo` is a fairly dynamic data structure so we should work
	// out the data structure approach separately.

	// TODO: calculate isolation level to use when checking for conflicts.
	// Leaving conflict checking unimplemented for now to get the "single writer" implementation off the ground.
	// Leaving some commented code in place as a guidepost for the future.

	// let no_data_changed = actions.iter().all(|a| match a {
	//     Action::add(x) => !x.dataChange,
	//     Action::remove(x) => !x.dataChange,
	//     _ => false,
	// });
	// let isolation_level = if no_data_changed {
	//     IsolationLevel::SnapshotIsolation
	// } else {
	//     IsolationLevel::Serializable
	// };

	PreparedCommit, err := transaction.PrepareCommit(operation, appMetadata)
	if err != nil {
		log.Debugf("delta-go: PrepareCommit attempt failed. %v", err)
		return transaction.DeltaTable.State.Version, err
	}

	err = transaction.TryCommitLoop(&PreparedCommit)
	return transaction.DeltaTable.State.Version, err
}

// / Low-level transaction API. Creates a temporary commit file. Once created,
// / the transaction object could be dropped and the actual commit could be executed
// / with `DeltaTable.try_commit_transaction`.
func (transaction *DeltaTransaction[RowType, PartitionType]) PrepareCommit(operation DeltaOperation, appMetadata map[string]any) (PreparedCommit, error) {

	anyCommitInfo := false
	for _, action := range transaction.Actions {
		switch action.(type) {
		case CommitInfo:
			anyCommitInfo = true
		}
	}
	//if not any commit, add new commit info
	if !anyCommitInfo {
		commitInfo := make(CommitInfo)
		commitInfo["timestamp"] = time.Now().UnixMilli()
		commitInfo["clientVersion"] = fmt.Sprintf("delta-go.%s", DELTA_CLIENT_VERSION)
		if operation != nil {
			maps.Copy(commitInfo, operation.GetCommitInfo())
		}
		if appMetadata != nil {
			maps.Copy(commitInfo, appMetadata)
		}
		transaction.AddAction(commitInfo)
	}

	// Serialize all actions that are part of this log entry.
	logEntry, err := LogEntryFromActions[RowType, PartitionType](transaction.Actions)
	if err != nil {
		return PreparedCommit{}, nil
	}

	// Write delta log entry as temporary file to storage. For the actual commit,
	// the temporary file is moved (atomic rename) to the delta log folder within `commit` function.
	token := uuid.New().String()
	fileName := fmt.Sprintf("_commit_%s.json.tmp", token)
	// TODO: Open question, should storagePath use the basePath for the transaction or just hard code the _delta_log path?
	path := storage.Path{Raw: filepath.Join("_delta_log", fileName)}
	commit := PreparedCommit{URI: path}

	err = transaction.DeltaTable.Store.Put(&path, logEntry)
	if err != nil {
		return commit, err
	}

	return commit, nil
}

// TryCommitLoop: Loads metadata from lock containing the latest locked version and tries to obtain the lock and commit for the version + 1 in a loop
func (transaction *DeltaTransaction[RowType, PartitionType]) TryCommitLoop(commit *PreparedCommit) error {
	attemptNumber := 0
	for {
		if attemptNumber > 0 {
			time.Sleep(transaction.Options.RetryWaitDuration)
		}
		if attemptNumber > int(transaction.Options.MaxRetryCommitAttempts)+1 {
			log.Debugf("delta-go: Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of %d so failing.", transaction.Options.MaxRetryCommitAttempts)
			return ErrorExceededCommitRetryAttempts
		}

		err := transaction.TryCommit(commit)
		//Reset local state with the version tried in the commit
		//The next attempt should use the max of the remote state and local state, enables local incrimination if the remote state is stuck
		if errors.Is(err, storage.ErrorObjectAlreadyExists) || errors.Is(err, lock.ErrorLockNotObtained) { //|| errors.Is(err, state.ErrorStateIsEmpty) || errors.Is(err, state.ErrorCanNotReadState) || errors.Is(err, state.ErrorCanNotWriteState) {
			if attemptNumber <= int(transaction.Options.MaxRetryCommitAttempts)+1 {
				attemptNumber += 1
				log.Debugf("delta-go: Transaction attempt failed with '%v'. Incrementing attempt number to %d and retrying.", err, attemptNumber)
			} else {
				log.Debugf("delta-go: Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of %d so failing.", transaction.Options.MaxRetryCommitAttempts)
				return err
			}
		} else {
			// Everything went smooth... exit
			log.Debugf("delta-go: Transaction succeeded on attempt number to %d", attemptNumber)
			return err
		}
	}

}

// TryCommitLoop: Loads metadata from lock containing the latest locked version and tries to obtain the lock and commit for the version + 1 in a loop
func (transaction *DeltaTransaction[RowType, PartitionType]) TryCommit(commit *PreparedCommit) (err error) {
	// Step 1) Acquire Lock
	locked, err := transaction.DeltaTable.LockClient.TryLock()
	// Step 5) Always Release Lock
	// defer transaction.DeltaTable.LockClient.Unlock()
	defer func() {
		// Defer the unlock and overwrite any errors if unlock fails
		if unlockErr := transaction.DeltaTable.LockClient.Unlock(); unlockErr != nil {
			log.Debugf("delta-go: Unlock attempt failed. %v", unlockErr)
			err = unlockErr
		}
	}()
	if err != nil {
		log.Debugf("delta-go: Lock attempt failed. %v", err)
		return errors.Join(lock.ErrorLockNotObtained, err)
	}

	if locked {
		// 2) Lookup the latest prior state
		priorState, err := transaction.DeltaTable.StateStore.Get()
		if err != nil {
			// Failed on state store get, fallback to using the local version
			log.Debugf("delta-go: StateStore Get() attempt failed. %v", err)
			// return max(remoteVersion, version), err
		}

		// 4) Update the state with the latest tried, even in the case that the
		// RenameNotExists was unsuccessful, this ensures that the next try increments the version
		// Take the max of the local state and remote state version in the case that the remote state is not accessible.
		version := max(priorState.Version, transaction.DeltaTable.State.Version) + 1
		transaction.DeltaTable.State.Version = version
		newState := state.CommitState{
			Version: version,
		}
		defer func() {
			if putErr := transaction.DeltaTable.StateStore.Put(newState); putErr != nil {
				log.Debugf("delta-go: StateStore Put() attempt failed. %v", putErr)
				err = putErr
			}
		}()

		// 3) Try to Rename the file
		from := storage.NewPath(commit.URI.Raw)
		to := transaction.DeltaTable.CommitUriFromVersion(version)
		err = transaction.DeltaTable.Store.RenameIfNotExists(from, to)
		if err != nil {
			log.Debugf("delta-go: RenameIfNotExists(from=%s, to=%s) attempt failed. %v", from.Raw, to.Raw, err)
			return err
		}

	} else {
		log.Debug("delta-go: Lock not obtained")
		return errors.Join(lock.ErrorLockNotObtained, err)
	}
	return nil
}

func max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

// Holds the uri to prepared commit temporary file created with `DeltaTransaction.prepare_commit`.
// Once created, the actual commit could be executed with `DeltaTransaction.try_commit`.
type PreparedCommit struct {
	URI storage.Path
}

const DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS uint32 = 10000000

// Options for customizing behavior of a `DeltaTransaction`
type DeltaTransactionOptions struct {
	// number of retry attempts allowed when committing a transaction
	MaxRetryCommitAttempts uint32
	// RetryWaitDuration sets the amount of times between retry's on the transaction
	RetryWaitDuration time.Duration
}

// NewDeltaTransactionOptions Sets the default MaxRetryCommitAttempts to DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS = 10000000
func NewDeltaTransactionOptions() *DeltaTransactionOptions {
	return &DeltaTransactionOptions{MaxRetryCommitAttempts: DEFAULT_DELTA_MAX_RETRY_COMMIT_ATTEMPTS}
}

// / Open the table at this specific version
func OpenTableWithVersion[RowType any, PartitionType any](store storage.ObjectStore, lock lock.Locker, stateStore state.StateStore, version state.DeltaDataTypeVersion) (*DeltaTable[RowType, PartitionType], error) {
	table := NewDeltaTable[RowType, PartitionType](store, lock, stateStore)
	err := table.LoadVersion(&version)
	if err != nil {
		return nil, err
	}

	return table, nil
}

// / Open the latest version of the table
func OpenTable[RowType any, PartitionType any](store storage.ObjectStore, lock lock.Locker, stateStore state.StateStore) (*DeltaTable[RowType, PartitionType], error) {
	table := NewDeltaTable[RowType, PartitionType](store, lock, stateStore)
	err := table.LoadVersion(nil)
	if err != nil {
		return nil, err
	}

	return table, nil
}
