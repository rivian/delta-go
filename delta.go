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
	"math"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rivian/delta-go/lock"
	"github.com/rivian/delta-go/logstore"
	"github.com/rivian/delta-go/state"
	"github.com/rivian/delta-go/storage"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/maps"
)

const clientVersion = "alpha-0.0.0"

const maxReaderVersionSupported = 1
const maxWriterVersionSupported = 1

var (
	ErrExceededCommitRetryAttempts error = errors.New("exceeded commit retry attempts")
	ErrNotATable                   error = errors.New("not a table")
	ErrInvalidVersion              error = errors.New("invalid version")
	ErrUnableToLoadVersion         error = errors.New("unable to load specified version")
	ErrLockFailed                  error = errors.New("lock failed unexpectedly without an error")
	ErrNotImplemented              error = errors.New("not implemented")
	ErrUnsupportedReaderVersion    error = errors.New("reader version is unsupported")
	ErrUnsupportedWriterVersion    error = errors.New("writer version is unsupported")
)

var (
	commitFileRegex         *regexp.Regexp = regexp.MustCompile(`^(\d{20}).json$`)
	checkpointRegex         *regexp.Regexp = regexp.MustCompile(`^(\d{20})\.checkpoint\.parquet$`)
	checkpointPartsRegex    *regexp.Regexp = regexp.MustCompile(`^(\d{20})\.checkpoint\.(\d{10})\.(\d{10})\.parquet$`)
	commitOrCheckpointRegex *regexp.Regexp = regexp.MustCompile(`^(\d{20})\.((json$)|(checkpoint\.parquet$)|(checkpoint\.(\d{10})\.(\d{10})\.parquet$))`)
)

type Table struct {
	// The state of the table as of the most recent loaded Delta log entry.
	State TableState
	// The remote store of the state of the table as of the most recent loaded Delta log entry.
	StateStore state.StateStore
	// object store to access log and data files
	Store storage.ObjectStore
	// Locking client to ensure optimistic locked commits from distributed workers
	LockClient lock.Locker
	// file metadata for latest checkpoint
	LastCheckPoint *CheckPoint
	// table versions associated with timestamps
	VersionTimestamp map[int64]time.Time
	// Log store which provides multi-cluster write support
	LogStore logstore.LogStore
}

// Create a new Table struct without loading any data from backing storage.
//
// NOTE: This is for advanced users. If you don't know why you need to use this method, please
// call one of the `open_table` helper methods instead.
func NewTable(store storage.ObjectStore, lock lock.Locker, stateStore state.StateStore) *Table {
	table := new(Table)
	table.Store = store
	table.StateStore = stateStore
	table.LockClient = lock
	table.LastCheckPoint = nil
	table.State = *NewTableState(-1)
	return table
}

func NewTableWithLogStore(store storage.ObjectStore, lock lock.Locker, logStore logstore.LogStore) *Table {
	t := new(Table)
	t.State = *NewTableState(-1)
	t.Store = store
	t.LockClient = lock
	t.LastCheckPoint = nil
	t.LogStore = logStore

	return t
}

// Creates a new Transaction for the Table.
// The transaction holds a mutable reference to the Table, preventing other references
// until the transaction is dropped.
func (table *Table) CreateTransaction(options *TransactionOptions) *Transaction {
	return NewTransaction(table, options)
}

// / Return the uri of commit version.
func CommitURIFromVersion(version int64) storage.Path {
	str := fmt.Sprintf("%020d.json", version)
	path := storage.PathFromIter([]string{"_delta_log", str})
	return path
}

// / The base path of commit uri's
func BaseCommitURI() storage.Path {
	return storage.NewPath("_delta_log/")
}

// / Return true if URI is a valid commit filename (not a checkpoint file, and not a temp commit)
func IsValidCommitURI(path storage.Path) bool {
	match := commitFileRegex.MatchString(path.Base())
	return match
}

// IsValidCommitOrCheckpointURI returns true if a URI is a valid commit or checkpoint file name.
// Otherwise, it returns false.
func IsValidCommitOrCheckpointURI(path storage.Path) bool {
	return commitFileRegex.MatchString(path.Base()) || commitOrCheckpointRegex.MatchString(path.Base())
}

// / Return true plus the version if the URI is a valid commit filename
func CommitVersionFromURI(path storage.Path) (bool, int64) {
	groups := commitFileRegex.FindStringSubmatch(path.Base())
	if len(groups) == 2 {
		version, err := strconv.ParseInt(groups[1], 10, 64)
		if err == nil {
			return true, int64(version)
		}
	}
	return false, 0
}

// / Return true plus the version if the URI is a valid commit or checkpoint filename
func CommitOrCheckpointVersionFromURI(path storage.Path) (bool, int64) {
	groups := commitOrCheckpointRegex.FindStringSubmatch(path.Base())
	if groups != nil {
		version, err := strconv.ParseInt(groups[1], 10, 64)
		if err == nil {
			return true, int64(version)
		}
	}
	return false, 0
}

// / Create a Table with version 0 given the provided MetaData, Protocol, and CommitInfo
// / Note that if the protocol MinReaderVersion or MinWriterVersion is too high, the table will be created
// / and then an error will be returned
func (table *Table) Create(metadata TableMetaData, protocol Protocol, commitInfo CommitInfo, addActions []Add) error {
	meta := metadata.ToMetaData()

	// delta-go commit info will include the delta-go version and timestamp as of now
	enrichedCommitInfo := maps.Clone(commitInfo)
	enrichedCommitInfo["clientVersion"] = fmt.Sprintf("delta-go.%s", clientVersion)
	enrichedCommitInfo["timestamp"] = time.Now().UnixMilli()

	actions := []Action{
		enrichedCommitInfo,
		protocol,
		meta,
	}

	for _, add := range addActions {
		actions = append(actions, add)
	}

	var version int64
	var err error

	transaction := table.CreateTransaction(NewTransactionOptions())
	transaction.AddActions(actions)

	if table.LogStore != nil {
		version, err = transaction.CommitLogStore()
		if err != nil {
			return err
		}
	} else {
		preparedCommit, err := transaction.PrepareCommit()
		if err != nil {
			return err
		}
		//Set StateStore Version=-1 synced with the table State Version
		zeroState := state.CommitState{
			Version: table.State.Version,
		}
		transaction.Table.StateStore.Put(zeroState)
		err = transaction.TryCommit(&preparedCommit)
		if err != nil {
			return err
		}

		version = table.State.Version
	}

	// Merge state from new commit version
	newState, err := NewTableStateFromCommit(table, version)
	if err != nil {
		return err
	}
	table.State.merge(newState, 150000, nil, false)

	// If either version is too high, we return an error, but we still create the table first
	if protocol.MinReaderVersion > maxReaderVersionSupported {
		err = ErrUnsupportedReaderVersion
	}
	if protocol.MinWriterVersion > maxWriterVersionSupported {
		err = errors.Join(err, ErrUnsupportedWriterVersion)
	}

	return err
}

// / Exists checks if a Table with version 0 exists in the object store.
func (table *Table) Exists() (bool, error) {
	path := CommitURIFromVersion(0)

	meta, err := table.Store.Head(path)
	if errors.Is(err, storage.ErrObjectDoesNotExist) {
		// Fallback: check for other variants of the version
		logIterator := storage.NewListIterator(BaseCommitURI(), table.Store)
		for {
			meta, err := logIterator.Next()
			if errors.Is(err, storage.ErrObjectDoesNotExist) {
				break
			}
			if err != nil {
				return false, err
			}
			isCommitOrCheckpoint, _ := CommitOrCheckpointVersionFromURI(meta.Location)
			if isCommitOrCheckpoint {
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
func (table *Table) ReadCommitVersion(version int64) ([]Action, error) {
	path := CommitURIFromVersion(version)
	return ReadCommitLog(table.Store, path)
}

// LatestVersion gets the latest version of a table.
func (t *Table) LatestVersion() (int64, error) {
	var (
		minVersion int64
		path       = lastCheckpointPath()
		bytes, err = t.Store.Get(path)
		objects    *storage.ListResult
	)
	if errors.Is(err, storage.ErrObjectDoesNotExist) {
		for {
			o, err := t.Store.List(storage.NewPath("_delta_log"), objects)
			if err != nil {
				return -1, fmt.Errorf("list Delta log: %w", err)
			}
			objects = &o

			found, v := findValidCommitOrCheckpointURI(objects.Objects)
			if !found {
				if objects.NextToken == "" {
					return -1, ErrNotATable
				}
				continue
			}
			minVersion = v
			break
		}
	} else {
		if checkpoint, err := checkpointFromBytes(bytes); err != nil {
			return -1, fmt.Errorf("checkpoint from bytes: %v", err)
		} else {
			minVersion = checkpoint.Version
		}
	}

	var (
		maxVersion int64 = minVersion + 1
		count      float64
	)
	for {
		if _, err = t.Store.Head(CommitURIFromVersion(maxVersion)); errors.Is(err, storage.ErrObjectDoesNotExist) {
			break
		} else {
			count++
			minVersion = maxVersion
			maxVersion += int64(math.Pow(count, 2))
		}
	}

	var (
		latestVersion int64
		currErr       error
		nextErr       error
	)
	for minVersion <= maxVersion {
		latestVersion = (minVersion + maxVersion) / 2

		_, currErr = t.Store.Head(CommitURIFromVersion(latestVersion))
		_, nextErr = t.Store.Head(CommitURIFromVersion(latestVersion + 1))

		if currErr == nil && nextErr != nil {
			break
		} else if currErr != nil {
			maxVersion = latestVersion - 1
		} else {
			minVersion = latestVersion + 1
		}
	}
	return latestVersion, nil
}

func findValidCommitOrCheckpointURI(metadata []storage.ObjectMeta) (bool, int64) {
	for _, m := range metadata {
		if IsValidCommitOrCheckpointURI(m.Location) {
			parsed, version := CommitVersionFromURI(m.Location)
			if !parsed {
				continue
			}
			return true, version
		}
	}

	return false, -1
}

// SyncStateStore syncs the table's state store with its latest version.
func (t *Table) SyncStateStore() (err error) {
	locked, err := t.LockClient.TryLock()
	if err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	defer func() {
		// Defer the unlock and overwrite any errors if the unlock fails.
		if unlockErr := t.LockClient.Unlock(); unlockErr != nil {
			err = fmt.Errorf("release lock: %w", unlockErr)
		}
	}()

	if locked {
		version, err := t.LatestVersion()
		if err != nil {
			return fmt.Errorf("get latest version: %w", err)
		}

		state := state.CommitState{
			Version: version,
		}
		if err := t.StateStore.Put(state); err != nil {
			return fmt.Errorf("put state: %w", err)
		}
	}

	return nil
}

// Load loads the table state using the given configuration
func (table *Table) Load(config *OptimizeCheckpointConfiguration) error {
	return table.LoadVersionWithConfiguration(nil, config)
}

// LoadVersion loads the table state at the specified version using default configuration options
func (table *Table) LoadVersion(version *int64) error {
	return table.LoadVersionWithConfiguration(version, nil)
}

// LoadVersionWithConfiguration loads the table state at the specified version using the given configuration
func (table *Table) LoadVersionWithConfiguration(version *int64, config *OptimizeCheckpointConfiguration) error {
	return table.loadVersionWithConfiguration(version, config, true)
}

func (table *Table) loadVersionWithConfiguration(version *int64, config *OptimizeCheckpointConfiguration, cleanupWorkingStorage bool) error {
	table.LastCheckPoint = nil
	table.State = *NewTableState(-1)
	err := setupOnDiskOptimization(config, &table.State, 0)
	if err != nil {
		return err
	}
	if cleanupWorkingStorage && table.State.onDiskOptimization {
		defer config.WorkingStore.DeleteFolder(config.WorkingFolder)
	}
	var checkpointLoadError error
	if version != nil {
		commitURI := CommitURIFromVersion(*version)
		_, err := table.Store.Head(commitURI)
		if errors.Is(err, storage.ErrObjectDoesNotExist) {
			return ErrInvalidVersion
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
		err = table.restoreCheckpoint(&checkpoints[checkpointIndex], config)
		if err == nil {
			// We successfully loaded a checkpoint
			checkpointLoadError = nil
			break
		} else {
			// Store the checkpoint load error for later in case we can't recover
			checkpointLoadError = err
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
			} else {
				break
			}
		}
	}

	err = table.updateIncremental(version, config)
	if err != nil {
		// If we happened to get both a checkpoint read error and an incremental load error, it may be helpful to return both
		return errors.Join(err, checkpointLoadError)
	}
	// If there was no error but we failed to load the specified version, return error indicating that
	if version != nil && table.State.Version != *version {
		return errors.Join(ErrUnableToLoadVersion, checkpointLoadError)
	}
	return nil
}

// / Find the most recent checkpoint(s) at or before the given version
// / If we are returning all checkpoints at or before the version, allReturned will be true, otherwise it will be false
// / If we are able to use the _last_checkpoint to retrieve the checkpoint then we will just return that one, and set allReturned to false
// / If we need to search through the directory for checkpoints, then allReturned will be true if the listing is ordered and false otherwise
func (table *Table) findLatestCheckpointsForVersion(version *int64) (checkpoints []CheckPoint, allReturned bool, err error) {
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
	} else if !errors.Is(err, storage.ErrObjectDoesNotExist) {
		return nil, false, err
	}

	logIterator := storage.NewListIterator(BaseCommitURI(), table.Store)

	foundCheckpoints := make([]CheckPoint, 0, 20)
	listResultsAreOrdered := table.Store.IsListOrdered()

	for {
		meta, err := logIterator.Next()

		if errors.Is(err, storage.ErrObjectDoesNotExist) {
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
			if checkpoint.Parts != nil {
				isCompleteCheckpoint, err = DoesCheckpointVersionExist(table.Store, checkpoint.Version, true)
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
			isCommit, checkpointVersion := CommitVersionFromURI(meta.Location)
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

// GetCheckpointDataPaths returns the expected file path(s) for the given checkpoint Parquet files
// If it is a multi-part checkpoint then there will be one path for each part
func (table *Table) GetCheckpointDataPaths(checkpoint *CheckPoint) []storage.Path {
	paths := make([]storage.Path, 0, 10)
	prefix := fmt.Sprintf("%020d", checkpoint.Version)
	if checkpoint.Parts == nil {
		paths = append(paths, storage.PathFromIter([]string{"_delta_log", prefix + ".checkpoint.parquet"}))
	} else {
		for i := int32(0); i < *checkpoint.Parts; i++ {
			part := fmt.Sprintf("%s.checkpoint.%010d.%010d.parquet", prefix, i+1, *checkpoint.Parts)
			paths = append(paths, storage.PathFromIter([]string{"_delta_log", part}))
		}
	}
	return paths
}

// Update the table state from the given checkpoint
func (table *Table) restoreCheckpoint(checkpoint *CheckPoint, config *OptimizeCheckpointConfiguration) error {
	state, err := stateFromCheckpoint(table, checkpoint, config)
	if err != nil {
		return err
	}
	table.State = *state
	return nil
}

// Updates the Table to the latest version by incrementally applying newer versions.
// It assumes that the table is already updated to the current version `self.version`.
// This function does not look for checkpoints
func (table *Table) updateIncremental(maxVersion *int64, config *OptimizeCheckpointConfiguration) error {
	for {
		if maxVersion != nil && table.State.Version == *maxVersion {
			break
		}

		nextCommitVersion, nextCommitActions, noMoreCommits, err := table.nextCommitDetails()
		if err != nil {
			return err
		}
		if noMoreCommits {
			break
		}
		newState, err := NewTableStateFromActions(nextCommitActions, nextCommitVersion)
		if err != nil {
			return err
		}
		// TODO configuration option for max rows per part? It is only used for on disk optimization.
		err = table.State.merge(newState, 150000, config, false)
		if err != nil {
			return err
		}
	}

	if table.State.Version == -1 {
		return ErrInvalidVersion
	}
	if table.State.onDiskOptimization {
		// We need to do one final "merge" to resolve any remaining adds/removes in our buffer
		err := table.State.merge(nil, 150000, config, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// / Get the actions inside the next commit log if it exists and return the next commit's version and its actions
// / If the next commit doesn't exist, returns false in the third return parameter
func (table *Table) nextCommitDetails() (int64, []Action, bool, error) {
	nextVersion := table.State.Version + 1
	nextCommitURI := CommitURIFromVersion(nextVersion)
	noMoreCommits := false
	actions, err := ReadCommitLog(table.Store, nextCommitURI)
	if errors.Is(err, storage.ErrObjectDoesNotExist) {
		noMoreCommits = true
		err = nil
	}
	return nextVersion, actions, noMoreCommits, err
}

// CreateCheckpoint creates a checkpoint for this table at the given version
// The existing table state will not be used or modified; a new table instance will be opened at the checkpoint version
// Returns whether the checkpoint was created and any error
// If the lock cannot be obtained, does not retry
func (table *Table) CreateCheckpoint(checkpointLock lock.Locker, checkpointConfiguration *CheckpointConfiguration, version int64) (bool, error) {
	return CreateCheckpoint(table.Store, checkpointLock, checkpointConfiguration, version)
}

// CreateCheckpoint creates a checkpoint for a table located at the store for the given version
// If expired log cleanup is enabled on this table, then after a successful checkpoint, run the cleanup to delete expired logs
// Returns whether the checkpoint was created and any error
// If the lock cannot be obtained, does not retry - if other processes are checkpointing there's no need to duplicate the effort
func CreateCheckpoint(store storage.ObjectStore, checkpointLock lock.Locker, checkpointConfiguration *CheckpointConfiguration, version int64) (checkpointed bool, err error) {
	// The table doesn't need a commit lock or state store as we are not going to perform any commits
	table, err := openTableWithVersionAndConfiguration(store, nil, nil, version, &checkpointConfiguration.ReadWriteConfiguration, false)
	if err != nil {
		// If the UnsafeIgnoreUnsupportedReaderWriterVersionErrors option is true, we can ignore unsupported version errors
		isUnsupportedVersionError := errors.Is(err, ErrUnsupportedReaderVersion) || errors.Is(err, ErrUnsupportedWriterVersion)
		if !(isUnsupportedVersionError && checkpointConfiguration.UnsafeIgnoreUnsupportedReaderWriterVersionErrors) {
			return false, err
		}
	}
	if table.State.onDiskOptimization {
		defer checkpointConfiguration.ReadWriteConfiguration.WorkingStore.DeleteFolder(checkpointConfiguration.ReadWriteConfiguration.WorkingFolder)
	}
	locked, err := checkpointLock.TryLock()
	if err != nil {
		return false, err
	}
	if !locked {
		// This is unexpected
		return false, ErrLockFailed
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
	if table.State.ExperimentalEnableExpiredLogCleanup && !checkpointConfiguration.DisableCleanup {
		err = validateCheckpointAndCleanup(table, table.Store, version)
		if err != nil {
			return true, err
		}
	}
	return true, err
}

// / Cleanup expired logs before the given checkpoint version, after confirming there is a readable checkpoint
func validateCheckpointAndCleanup(table *Table, store storage.ObjectStore, checkpointVersion int64) error {
	// First confirm there is a valid checkpoint at the given version
	checkpoints, _, err := table.findLatestCheckpointsForVersion(&checkpointVersion)
	if err != nil {
		return err
	}
	if len(checkpoints) == 0 || checkpoints[len(checkpoints)-1].Version != checkpointVersion {
		return ErrReadingCheckpoint
	}
	checkpoint := checkpoints[len(checkpoints)-1]
	err = table.restoreCheckpoint(&checkpoint, nil)
	if err != nil {
		return err
	}
	if table.State.Version != checkpointVersion {
		return ErrReadingCheckpoint
	}

	// Now remove expired logs before the checkpoint
	_, err = removeExpiredLogsAndCheckpoints(checkpointVersion, time.Now().Add(-table.State.LogRetention), store)
	return err
}

// ReadCommitLog reads a commit log and return the actions inside it
func ReadCommitLog(store storage.ObjectStore, location storage.Path) ([]Action, error) {
	commitData, err := store.Get(location)
	if err != nil {
		return nil, err
	}

	actions, err := ActionsFromLogEntries(commitData)
	if err != nil {
		return nil, err
	}
	return actions, nil
}

// Delta table metadata
type TableMetaData struct {
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

// / Create metadata for a Table from scratch
func NewTableMetaData(name string, description string, format Format, schema Schema, partitionColumns []string, configuration map[string]string) *TableMetaData {
	// Reference implementation uses uuid v4 to create GUID:
	// https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala#L350
	metaData := new(TableMetaData)
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

// TableMetaData.ToMetaData() converts a TableMetaData to MetaData
func (dtmd *TableMetaData) ToMetaData() MetaData {
	createdTime := dtmd.CreatedTime.UnixMilli()
	metadata := MetaData{
		Id:               dtmd.Id,
		IdAsString:       dtmd.Id.String(),
		Name:             &dtmd.Name,
		Description:      &dtmd.Description,
		Format:           dtmd.Format,
		SchemaString:     string(dtmd.Schema.Json()),
		PartitionColumns: dtmd.PartitionColumns,
		CreatedTime:      &createdTime,
		Configuration:    dtmd.Configuration,
	}
	return metadata
}

func (dtmd *TableMetaData) GetPartitionColDataTypes() map[string]SchemaDataType {
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

// / Object representing a Delta transaction.
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
type Transaction struct {
	Table       *Table
	Actions     []Action
	Operation   Operation
	AppMetadata map[string]any
	Options     *TransactionOptions
}

// ReadActions gets actions from a file.
//
// With many concurrent readers/writers, there's a chance that concurrent recovery
// operations occur on the same file, i.e. the same temp file T(N) is copied into the
// target N.json file more than once. Though data loss will *NOT* occur, readers of N.json
// may receive an error from S3 as the ETag of N.json was changed. This is safe to
// retry, so we do so here.
func (transaction *Transaction) ReadActions(path storage.Path) ([]Action, error) {
	attempt := 0
	for {
		if attempt >= int(transaction.Options.MaxRetryReadAttempts) {
			return nil, fmt.Errorf("failed to get actions after %d attempts", transaction.Options.MaxRetryReadAttempts)
		}

		entry, err := transaction.Table.Store.Get(path)
		if err != nil {
			attempt++
			log.Debugf("delta-go: Failed to get log entry. Incrementing attempt number to %d and retrying. %v", attempt, err)
			continue
		}

		actions, err := ActionsFromLogEntries(entry)
		if err != nil {
			attempt++
			log.Debugf("delta-go: Failed to get actions from log entry. Incrementing attempt number to %d and retrying. %v",
				attempt, err)
			continue
		}

		return actions, nil
	}
}

// CommitLogStore writes actions to a file with or without overwrite as indicated.
// An error is thrown if the file already exists and overwriting is disabled.
//
// If overwriting is enabled, then write normally without any interaction with a log store.
// Otherwise, to commit for Delta version N:
// - Step 0: Fail if N.json already exists in the file system.
// - Step 1: Ensure that N-1.json exists. If not, perform a recovery.
// - Step 2: PREPARE the commit.
//   - Write the actions into temp file T(N).
//   - Write uncompleted commit entry E(N, T(N)) with mutual exclusion to the log store.
//
// - Step 3: COMMIT the commit to the Delta log.
//   - Copy T(N) into N.json.
//
// - Step 4: ACKNOWLEDGE the commit.
//   - Overwrite and complete commit entry E in the log store.
func (transaction *Transaction) CommitLogStore() (int64, error) {
	attempt := 0
	for {
		if attempt >= int(transaction.Options.MaxRetryWriteAttempts) {
			return -1, fmt.Errorf("failed to commit with log store after %d attempts", transaction.Options.MaxRetryWriteAttempts)
		}

		version, err := transaction.tryCommitLogStore()
		if err != nil {
			attempt++
			log.Debugf("delta-go: Incrementing log store commit attempt number to %d and retrying: %v", attempt, err)
			continue
		}

		return version, nil
	}
}

func (t *Transaction) tryCommitLogStore() (version int64, err error) {
	var currURI storage.Path
	prevURI, err := t.Table.LogStore.Latest(t.Table.Store.BaseURI())

	if prevURI != nil {
		parsed, prevVersion := CommitVersionFromURI(prevURI.FileName())
		if !parsed {
			return -1, fmt.Errorf("failed to parse previous version from %s", prevURI.FileName().Raw)
		}

		currURI = CommitURIFromVersion(prevVersion + 1)
	} else {
		if version, err := t.Table.LatestVersion(); err != nil {
			currURI = CommitURIFromVersion(0)
		} else {
			uri := CommitURIFromVersion(version).Raw
			fileName := storage.NewPath(strings.Split(uri, "_delta_log/")[1])
			seconds := t.Table.LogStore.ExpirationDelaySeconds()

			entry, err := logstore.New(t.Table.Store.BaseURI(), fileName,
				storage.NewPath("") /* tempPath */, true /* isComplete */, uint64(time.Now().Unix())+seconds)
			if err != nil {
				return -1, fmt.Errorf("create first commit entry: %v", err)
			}

			if err := t.Table.LogStore.Put(entry, t.Table.Store.SupportsAtomicPutIfAbsent()); err != nil {
				return -1, fmt.Errorf("put first commit entry: %v", err)
			}
			log.Debugf("delta-go: Put completed commit entry for table path %s and file name %s in the empty log store.",
				t.Table.Store.BaseURI(), fileName)

			currURI = CommitURIFromVersion(version + 1)
		}
	}

	t.AddCommitInfoIfNotPresent()

	// Prevent concurrent writers from either
	// a) concurrently overwriting N.json if overwriting is enabled
	// b) both checking if N-1.json exists and performing a recovery where they both
	// copy T(N-1) into N-1.json
	//
	// Note that the mutual exclusion on writing into N.json with overwriting disabled from
	// different machines is provided by the log store, not by this lock.
	//
	// Also note that this lock is for N.json, while the lock used during a recovery is for
	// N-1.json. Thus, there is no deadlock.
	lock, err := t.Table.LockClient.NewLock(t.Table.Store.BaseURI().Join(currURI).Raw)
	if err != nil {
		return -1, fmt.Errorf("create lock: %v", err)
	}
	if _, err = lock.TryLock(); err != nil {
		return -1, fmt.Errorf("acquire lock: %v", err)
	}
	defer func() {
		// Defer the unlock and overwrite any errors if the unlock fails.
		if unlockErr := lock.Unlock(); unlockErr != nil {
			err = unlockErr
		}
	}()

	fileName := storage.NewPath(strings.Split(currURI.Raw, "_delta_log/")[1])

	parsed, currVersion := CommitVersionFromURI(currURI)
	if !parsed {
		return -1, fmt.Errorf("failed to parse previous version from %s", currURI.Raw)
	}

	if t.Table.Store.SupportsAtomicPutIfAbsent() {
		t.writeActions(currURI, t.Actions)

		return currVersion, nil
	} else {
		// Step 0: Fail if N.json already exists in the file system and overwriting is disabled.
		if _, err = t.Table.Store.Head(currURI); err == nil {
			return -1, errors.New("current version already exists")
		}
	}

	// Step 1: Ensure that N-1.json exists.
	if currVersion > 0 {
		prevFileName := storage.NewPath(strings.Split(CommitURIFromVersion(currVersion-1).Raw, "_delta_log/")[1])

		if prevEntry, err := t.Table.LogStore.Get(t.Table.Store.BaseURI(), prevFileName); err != nil {
			return -1, fmt.Errorf("get previous commit: %v", err)
		} else if !prevEntry.IsComplete() {
			if err := t.fixLog(prevEntry); err != nil {
				return -1, fmt.Errorf("fix Delta log: %v", err)
			}
		}
	} else {
		if entry, err := t.Table.LogStore.Get(t.Table.Store.BaseURI(), fileName); err == nil {
			if _, err := t.Table.Store.Head(currURI); entry.IsComplete() && err != nil {
				return -1, fmt.Errorf("old entries for table %s still in log store", t.Table.Store.BaseURI())
			}
		}
	}

	// Step 2: PREPARE the commit.
	tempPath, err := t.createTempPath(fileName)
	if err != nil {
		return -1, fmt.Errorf("create temp path: %v", err)
	}
	relativeTempPath := storage.NewPath("_delta_log").Join(tempPath)

	entry, err := logstore.New(t.Table.Store.BaseURI(), fileName, tempPath, false /* isComplete */, 0 /* expirationTime */)
	if err != nil {
		return -1, fmt.Errorf("create commit entry: %v", err)
	}

	if err := t.writeActions(relativeTempPath, t.Actions); err != nil {
		return -1, fmt.Errorf("write actions: %v", err)
	}

	// Step 2.2: Create uncompleted commit entry E(N, T(N)).
	if err := t.Table.LogStore.Put(entry, t.Table.Store.SupportsAtomicPutIfAbsent()); err != nil {
		return -1, fmt.Errorf("put uncompleted commit entry: %v", err)
	}

	// Step 3: COMMIT the commit to the Delta log.
	//         Copy T(N) -> N.json with overwriting disabled.
	if err := t.copyTempFile(relativeTempPath, currURI); err != nil {
		return -1, fmt.Errorf("copy temp file to N.json: %v", err)
	}

	// Step 4: ACKNOWLEDGE the commit.
	if err := t.complete(entry); err != nil {
		return -1, fmt.Errorf("acknowledge commit: %v", err)
	}

	return currVersion, nil
}

func (t *Transaction) complete(entry *logstore.CommitEntry) error {
	seconds := t.Table.LogStore.ExpirationDelaySeconds()
	entry, err := entry.Complete(seconds)
	if err != nil {
		return fmt.Errorf("complete commit entry: %v", err)
	}

	if err := t.Table.LogStore.Put(entry, true); err != nil {
		return fmt.Errorf("put completed commit entry: %v", err)
	}

	return nil
}

func (t *Transaction) copyTempFile(src storage.Path, dst storage.Path) error {
	return t.Table.Store.RenameIfNotExists(src, dst)
}

func (t *Transaction) createTempPath(path storage.Path) (storage.Path, error) {
	return storage.NewPath(fmt.Sprintf(".tmp/%s.%s", path.Raw, uuid.New().String())), nil
}

func (t *Transaction) fixLog(entry *logstore.CommitEntry) (err error) {
	if entry.IsComplete() {
		return nil
	}

	filePath, err := entry.AbsoluteFilePath()
	if err != nil {
		return fmt.Errorf("get absolute file path: %v", err)
	}

	lock, err := t.Table.LockClient.NewLock(filePath.Raw)
	if err != nil {
		return fmt.Errorf("create lock: %v", err)
	}
	if _, err = lock.TryLock(); err != nil {
		return fmt.Errorf("acquire lock: %v", err)
	}
	defer func() {
		// Defer the unlock and overwrite any errors if the unlock fails.
		if unlockErr := lock.Unlock(); err != nil {
			err = unlockErr
		}
	}()

	attempt, copied := 0, false
	for {
		if attempt >= int(t.Options.MaxRetryLogFixAttempts) {
			return fmt.Errorf("failed to fix Delta log after %d attempts", t.Options.MaxRetryLogFixAttempts)
		}

		log.Infof("delta-go: Trying to fix %s.", entry.FileName().Raw)

		if _, err = t.Table.Store.Head(filePath); !copied && err != nil {
			tempPath, err := entry.AbsoluteTempPath()
			if err != nil {
				attempt++
				log.Debugf("delta-go: Failed to get absolute temp path. Incrementing attempt number to %d and retrying. %v",
					attempt, err)
				continue
			}

			if err := t.copyTempFile(tempPath, filePath); err != nil {
				attempt++
				log.Debugf("delta-go: File %s already copied. Incrementing attempt number to %d and retrying. %v",
					entry.FileName().Raw, attempt, err)
				copied = true
				continue
			}

			copied = true
		}

		if err := t.complete(entry); err != nil {
			attempt++
			log.Debugf("delta-go: Failed to complete commit entry. Incrementing attempt number to %d and retrying. %v", attempt, err)
			continue
		}

		log.Infof("delta-go: Fixed file %s.", entry.FileName().Raw)
		return nil
	}
}

func (transaction *Transaction) writeActions(path storage.Path, actions []Action) error {
	// Serialize all actions that are part of this log entry.
	entry, err := LogEntryFromActions(actions)
	if err != nil {
		return errors.New("failed to serialize actions")
	}

	return transaction.Table.Store.Put(path, entry)
}

// / Creates a new Delta transaction.
// / Holds a mutable reference to the Delta table to prevent outside mutation while a transaction commit is in progress.
// / Transaction behavior may be customized by passing an instance of `TransactionOptions`.
func NewTransaction(table *Table, options *TransactionOptions) *Transaction {
	transaction := new(Transaction)
	transaction.Table = table
	transaction.Options = options
	return transaction
}

// / Add an arbitrary "action" to the actions associated with this transaction
func (transaction *Transaction) AddAction(action Action) {
	transaction.Actions = append(transaction.Actions, action)
}

// / Add an arbitrary number of actions to the actions associated with this transaction
func (transaction *Transaction) AddActions(actions []Action) {
	transaction.Actions = append(transaction.Actions, actions...)
}

// AddCommitInfo adds a `commitInfo` action to a transaction's actions if not already present.
func (t *Transaction) AddCommitInfoIfNotPresent() {
	found := false
	for _, action := range t.Actions {
		switch action.(type) {
		case CommitInfo:
			found = true
		}
	}

	if !found {
		commitInfo := make(CommitInfo)
		commitInfo["timestamp"] = time.Now().UnixMilli()
		commitInfo["clientVersion"] = fmt.Sprintf("delta-go.%s", clientVersion)

		if t.Operation != nil {
			maps.Copy(commitInfo, t.Operation.GetCommitInfo())
		}
		if t.AppMetadata != nil {
			maps.Copy(commitInfo, t.AppMetadata)
		}

		t.AddAction(commitInfo)
	}
}

// SetOperation sets the Delta operation for this transaction.
func (transaction *Transaction) SetOperation(operation Operation) {
	transaction.Operation = operation
}

// SetAppMetadata sets the app metadata for this transaction.
func (transaction *Transaction) SetAppMetadata(appMetadata map[string]any) {
	transaction.AppMetadata = appMetadata
}

// Commits the given actions to the Delta log.
// This method will retry the transaction commit based on the value of `max_retry_commit_attempts` set in `TransactionOptions`.
func (transaction *Transaction) Commit() (int64, error) {
	PreparedCommit, err := transaction.PrepareCommit()
	if err != nil {
		log.Debugf("delta-go: PrepareCommit attempt failed. %v", err)
		return transaction.Table.State.Version, err
	}

	err = transaction.TryCommitLoop(&PreparedCommit)
	return transaction.Table.State.Version, err
}

// / Low-level transaction API. Creates a temporary commit file. Once created,
// / the transaction object could be dropped and the actual commit could be executed
// / with `Table.try_commit_transaction`.
func (transaction *Transaction) PrepareCommit() (PreparedCommit, error) {
	transaction.AddCommitInfoIfNotPresent()

	// Serialize all actions that are part of this log entry.
	logEntry, err := LogEntryFromActions(transaction.Actions)
	if err != nil {
		return PreparedCommit{}, nil
	}

	// Write Delta log entry as temporary file to storage. For the actual commit,
	// the temporary file is moved (atomic rename) to the Delta log folder within `commit` function.
	token := uuid.New().String()
	fileName := fmt.Sprintf("_commit_%s.json", token)
	// TODO: Open question, should storagePath use the basePath for the transaction or just hard code the _delta_log path?
	path := storage.Path{Raw: filepath.Join("_delta_log", ".tmp", fileName)}
	commit := PreparedCommit{URI: path}

	err = transaction.Table.Store.Put(path, logEntry)
	if err != nil {
		return commit, err
	}

	return commit, nil
}

// TryCommitLoop loads metadata from lock containing the latest locked version and tries to obtain the lock and commit for the version + 1 in a loop
func (transaction *Transaction) TryCommitLoop(commit *PreparedCommit) error {
	attempt := 0
	for {
		if attempt > 0 {
			time.Sleep(transaction.Options.RetryWaitDuration)
		}
		if attempt >= int(transaction.Options.MaxRetryCommitAttempts) {
			log.Debugf("delta-go: Transaction attempt failed. Attempts exhausted beyond max_retry_commit_attempts of %d so failing.", transaction.Options.MaxRetryCommitAttempts)
			return ErrExceededCommitRetryAttempts
		}

		if err := transaction.TryCommit(commit); err != nil {
			attempt++
			log.Debugf("delta-go: Transaction attempt failed with '%v'. Incrementing attempt number to %d and retrying.", err, attempt)

			// TODO: check if state is higher than current latest version of table by checking n-1
			if attempt%int(transaction.Options.RetryCommitAttemptsBeforeLoadingTable) == 0 {
				// Every 100 attempts, sync the table's state store with its latest version.
				if err := transaction.Table.SyncStateStore(); err != nil {
					log.Debugf("delta-go: Table.SyncStateStore() failed with '%v'.", err)
				}
			}
		}
	}
}

// TryCommitLoop: Loads metadata from lock containing the latest locked version and tries to obtain the lock and commit for the version + 1 in a loop
func (transaction *Transaction) TryCommit(commit *PreparedCommit) (err error) {
	// Step 1) Acquire Lock
	locked, err := transaction.Table.LockClient.TryLock()
	// Step 5) Always Release Lock
	// defer transaction.Table.LockClient.Unlock()
	defer func() {
		// Defer the unlock and overwrite any errors if unlock fails
		if unlockErr := transaction.Table.LockClient.Unlock(); unlockErr != nil {
			log.Debugf("delta-go: Unlock attempt failed. %v", unlockErr)
			err = unlockErr
		}
	}()
	if err != nil {
		log.Debugf("delta-go: Lock attempt failed. %v", err)
		return errors.Join(lock.ErrLockNotObtained, err)
	}

	if locked {
		// 2) Lookup the latest prior state
		priorState, err := transaction.Table.StateStore.Get()
		if err != nil {
			// Failed on state store get, fallback to using the local version
			log.Debugf("delta-go: StateStore Get() attempt failed. %v", err)
			// return max(remoteVersion, version), err
		}

		// 4) Update the state with the latest tried, even in the case that the
		// RenameNotExists was unsuccessful, this ensures that the next try increments the version
		// Take the max of the local state and remote state version in the case that the remote state is not accessible.
		version := max(priorState.Version, transaction.Table.State.Version) + 1
		transaction.Table.State.Version = version
		newState := state.CommitState{
			Version: version,
		}
		defer func() {
			if putErr := transaction.Table.StateStore.Put(newState); putErr != nil {
				log.Debugf("delta-go: StateStore Put() attempt failed. %v", putErr)
				err = putErr
			}
		}()

		// 3) Try to Rename the file
		from := storage.NewPath(commit.URI.Raw)
		to := CommitURIFromVersion(version)
		err = transaction.Table.Store.RenameIfNotExists(from, to)
		if errors.Is(err, storage.ErrCopyObject) {
			version = max(priorState.Version, transaction.Table.State.Version)
			transaction.Table.State.Version = version
			newState = state.CommitState{
				Version: version,
			}
		}
		if err != nil && !errors.Is(err, storage.ErrDeleteObject) {
			log.Debugf("delta-go: RenameIfNotExists(from=%s, to=%s) attempt failed. %v", from.Raw, to.Raw, err)
			return err
		}
	} else {
		log.Debug("delta-go: Lock not obtained")
		return errors.Join(lock.ErrLockNotObtained, err)
	}
	return nil
}

func max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

// Holds the uri to prepared commit temporary file created with `Transaction.prepare_commit`.
// Once created, the actual commit could be executed with `Transaction.try_commit`.
type PreparedCommit struct {
	URI storage.Path
}

const (
	defaultMaxRetryCommitAttempts                uint32 = 10000000
	defaultMaxWriteCommitAttempts                uint32 = 10000000
	defaultRetryCommitAttemptsBeforeLoadingTable uint32 = 100
	defaultMaxRetryLogFixAttempts                uint16 = 3
)

// Options for customizing behavior of a `Transaction`
type TransactionOptions struct {
	// number of retry attempts allowed when committing a transaction
	MaxRetryCommitAttempts uint32
	// RetryWaitDuration sets the amount of times between retry's on the transaction
	RetryWaitDuration time.Duration
	// Number of retry attempts allowed when reading actions from a log entry
	MaxRetryReadAttempts uint32
	// Number of retry attempts allowed when writing actions to a log entry
	MaxRetryWriteAttempts uint32
	// number of retry commit attempts before loading the latest version from the table rather
	// than using the state store
	RetryCommitAttemptsBeforeLoadingTable uint32
	// Number of retry attempts allowed when fixing the Delta log
	MaxRetryLogFixAttempts uint16
}

// NewTransactionOptions sets the default transaction options.
func NewTransactionOptions() *TransactionOptions {
	return &TransactionOptions{MaxRetryCommitAttempts: defaultMaxRetryCommitAttempts, MaxRetryWriteAttempts: defaultMaxWriteCommitAttempts, MaxRetryLogFixAttempts: defaultMaxRetryLogFixAttempts, RetryCommitAttemptsBeforeLoadingTable: defaultRetryCommitAttemptsBeforeLoadingTable}
}

// OpenTableWithVersion loads the table at this specific version
// If the table reader or writer version is greater than the client supports, the table will still be opened, but an error will also be returned
func OpenTableWithVersion(store storage.ObjectStore, lock lock.Locker, stateStore state.StateStore, version int64) (*Table, error) {
	return OpenTableWithVersionAndConfiguration(store, lock, stateStore, version, nil)
}

// OpenTableWithVersionAndConfiguration loads the table at this specific version using the given configuration for optimization settings
func OpenTableWithVersionAndConfiguration(store storage.ObjectStore, lock lock.Locker, stateStore state.StateStore, version int64, config *OptimizeCheckpointConfiguration) (*Table, error) {
	return openTableWithVersionAndConfiguration(store, lock, stateStore, version, config, true)
}

func openTableWithVersionAndConfiguration(store storage.ObjectStore, lock lock.Locker, stateStore state.StateStore, version int64, config *OptimizeCheckpointConfiguration, cleanupWorkingStorage bool) (*Table, error) {
	table := NewTable(store, lock, stateStore)
	err := table.loadVersionWithConfiguration(&version, config, cleanupWorkingStorage)
	if err != nil {
		return nil, err
	}

	if table.State.MinReaderVersion > maxReaderVersionSupported {
		err = errors.Join(ErrUnsupportedReaderVersion, fmt.Errorf("table minimum reader version %d, max supported reader version %d", table.State.MinReaderVersion, maxReaderVersionSupported))
	}

	if table.State.MinWriterVersion > maxWriterVersionSupported {
		err = errors.Join(err, ErrUnsupportedWriterVersion, fmt.Errorf("table minimum writer version %d, max supported writer version %d", table.State.MinWriterVersion, maxWriterVersionSupported))
	}

	return table, err
}

// OpenTable loads the latest version of the table
// If the table reader or writer version is greater than the client supports, the table will still be opened, but an error will also be returned
func OpenTable(store storage.ObjectStore, lock lock.Locker, stateStore state.StateStore) (*Table, error) {
	return OpenTableWithConfiguration(store, lock, stateStore, nil)
}

// OpenTableWithConfiguration loads the latest version of the table, using the given configuration for optimization settings
func OpenTableWithConfiguration(store storage.ObjectStore, lock lock.Locker, stateStore state.StateStore, config *OptimizeCheckpointConfiguration) (*Table, error) {
	table := NewTable(store, lock, stateStore)
	err := table.LoadVersionWithConfiguration(nil, config)
	if err != nil {
		return nil, err
	}

	if table.State.MinReaderVersion > maxReaderVersionSupported {
		err = ErrUnsupportedReaderVersion
	}

	if table.State.MinWriterVersion > maxWriterVersionSupported {
		err = errors.Join(err, ErrUnsupportedWriterVersion)
	}

	return table, err
}
