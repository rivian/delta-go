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

// Package delta contains the resources required to interact with a Delta table.
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

const (
	clientVersion             = "alpha-0.0.0"
	maxReaderVersionSupported = 1
	maxWriterVersionSupported = 1
)

var (
	// ErrExceededCommitRetryAttempts is returned when the maximum number of commit retry attempts has been exceeded.
	ErrExceededCommitRetryAttempts error = errors.New("exceeded commit retry attempts")
	// ErrNotATable is returned when a Delta table is not valid.
	ErrNotATable error = errors.New("not a table")
	// ErrInvalidVersion is returned when a version is invalid.
	ErrInvalidVersion error = errors.New("invalid version")
	// ErrUnableToLoadVersion is returned when a version cannot be loaded.
	ErrUnableToLoadVersion error = errors.New("unable to load specified version")
	// ErrLockFailed is returned a lock fails unexpectedly.
	ErrLockFailed error = errors.New("lock failed unexpectedly without an error")
	// ErrNotImplemented is returned when a feature has not been implemented.
	ErrNotImplemented error = errors.New("not implemented")
	// ErrUnsupportedReaderVersion is returned when a reader version is unsupported.
	ErrUnsupportedReaderVersion error = errors.New("reader version is unsupported")
	// ErrUnsupportedWriterVersion is returned when a writer version is unsupported.
	ErrUnsupportedWriterVersion error = errors.New("writer version is unsupported")
	// ErrFailedToCopyTempFile is returned when a temp file fails to be copied into a commit URI.
	ErrFailedToCopyTempFile error = errors.New("failed to copy temp file")
	// ErrFailedToAcknowledgeCommit is returned when a commit fails to be acknowledged.
	ErrFailedToAcknowledgeCommit error = errors.New("failed to acknowledge commit")
)

var (
	commitFileRegex         *regexp.Regexp = regexp.MustCompile(`^(\d{20}).json$`)
	checkpointRegex         *regexp.Regexp = regexp.MustCompile(`^(\d{20})\.checkpoint\.parquet$`)
	checkpointPartsRegex    *regexp.Regexp = regexp.MustCompile(`^(\d{20})\.checkpoint\.(\d{10})\.(\d{10})\.parquet$`)
	commitOrCheckpointRegex *regexp.Regexp = regexp.MustCompile(`^(\d{20})\.((json$)|(checkpoint\.parquet$)|(checkpoint\.(\d{10})\.(\d{10})\.parquet$))`)
)

// Table represents a Delta table.
type Table struct {
	// The state of the table as of the most recent loaded Delta log entry.
	State TableState
	// The remote store of the state of the table as of the most recent loaded Delta log entry.
	StateStore state.Store
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

// NewTable creates a new Table struct without loading any data from backing storage.
//
// NOTE: This is for advanced users. If you don't know why you need to use this method, please
// call one of the `open_table` helper methods instead.
func NewTable(store storage.ObjectStore, lock lock.Locker, stateStore state.Store) *Table {
	table := new(Table)
	table.Store = store
	table.StateStore = stateStore
	table.LockClient = lock
	table.LastCheckPoint = nil
	table.State = *NewTableState(-1)
	return table
}

// NewTableWithLogStore creates a new Table instance with a log store configured.
func NewTableWithLogStore(store storage.ObjectStore, lock lock.Locker, logStore logstore.LogStore) *Table {
	t := new(Table)
	t.State = *NewTableState(-1)
	t.Store = store
	t.LockClient = lock
	t.LastCheckPoint = nil
	t.LogStore = logStore

	return t
}

// CreateTransaction creates a new Transaction for the Table.
// The transaction holds a mutable reference to the Table, preventing other references
// until the transaction is dropped.
func (t *Table) CreateTransaction(opts TransactionOptions) *Transaction {
	opts.setOptionsDefaults()

	transaction := new(Transaction)
	transaction.Table = t
	transaction.options = opts

	return transaction
}

// setOptionsDefaults sets the default transaction options.
func (o *TransactionOptions) setOptionsDefaults() {
	if o.MaxRetryCommitAttempts == 0 {
		o.MaxRetryCommitAttempts = defaultMaxRetryCommitAttempts
	}
	if o.MaxRetryReadAttempts == 0 {
		o.MaxRetryReadAttempts = defaultMaxReadAttempts
	}
	if o.MaxRetryWriteAttempts == 0 {
		o.MaxRetryWriteAttempts = defaultMaxWriteAttempts
	}
	if o.RetryCommitAttemptsBeforeLoadingTable == 0 {
		o.RetryCommitAttemptsBeforeLoadingTable = defaultRetryCommitAttemptsBeforeLoadingTable
	}
	if o.MaxRetryLogFixAttempts == 0 {
		o.MaxRetryLogFixAttempts = defaultMaxRetryLogFixAttempts
	}
}

// CommitURIFromVersion returns the URI of commit version.
func CommitURIFromVersion(version int64) storage.Path {
	str := fmt.Sprintf("%020d.json", version)
	path := storage.PathFromIter([]string{"_delta_log", str})
	return path
}

// BaseCommitURI returns the base path of a commit URI.
func BaseCommitURI() storage.Path {
	return storage.NewPath("_delta_log/")
}

// IsValidCommitURI returns true if a URI is a valid commit filename (not a checkpoint file, and not a temp commit).
func IsValidCommitURI(path storage.Path) bool {
	match := commitFileRegex.MatchString(path.Base())
	return match
}

// IsValidCommitOrCheckpointURI returns true if a URI is a valid commit or checkpoint file name.
// Otherwise, it returns false.
func IsValidCommitOrCheckpointURI(path storage.Path) bool {
	return commitFileRegex.MatchString(path.Base()) || commitOrCheckpointRegex.MatchString(path.Base())
}

// CommitVersionFromURI returns true plus the version if the URI is a valid commit filename.
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

// CommitOrCheckpointVersionFromURI returns true plus the version if the URI is a valid commit or checkpoint filename.
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

// Create creates a Table with version 0 given the provided MetaData, Protocol, and CommitInfo.
// Note that if the protocol MinReaderVersion or MinWriterVersion is too high, the table will be created
// and then an error will be returned
func (t *Table) Create(metadata TableMetaData, protocol Protocol, commitInfo CommitInfo, addActions []Add) error {
	meta := metadata.toMetaData()

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

	transaction := t.CreateTransaction(NewTransactionOptions())
	transaction.AddActions(actions)

	if t.LogStore != nil {
		version, err = transaction.CommitLogStore()
		if err != nil {
			return err
		}
	} else {
		preparedCommit, err := transaction.prepareCommit()
		if err != nil {
			return err
		}
		//Set StateStore Version=-1 synced with the table State Version
		zeroState := state.CommitState{
			Version: t.State.Version,
		}
		if err := transaction.Table.StateStore.Put(zeroState); err != nil {
			return errors.Join(fmt.Errorf("failed to add commit state with version %d to state store", zeroState.Version), err)
		}

		if err := transaction.tryCommit(&preparedCommit); err != nil {
			return err
		}

		version = t.State.Version
	}

	// Merge state from new commit version
	newState, err := NewTableStateFromCommit(t, version)
	if err != nil {
		return err
	}
	if err := t.State.merge(newState, 150000, nil, false); err != nil && !errors.Is(err, ErrVersionOutOfOrder) {
		return errors.Join(errors.New("failed to merge new state into existing state"), err)
	}

	// If either version is too high, we return an error, but we still create the table first
	if protocol.MinReaderVersion > maxReaderVersionSupported {
		err = ErrUnsupportedReaderVersion
	}
	if protocol.MinWriterVersion > maxWriterVersionSupported {
		err = errors.Join(err, ErrUnsupportedWriterVersion)
	}

	return err
}

// Exists checks if a Table with version 0 exists in the object store.
func (t *Table) Exists() (bool, error) {
	path := CommitURIFromVersion(0)

	meta, err := t.Store.Head(path)
	if errors.Is(err, storage.ErrObjectDoesNotExist) {
		// Fallback: check for other variants of the version
		logIterator := storage.NewListIterator(BaseCommitURI(), t.Store)
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

// ReadCommitVersion retrieves the actions from a commit log.
func (t *Table) ReadCommitVersion(version int64) ([]Action, error) {
	path := CommitURIFromVersion(version)
	transaction := t.CreateTransaction(NewTransactionOptions())
	return transaction.ReadActions(path)
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
				return -1, errors.Join(errors.New("failed to list Delta log"), err)
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
		checkpoint, err := checkpointFromBytes(bytes)
		if err != nil {
			return -1, errors.Join(errors.New("failed to get checkpoint from bytes"), err)
		}

		minVersion = checkpoint.Version
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

// syncStateStore syncs the table's state store with its latest version.
func (t *Table) syncStateStore() (err error) {
	locked, err := t.LockClient.TryLock()
	if err != nil {
		return errors.Join(lock.ErrLockNotObtained, err)
	}
	defer func() {
		// Defer the unlock and overwrite any errors if the unlock fails.
		if unlockErr := t.LockClient.Unlock(); unlockErr != nil {
			err = errors.Join(lock.ErrUnableToUnlock, err)
		}
	}()

	if locked {
		version, err := t.LatestVersion()
		if err != nil {
			return errors.Join(errors.New("failed to get latest version"), err)
		}

		state := state.CommitState{
			Version: version,
		}
		if err := t.StateStore.Put(state); err != nil {
			return errors.Join(errors.New("failed to put state"), err)
		}
	}

	return nil
}

// Load loads the table state using the given configuration
func (t *Table) Load(config *OptimizeCheckpointConfiguration) error {
	return t.LoadVersionWithConfiguration(nil, config)
}

// LoadVersion loads the table state at the specified version using default configuration options
func (t *Table) LoadVersion(version *int64) error {
	return t.LoadVersionWithConfiguration(version, nil)
}

// LoadVersionWithConfiguration loads the table state at the specified version using the given configuration
func (t *Table) LoadVersionWithConfiguration(version *int64, config *OptimizeCheckpointConfiguration) error {
	return t.loadVersionWithConfiguration(version, config, true)
}

func (t *Table) loadVersionWithConfiguration(version *int64, config *OptimizeCheckpointConfiguration, cleanupWorkingStorage bool) (returnErr error) {
	t.LastCheckPoint = nil
	t.State = *NewTableState(-1)
	err := setupOnDiskOptimization(config, &t.State, 0)
	if err != nil {
		return err
	}
	if cleanupWorkingStorage && t.State.onDiskOptimization {
		defer func() {
			if err := config.WorkingStore.DeleteFolder(config.WorkingFolder); err != nil {
				returnErr = errors.Join(fmt.Errorf("failed to delete folder %s", config.WorkingFolder), err)
			}
		}()
	}
	var checkpointLoadError error
	if version != nil {
		commitURI := CommitURIFromVersion(*version)
		_, err := t.Store.Head(commitURI)
		if errors.Is(err, storage.ErrObjectDoesNotExist) {
			return ErrInvalidVersion
		}
		if err != nil {
			return err
		}
	}
	checkpoints, allReturned, err := t.findLatestCheckpointsForVersion(version)
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
		err = t.restoreCheckpoint(&checkpoints[checkpointIndex], config)
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
				checkpoints, allReturned, err = t.findLatestCheckpointsForVersion(&prevVersion)
				if err != nil {
					return err
				}
			} else {
				break
			}
		}
	}

	err = t.updateIncremental(version, config)
	if err != nil {
		// If we happened to get both a checkpoint read error and an incremental load error, it may be helpful to return both
		return errors.Join(err, checkpointLoadError)
	}
	// If there was no error but we failed to load the specified version, return error indicating that
	if version != nil && t.State.Version != *version {
		return errors.Join(ErrUnableToLoadVersion, checkpointLoadError)
	}
	return nil
}

// Find the most recent checkpoint(s) at or before the given version
// If we are returning all checkpoints at or before the version, allReturned will be true, otherwise it will be false
// If we are able to use the _last_checkpoint to retrieve the checkpoint then we will just return that one, and set allReturned to false
// If we need to search through the directory for checkpoints, then allReturned will be true if the listing is ordered and false otherwise
func (t *Table) findLatestCheckpointsForVersion(version *int64) (checkpoints []CheckPoint, allReturned bool, err error) {
	// First check if _last_checkpoint exists and is prior to the desired version
	var errReadingLastCheckpoint error
	path := lastCheckpointPath()
	lastCheckpointBytes, err := t.Store.Get(path)
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

	logIterator := storage.NewListIterator(BaseCommitURI(), t.Store)

	foundCheckpoints := make([]CheckPoint, 0, 20)
	listResultsAreOrdered := t.Store.IsListOrdered()

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
				isCompleteCheckpoint, err = DoesCheckpointVersionExist(t.Store, checkpoint.Version, true)
				if err != nil && !errors.Is(err, ErrCheckpointIncomplete) && !errors.Is(err, ErrCheckpointInvalidMultipartFileName) {
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
func (t *Table) GetCheckpointDataPaths(checkpoint *CheckPoint) []storage.Path {
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
func (t *Table) restoreCheckpoint(checkpoint *CheckPoint, config *OptimizeCheckpointConfiguration) error {
	state, err := stateFromCheckpoint(t, checkpoint, config)
	if err != nil {
		return err
	}
	t.State = *state
	return nil
}

// Updates the Table to the latest version by incrementally applying newer versions.
// It assumes that the table is already updated to the current version `self.version`.
// This function does not look for checkpoints
func (t *Table) updateIncremental(maxVersion *int64, config *OptimizeCheckpointConfiguration) error {
	for {
		if maxVersion != nil && t.State.Version == *maxVersion {
			break
		}

		nextCommitVersion, nextCommitActions, noMoreCommits, err := t.nextCommitDetails()
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
		err = t.State.merge(newState, 150000, config, false)
		if err != nil {
			return err
		}
	}

	if t.State.Version == -1 {
		return ErrInvalidVersion
	}
	if t.State.onDiskOptimization {
		// We need to do one final "merge" to resolve any remaining adds/removes in our buffer
		err := t.State.merge(nil, 150000, config, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// Get the actions inside the next commit log if it exists and return the next commit's version and its actions
// If the next commit doesn't exist, returns false in the third return parameter
func (t *Table) nextCommitDetails() (int64, []Action, bool, error) {
	nextVersion := t.State.Version + 1
	nextCommitURI := CommitURIFromVersion(nextVersion)
	noMoreCommits := false
	transaction := t.CreateTransaction(NewTransactionOptions())
	actions, err := transaction.ReadActions(nextCommitURI)
	if err != nil {
		noMoreCommits = true
		err = nil
	}
	return nextVersion, actions, noMoreCommits, err
}

// CreateCheckpoint creates a checkpoint for this table at the given version
// The existing table state will not be used or modified; a new table instance will be opened at the checkpoint version
// Returns whether the checkpoint was created and any error
// If the lock cannot be obtained, does not retry
func (t *Table) CreateCheckpoint(checkpointLock lock.Locker, checkpointConfiguration *CheckpointConfiguration, version int64) (bool, error) {
	return CreateCheckpoint(t.Store, checkpointLock, checkpointConfiguration, version)
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
		defer func() {
			if deleteErr := checkpointConfiguration.ReadWriteConfiguration.WorkingStore.DeleteFolder(checkpointConfiguration.ReadWriteConfiguration.WorkingFolder); deleteErr != nil {
				err = errors.Join(fmt.Errorf("failed to delete folder %s", checkpointConfiguration.ReadWriteConfiguration.WorkingFolder), deleteErr)
			}
		}()
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

// Cleanup expired logs before the given checkpoint version, after confirming there is a readable checkpoint
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

// TableMetaData represents the metadata of a Delta table.
type TableMetaData struct {
	// Unique identifier for this table
	ID uuid.UUID
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

// NewTableMetaData creates a new TableMetaData instance.
func NewTableMetaData(name string, description string, format Format, schema Schema, partitionColumns []string, configuration map[string]string) *TableMetaData {
	// Reference implementation uses uuid v4 to create GUID:
	// https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala#L350
	metaData := new(TableMetaData)
	metaData.ID = uuid.New()
	metaData.Name = name
	metaData.Description = description
	metaData.Format = format
	metaData.Schema = schema
	metaData.PartitionColumns = partitionColumns
	metaData.CreatedTime = time.Now()
	metaData.Configuration = configuration
	return metaData

}

// toMetaData converts a TableMetaData instance to a MetaData instance.
func (md *TableMetaData) toMetaData() MetaData {
	createdTime := md.CreatedTime.UnixMilli()
	metadata := MetaData{
		ID:               md.ID,
		IDAsString:       md.ID.String(),
		Name:             &md.Name,
		Description:      &md.Description,
		Format:           md.Format,
		SchemaString:     string(md.Schema.JSON()),
		PartitionColumns: md.PartitionColumns,
		CreatedTime:      &createdTime,
		Configuration:    md.Configuration,
	}
	return metadata
}

// Transaction represents a Delta transaction.
// Clients that do not need to mutate action content in case a Transaction conflict is encountered
// may use the `commit` method and rely on optimistic concurrency to determine the
// appropriate Delta version number for a commit. A good example of this type of client is an
// append only client that does not need to maintain Transaction state with external systems.
// Clients that may need to do conflict resolution if the Delta version changes should use
// the `prepare_commit` and `try_commit_transaction` methods and manage the Delta version
// themselves so that they can resolve data conflicts that may occur between Delta versions.
//
// Please not that in case of non-retryable error the temporary commit file such as
// `_delta_log/_commit_<uuid>.json` will orphaned in storage.
type Transaction struct {
	Table       *Table
	Actions     []Action
	Operation   Operation
	AppMetadata map[string]any
	options     TransactionOptions
}

// ReadActions gets actions from a file.
//
// With many concurrent readers/writers, there's a chance that concurrent recovery
// operations occur on the same file, i.e. the same temp file T(N) is copied into the
// target N.json file more than once. Though data loss will *NOT* occur, readers of N.json
// may receive an error from S3 as the ETag of N.json was changed. This is safe to
// retry, so we do so here.
func (t *Transaction) ReadActions(path storage.Path) ([]Action, error) {
	var (
		entry   []byte
		err     error
		attempt = 0
	)
	for {
		if attempt >= int(t.options.MaxRetryReadAttempts) {
			return nil, errors.Join(fmt.Errorf("failed to get actions after %d attempts", t.options.MaxRetryReadAttempts), err)
		}

		entry, err = t.Table.Store.Get(path)
		if err != nil {
			attempt++
			log.Debugf("delta-go: Attempt number %d: failed to get log entry. %v", attempt, err)
			continue
		}

		actions, err := ActionsFromLogEntries(entry)
		if err != nil {
			attempt++
			log.Debugf("delta-go: Attempt number %d: failed to get actions from log entry. %v", attempt, err)
			continue
		}

		return actions, nil
	}
}

// CommitLogStore writes actions to a file.
//
// To commit for Delta version N:
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
func (t *Transaction) CommitLogStore() (int64, error) {
	var (
		version int64
		err     error
		attempt = 0
	)
	for {
		if attempt >= int(t.options.MaxRetryWriteAttempts) {
			return -1, errors.Join(fmt.Errorf("failed to commit with log store after %d attempts", t.options.MaxRetryWriteAttempts), err)
		}

		version, err = t.tryCommitLogStore()
		if errors.Is(err, ErrFailedToCopyTempFile) || errors.Is(err, ErrFailedToAcknowledgeCommit) {
			return version, err
		} else if err != nil {
			attempt++
			log.Debugf("delta-go: Attempt number %d: failed to commit with log store. %v", attempt, err)

			time.Sleep(t.options.RetryWaitDuration)

			continue
		}

		return version, nil
	}
}

func (t *Transaction) tryCommitLogStore() (version int64, err error) {
	var currURI storage.Path
	prevURI, err := t.Table.LogStore.Latest(t.Table.Store.BaseURI())
	if errors.Is(err, logstore.ErrLatestDoesNotExist) {
		if version, err := t.Table.LatestVersion(); err == nil {
			var (
				uri      = CommitURIFromVersion(version).Raw
				fileName = storage.NewPath(strings.Split(uri, "_delta_log/")[1])
				seconds  = t.Table.LogStore.ExpirationDelaySeconds()
			)

			entry := logstore.New(t.Table.Store.BaseURI(), fileName,
				storage.NewPath("") /* tempPath */, true /* isComplete */, uint64(time.Now().Unix())+seconds)

			if err := t.Table.LogStore.Put(entry, false); err != nil {
				return -1, errors.Join(errors.New("failed to put first commit entry"), err)
			}
			log.WithFields(log.Fields{"tablePath": t.Table.Store.BaseURI().Raw, "fileName": fileName.Raw}).
				Infof("delta-go: Put completed commit entry in empty log store")

			currURI = CommitURIFromVersion(version + 1)
		} else if errors.Is(err, ErrNotATable) {
			currURI = CommitURIFromVersion(0)
		} else {
			return -1, errors.Join(fmt.Errorf("failed to determine if table %s exists", t.Table.Store.BaseURI().Raw), err)
		}
	} else if err != nil {
		return -1, errors.Join(errors.New("failed to get latest log store entry"), err)
	} else {
		parsed, prevVersion := CommitVersionFromURI(prevURI.FileName())
		if !parsed {
			return -1, fmt.Errorf("failed to parse previous version from %s", prevURI.FileName().Raw)
		}

		currURI = CommitURIFromVersion(prevVersion + 1)
	}

	t.addCommitInfoIfNotPresent()

	// Prevent concurrent writers from checking if N-1.json exists and performing a recovery
	// where they all copy T(N-1) into N-1.json. Note that the mutual exclusion on writing
	// into N.json from different machines is provided by the log store, not by this lock.
	// Also note that this lock is for N.json, while the lock used during a recovery is for
	// N-1.json. Thus, there is no deadlock.
	versionLock, err := t.Table.LockClient.NewLock(t.Table.Store.BaseURI().Join(currURI).Raw)
	if err != nil {
		return -1, errors.Join(errors.New("failed to create lock"), err)
	}
	if _, err = versionLock.TryLock(); err != nil {
		return -1, errors.Join(lock.ErrLockNotObtained, err)
	}
	defer func() {
		// Defer the unlock and overwrite any errors if the unlock fails.
		if unlockErr := versionLock.Unlock(); unlockErr != nil {
			err = unlockErr
		}
	}()

	fileName := storage.NewPath(strings.Split(currURI.Raw, "_delta_log/")[1])

	parsed, currVersion := CommitVersionFromURI(currURI)
	if !parsed {
		return -1, fmt.Errorf("failed to parse previous version from %s", currURI.Raw)
	}

	// Step 0: Fail if N.json already exists in the file system.
	if _, err = t.Table.Store.Head(currURI); err == nil {
		return -1, errors.New("current version already exists")
	}

	// Step 1: Ensure that N-1.json exists.
	if currVersion > 0 {
		prevFileName := storage.NewPath(strings.Split(CommitURIFromVersion(currVersion-1).Raw, "_delta_log/")[1])

		if prevEntry, err := t.Table.LogStore.Get(t.Table.Store.BaseURI(), prevFileName); err != nil {
			return -1, errors.Join(errors.New("failed to get previous commit"), err)
		} else if !prevEntry.IsComplete() {
			if err := t.fixLog(prevEntry); err != nil {
				return -1, errors.Join(errors.New("failed to fix Delta log"), err)
			}
		}
	} else {
		if entry, err := t.Table.LogStore.Get(t.Table.Store.BaseURI(), fileName); err == nil {
			if _, err := t.Table.Store.Head(currURI); entry.IsComplete() && err != nil {
				return -1, fmt.Errorf("old entries for table %s still in log store", t.Table.Store.BaseURI().Raw)
			}
		}
	}

	// Step 2: PREPARE the commit.
	tempPath, err := t.createTempPath(fileName)
	if err != nil {
		return -1, errors.Join(errors.New("failed to create temp path"), err)
	}

	var (
		entry            = logstore.New(t.Table.Store.BaseURI(), fileName, tempPath, false /* isComplete */, 0 /* expirationTime */)
		relativeTempPath = entry.RelativeTempPath()
	)

	if err := t.writeActions(relativeTempPath, t.Actions); err != nil {
		return -1, errors.Join(errors.New("failed to write actions"), err)
	}

	// Step 2.2: Create uncompleted commit entry E(N, T(N)).
	if err := t.Table.LogStore.Put(entry, false); err != nil {
		return -1, errors.Join(errors.New("failed to put uncompleted commit entry"), err)
	}

	// Step 3: COMMIT the commit to the Delta log.
	//         Copy T(N) -> N.json.
	if err := t.copyTempFile(relativeTempPath, currURI); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
		return -1, errors.Join(ErrFailedToCopyTempFile,
			errors.Join(fmt.Errorf("failed to copy %s to %s", relativeTempPath.Raw, currURI.Raw), err))
	}

	// Step 4: ACKNOWLEDGE the commit.
	if err := t.complete(entry); err != nil {
		return currVersion, errors.Join(ErrFailedToAcknowledgeCommit, err)
	}

	return currVersion, nil
}

func (t *Transaction) complete(entry *logstore.CommitEntry) error {
	seconds := t.Table.LogStore.ExpirationDelaySeconds()
	entry = entry.Complete(seconds)

	if err := t.Table.LogStore.Put(entry, true); err != nil {
		return errors.Join(errors.New("failed to put completed commit entry"), err)
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

	versionLock, err := t.Table.LockClient.NewLock(entry.AbsoluteFilePath().Raw)
	if err != nil {
		return errors.Join(errors.New("failed to create lock"), err)
	}
	if _, err = versionLock.TryLock(); err != nil {
		return errors.Join(lock.ErrLockNotObtained, err)
	}
	defer func() {
		// Defer the unlock and overwrite any errors if the unlock fails.
		if unlockErr := versionLock.Unlock(); err != nil {
			err = unlockErr
		}
	}()

	var (
		attempt          = 0
		relativeFilePath = entry.RelativeFilePath()
	)
	for {
		if attempt >= int(t.options.MaxRetryLogFixAttempts) {
			return errors.Join(fmt.Errorf("failed to fix Delta log after %d attempts", t.options.MaxRetryLogFixAttempts), err)
		}

		log.WithField("tablePath", entry.TablePath().Raw).Infof("delta-go: Trying to fix %s", entry.FileName().Raw)

		if _, err = t.Table.Store.Head(relativeFilePath); err != nil {
			relativeTempPath := entry.RelativeTempPath()

			if err := t.copyTempFile(relativeTempPath, relativeFilePath); err != nil && !errors.Is(err, storage.ErrObjectAlreadyExists) {
				attempt++
				log.Debugf("delta-go: Attempt number %d: failed to copy %s to %s. %v", attempt, relativeTempPath.Raw, relativeFilePath.Raw, err)
				continue
			}
		}

		if err := t.complete(entry); err != nil {
			attempt++
			log.Debugf("delta-go: Attempt number %d: failed to complete commit entry. %v", attempt, err)
			continue
		}

		log.WithField("tablePath", entry.TablePath().Raw).Infof("delta-go: Fixed %s", entry.FileName().Raw)
		return nil
	}
}

func (t *Transaction) writeActions(path storage.Path, actions []Action) error {
	// Serialize all actions that are part of this log entry.
	entry, err := LogEntryFromActions(actions)
	if err != nil {
		return errors.New("failed to serialize actions")
	}

	return t.Table.Store.Put(path, entry)
}

// AddAction adds an arbitrary "action" to the actions associated with this transaction.
func (t *Transaction) AddAction(action Action) {
	t.Actions = append(t.Actions, action)
}

// AddActions adds an arbitrary number of actions to the actions associated with this transaction.
func (t *Transaction) AddActions(actions []Action) {
	t.Actions = append(t.Actions, actions...)
}

// addCommitInfoIfNotPresent adds a `commitInfo` action to a transaction's actions if not already present.
func (t *Transaction) addCommitInfoIfNotPresent() {
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
func (t *Transaction) SetOperation(operation Operation) {
	t.Operation = operation
}

// SetAppMetadata sets the app metadata for this transaction.
func (t *Transaction) SetAppMetadata(appMetadata map[string]any) {
	t.AppMetadata = appMetadata
}

// Commit commits the given actions to the Delta log.
// This method will retry the transaction commit based on the value of `max_retry_commit_attempts` set in `TransactionOptions`.
func (t *Transaction) Commit() (int64, error) {
	PreparedCommit, err := t.prepareCommit()
	if err != nil {
		log.Debugf("delta-go: PrepareCommit attempt failed. %v", err)
		return t.Table.State.Version, err
	}

	err = t.tryCommitLoop(&PreparedCommit)
	return t.Table.State.Version, err
}

// Low-level transaction API. Creates a temporary commit file. Once created,
// the transaction object could be dropped and the actual commit could be executed
// with `Table.try_commit_transaction`.
func (t *Transaction) prepareCommit() (PreparedCommit, error) {
	t.addCommitInfoIfNotPresent()

	// Serialize all actions that are part of this log entry.
	logEntry, err := LogEntryFromActions(t.Actions)
	if err != nil {
		return PreparedCommit{}, nil
	}

	// Write Delta log entry as temporary file to storage. For the actual commit,
	// the temporary file is moved (atomic rename) to the Delta log folder within `commit` function.
	fileName := fmt.Sprintf("_commit_%s.json", uuid.NewString())
	// TODO: Open question, should storagePath use the basePath for the transaction or just hard code the _delta_log path?
	path := storage.Path{Raw: filepath.Join("_delta_log", ".tmp", fileName)}
	commit := PreparedCommit{URI: path}

	err = t.Table.Store.Put(path, logEntry)
	if err != nil {
		return commit, err
	}

	return commit, nil
}

// tryCommitLoop continues to iterate until a temp file is successfully copied into a commit URI or the
// maximum number of attempts have been exhausted.
func (t *Transaction) tryCommitLoop(commit *PreparedCommit) error {
	var (
		err     error
		attempt = 0
	)
	for {
		if attempt >= int(t.options.MaxRetryCommitAttempts) {
			return errors.Join(ErrExceededCommitRetryAttempts,
				fmt.Errorf("failed to commit after %d attempts", t.options.MaxRetryCommitAttempts), err)
		}

		err = t.tryCommit(commit)
		if err == nil {
			return nil
		}

		attempt++
		log.Debugf("delta-go: Attempt number %d: failed to commit. %v", attempt, err)

		// TODO: Check if the state store version exists. If it doesn't, the state store needs to be
		// synced with the table's latest version before attempting to commit.
		if attempt%int(t.options.RetryCommitAttemptsBeforeLoadingTable) == 0 {
			// Sync the table's state store with its latest version.
			_ = t.Table.syncStateStore()
		}

		time.Sleep(t.options.RetryWaitDuration)
	}
}

func (t *Transaction) tryCommit(commit *PreparedCommit) (err error) {
	// 1) Acquire the lock.
	locked, err := t.Table.LockClient.TryLock()
	defer func() {
		// Defer the unlock and overwrite any errors if the unlock fails.
		if unlockErr := t.Table.LockClient.Unlock(); unlockErr != nil {
			err = errors.Join(lock.ErrUnableToUnlock, unlockErr)
		}
	}()
	if err != nil || !locked {
		return errors.Join(lock.ErrLockNotObtained, err)
	}

	// 2) Look up the prior state.
	priorState, _ := t.Table.StateStore.Get()

	// 3) Compute the maximum of the local and remote state's versions to find the table's latest version.
	version := max(priorState.Version, t.Table.State.Version) + 1
	t.Table.State.Version = version
	newState := state.CommitState{
		Version: version,
	}
	incrementVersion := true
	defer func() {
		if incrementVersion {
			if putErr := t.Table.StateStore.Put(newState); putErr != nil {
				err = errors.Join(errors.New("failed to increment state store version"), putErr)
			}
		}
	}()

	// 4) Try to copy the temp file into the commit URI.
	from := commit.URI
	to := CommitURIFromVersion(version)
	if err := t.Table.Store.RenameIfNotExists(from, to); errors.Is(err, storage.ErrCopyObject) {
		// The temp file wasn't successfully copied into the commit URI, so the current version still
		// needs to be committed.
		incrementVersion = false
		return errors.Join(errors.New("failed to copy temp file into commit URI"), err)
	} else if errors.Is(err, storage.ErrObjectAlreadyExists) {
		// The state store version will be incremented since the current version has been committed.
		return errors.Join(errors.New("failed to copy temp file into commit URI"), err)
	}

	// The current version has still been committed even if the temp file isn't successfully deleted.
	return nil
}

func max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

// PreparedCommit holds the URI of a temp commit.
type PreparedCommit struct {
	URI storage.Path
}

const (
	defaultMaxRetryCommitAttempts                uint32        = 10000000
	defaultRetryWaitDuration                     time.Duration = 500 * time.Millisecond
	defaultMaxReadAttempts                       uint16        = 3
	defaultMaxWriteAttempts                      uint32        = 10000000
	defaultRetryCommitAttemptsBeforeLoadingTable uint32        = 100
	defaultMaxRetryLogFixAttempts                uint16        = 3
)

// TransactionOptions customizes the behavior of a transaction.
type TransactionOptions struct {
	// number of retry attempts allowed when committing a transaction
	MaxRetryCommitAttempts uint32
	// RetryWaitDuration sets the amount of times between retry's on the transaction
	RetryWaitDuration time.Duration
	// Number of retry attempts allowed when reading actions from a log entry
	MaxRetryReadAttempts uint16
	// Number of retry attempts allowed when writing actions to a log entry
	MaxRetryWriteAttempts uint32
	// number of retry commit attempts before loading the latest version from the table rather
	// than using the state store
	RetryCommitAttemptsBeforeLoadingTable uint32
	// Number of retry attempts allowed when fixing the Delta log
	MaxRetryLogFixAttempts uint16
}

// NewTransactionOptions sets the default transaction options.
func NewTransactionOptions() TransactionOptions {
	return TransactionOptions{MaxRetryCommitAttempts: defaultMaxRetryCommitAttempts,
		RetryWaitDuration:                     defaultRetryWaitDuration,
		MaxRetryReadAttempts:                  defaultMaxReadAttempts,
		MaxRetryWriteAttempts:                 defaultMaxWriteAttempts,
		RetryCommitAttemptsBeforeLoadingTable: defaultRetryCommitAttemptsBeforeLoadingTable,
		MaxRetryLogFixAttempts:                defaultMaxRetryLogFixAttempts}
}

// OpenTableWithVersion loads the table at this specific version
// If the table reader or writer version is greater than the client supports, the table will still be opened, but an error will also be returned
func OpenTableWithVersion(store storage.ObjectStore, lock lock.Locker, stateStore state.Store, version int64) (*Table, error) {
	return OpenTableWithVersionAndConfiguration(store, lock, stateStore, version, nil)
}

// OpenTableWithVersionAndConfiguration loads the table at this specific version using the given configuration for optimization settings
func OpenTableWithVersionAndConfiguration(store storage.ObjectStore, lock lock.Locker, stateStore state.Store, version int64, config *OptimizeCheckpointConfiguration) (*Table, error) {
	return openTableWithVersionAndConfiguration(store, lock, stateStore, version, config, true)
}

func openTableWithVersionAndConfiguration(store storage.ObjectStore, lock lock.Locker, stateStore state.Store, version int64, config *OptimizeCheckpointConfiguration, cleanupWorkingStorage bool) (*Table, error) {
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
func OpenTable(store storage.ObjectStore, lock lock.Locker, stateStore state.Store) (*Table, error) {
	return OpenTableWithConfiguration(store, lock, stateStore, nil)
}

// OpenTableWithConfiguration loads the latest version of the table, using the given configuration for optimization settings
func OpenTableWithConfiguration(store storage.ObjectStore, lock lock.Locker, stateStore state.Store, config *OptimizeCheckpointConfiguration) (*Table, error) {
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
