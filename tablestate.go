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
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/chelseajonesr/rfarrow"
	"github.com/rivian/delta-go/storage"
)

// TableState maintains the current known state of a table
// This is used in reading and generating checkpoints
// If on-disk optimization is enabled, some of the information here is empty as the
// state is offloaded to disk to reduce memory use
type TableState struct {
	// current table version represented by this table state
	Version int64
	// A remove action should remain in the state of the table as a tombstone until it has expired.
	// A tombstone expires when the creation timestamp of the Delta file exceeds the expiration
	// This is empty if on-disk optimization is enabled
	Tombstones map[string]Remove
	// Active files for table state
	// This is empty if on-disk optimization is enabled
	Files map[string]Add
	// Information added to individual commits
	CommitInfos           []CommitInfo
	AppTransactionVersion map[string]int64
	MinReaderVersion      int32
	MinWriterVersion      int32
	// Table metadata corresponding to current version
	CurrentMetadata *TableMetaData
	// Retention period for tombstones as time.Duration (nanoseconds)
	TombstoneRetention time.Duration
	// Retention period for log entries as time.Duration (nanoseconds)
	LogRetention time.Duration
	// Expired log cleanup has not been thoroughly tested, so marking as experimental
	ExperimentalEnableExpiredLogCleanup bool
	// Additional state for on-disk optimizations for large checkpoints
	onDiskOptimization bool
	OnDiskTableState
}

var (
	// ErrMissingMetadata is returned if trying to create a checkpoint with no metadata
	ErrMissingMetadata error = errors.New("missing metadata")
	// ErrConvertingCheckpointAdd is returned if there is an error converting an Add action to checkpoint format
	ErrConvertingCheckpointAdd error = errors.New("unable to generate checkpoint add")
	// ErrCDCNotSupported is returned if a CDC action is seen when generating a checkpoint
	ErrCDCNotSupported error = errors.New("cdc is not supported")
	// ErrReadingCheckpoint is returned if there is an error reading a checkpoint
	ErrReadingCheckpoint error = errors.New("unable to read checkpoint")
	// ErrVersionOutOfOrder is returned if the versions are out of order when loading the table state
	// This would indicate an internal logic error
	ErrVersionOutOfOrder error = errors.New("versions out of order during update")
)

// NewTableState creates an empty table state for the given version
func NewTableState(version int64) *TableState {
	tableState := new(TableState)
	tableState.Version = version
	tableState.Files = make(map[string]Add)
	tableState.Tombstones = make(map[string]Remove)
	tableState.AppTransactionVersion = make(map[string]int64)
	// Default 7 days
	tableState.TombstoneRetention = time.Hour * 24 * 7
	// Default 30 days
	tableState.LogRetention = time.Hour * 24 * 30
	tableState.ExperimentalEnableExpiredLogCleanup = false
	tableState.concurrentUpdateMutex = new(sync.Mutex)
	return tableState
}

func setupOnDiskOptimization(config *OptimizeCheckpointConfiguration, tableState *TableState, initialFileCount int) error {
	if config != nil && config.OnDiskOptimization {
		if config.WorkingStore == nil {
			return errors.Join(ErrCheckpointOptimizationWorkingFolder, errors.New("the optimization working store is not set"))
		}
		existingFiles, err := config.WorkingStore.List(config.WorkingFolder, nil)
		if err != nil {
			return errors.Join(ErrCheckpointOptimizationWorkingFolder, err)
		}
		if len(existingFiles.Objects) > 0 {
			// List may return a single result consisting of the folder itself
			if len(existingFiles.Objects) > 1 || !strings.HasSuffix(existingFiles.Objects[0].Location.Raw, "/") {
				return errors.Join(ErrCheckpointOptimizationWorkingFolder, errors.New("the optimization working folder is not empty"))
			}
		}
		tableState.onDiskOptimization = true
		tableState.onDiskTempFiles = make([]storage.Path, 0, initialFileCount)
	}
	return nil
}

// FileCount returns the total number of Parquet files making up the table at the loaded version
func (tableState *TableState) FileCount() int {
	if tableState.onDiskOptimization {
		return tableState.onDiskFileCount
	}
	return len(tableState.Files)
}

// TombstoneCount returns the total number of tombstones (logically but not physically deleted files) in the table at the loaded version
func (tableState *TableState) TombstoneCount() int {
	if tableState.onDiskOptimization {
		return tableState.onDiskTombstoneCount
	}
	return len(tableState.Tombstones)
}

// NewTableStateFromCommit reads a specific commit version and returns the contained TableState
func NewTableStateFromCommit(table *Table, version int64) (*TableState, error) {
	actions, err := table.ReadCommitVersion(version)
	if err != nil {
		return nil, err
	}
	return NewTableStateFromActions(actions, version)
}

// NewTableStateFromActions generates table state from a list of actions
func NewTableStateFromActions(actions []Action, version int64) (*TableState, error) {
	tableState := NewTableState(version)
	for _, action := range actions {
		err := tableState.processAction(action)
		if err != nil {
			return nil, err
		}
	}
	return tableState, nil
}

// Update the table state by applying a single action
func (tableState *TableState) processAction(actionInterface Action) error {
	switch action := actionInterface.(type) {
	case *Add:
		tableState.Files[action.Path] = *action
	case *Remove:
		// TODO - do we need to decode as in delta-rs?
		tableState.Tombstones[action.Path] = *action
	case *MetaData:
		if action.Configuration != nil {
			// Parse the configuration options that we make use of
			option, ok := action.Configuration[string(DeletedFileRetentionDurationDeltaConfigKey)]
			if ok {
				duration, err := parseInterval(option)
				if err != nil {
					return err
				}
				tableState.TombstoneRetention = duration
			}
			option, ok = action.Configuration[string(LogRetentionDurationDeltaConfigKey)]
			if ok {
				duration, err := parseInterval(option)
				if err != nil {
					return err
				}
				tableState.LogRetention = duration
			}
			option, ok = action.Configuration[string(EnableExpiredLogCleanupDeltaConfigKey)]
			if ok {
				boolOption, err := strconv.ParseBool(option)
				if err != nil {
					return err
				}
				tableState.ExperimentalEnableExpiredLogCleanup = boolOption
			}
		}
		deltaTableMetadata, err := action.toTableMetadata()
		if err != nil {
			return err
		}
		tableState.CurrentMetadata = &deltaTableMetadata
	case *Txn:
		tableState.AppTransactionVersion[action.AppId] = action.Version
	case *Protocol:
		tableState.MinReaderVersion = action.MinReaderVersion
		tableState.MinWriterVersion = action.MinWriterVersion
	case *CommitInfo:
		tableState.CommitInfos = append(tableState.CommitInfos, *action)
	case *Cdc:
		return ErrCDCNotSupported
	default:
		return errors.Join(ErrActionUnknown, fmt.Errorf("unknown %v", action))
	}
	return nil
}

// Merges new state information into our state
func (tableState *TableState) merge(newTableState *TableState, maxRowsPerPart int, config *OptimizeCheckpointConfiguration, finalMerge bool) error {
	var err error

	if tableState.onDiskOptimization {
		err = tableState.mergeOnDiskState(newTableState, maxRowsPerPart, config, finalMerge)
		if err != nil {
			return err
		}
		// the final merge is to resolve pending adds/tombstones and does not include a new table state
		if finalMerge {
			return nil
		}
	}

	// In memory file updates
	for k, v := range newTableState.Tombstones {
		// Remove deleted files from existing added files
		delete(tableState.Files, k)
		// Add deleted file tombstones to state so they're available for vacuum
		tableState.Tombstones[k] = v
	}
	for k, v := range newTableState.Files {
		// If files were deleted and then re-added, remove from updated tombstones
		delete(tableState.Tombstones, k)
		tableState.Files[k] = v
	}

	if newTableState.MinReaderVersion > 0 {
		tableState.MinReaderVersion = newTableState.MinReaderVersion
		tableState.MinWriterVersion = newTableState.MinWriterVersion
	}

	if newTableState.CurrentMetadata != nil {
		tableState.TombstoneRetention = newTableState.TombstoneRetention
		tableState.LogRetention = newTableState.LogRetention
		tableState.ExperimentalEnableExpiredLogCleanup = newTableState.ExperimentalEnableExpiredLogCleanup
		tableState.CurrentMetadata = newTableState.CurrentMetadata
	}

	for k, v := range newTableState.AppTransactionVersion {
		tableState.AppTransactionVersion[k] = v
	}

	tableState.CommitInfos = append(tableState.CommitInfos, newTableState.CommitInfos...)

	if newTableState.Version <= tableState.Version {
		return ErrVersionOutOfOrder
	}
	tableState.Version = newTableState.Version

	return nil
}

func stateFromCheckpoint(table *Table, checkpoint *CheckPoint, config *OptimizeCheckpointConfiguration) (*TableState, error) {
	newState := NewTableState(checkpoint.Version)
	checkpointDataPaths := table.GetCheckpointDataPaths(checkpoint)
	err := setupOnDiskOptimization(config, newState, len(checkpointDataPaths))
	if err != nil {
		return nil, err
	}

	// Optional concurrency support
	if newState.onDiskOptimization && config.ConcurrentCheckpointRead > 1 {
		err := newState.applyCheckpointConcurrently(table.Store, checkpointDataPaths, config)
		if err != nil {
			return nil, err
		}
	} else {
		// No concurrency
		for i, location := range checkpointDataPaths {
			task := checkpointProcessingTask{location: location, state: newState, part: i, config: config, store: table.Store}
			err := stateFromCheckpointPart(task)
			if err != nil {
				return nil, err
			}
		}
	}

	return newState, nil
}

type checkpointProcessingTask struct {
	location storage.Path
	state    *TableState
	part     int
	config   *OptimizeCheckpointConfiguration
	store    storage.ObjectStore
}

func stateFromCheckpointPart(task checkpointProcessingTask) error {
	checkpointBytes, err := task.store.Get(task.location)
	if err != nil {
		return err
	}
	if len(checkpointBytes) > 0 {
		err = task.state.processCheckpointBytes(checkpointBytes, task.part, task.config)
		if err != nil {
			return err
		}
	} else {
		return errors.Join(ErrCheckpointIncomplete, fmt.Errorf("zero size checkpoint at %s", task.location.Raw))
	}
	return nil
}

func actionFromCheckpointEntry(checkpointEntry *CheckpointEntry) (Action, error) {
	var action Action
	if checkpointEntry.Add != nil {
		if action != nil {
			return action, ErrCheckpointEntryMultipleActions
		}
		action = checkpointEntry.Add
	}
	if checkpointEntry.Remove != nil {
		if action != nil {
			return action, ErrCheckpointEntryMultipleActions
		}
		action = checkpointEntry.Remove
	}
	if checkpointEntry.MetaData != nil {
		if action != nil {
			return action, ErrCheckpointEntryMultipleActions
		}
		action = checkpointEntry.MetaData
	}
	if checkpointEntry.Protocol != nil {
		if action != nil {
			return action, ErrCheckpointEntryMultipleActions
		}
		action = checkpointEntry.Protocol
	}
	if checkpointEntry.Txn != nil {
		if action != nil {
			return action, ErrCheckpointEntryMultipleActions
		}
		action = checkpointEntry.Txn
	}
	return action, nil
}

func (tableState *TableState) processCheckpointBytes(checkpointBytes []byte, part int, config *OptimizeCheckpointConfiguration) (returnErr error) {
	concurrentCheckpointRead := tableState.onDiskOptimization && config.ConcurrentCheckpointRead > 1
	var processEntryAction = func(checkpointEntry *CheckpointEntry) error {
		action, err := actionFromCheckpointEntry(checkpointEntry)
		if err != nil {
			return err
		}

		if action != nil {
			if concurrentCheckpointRead {
				tableState.concurrentUpdateMutex.Lock()
				defer tableState.concurrentUpdateMutex.Unlock()
			}
			err := tableState.processAction(action)
			if err != nil {
				return err
			}
		} else {
			if !tableState.onDiskOptimization {
				// This is expected during optimized on-disk reading but not otherwise
				return errors.New("no action found in checkpoint record")
			}
		}
		return nil
	}

	bytesReader := bytes.NewReader(checkpointBytes)
	parquetReader, err := file.NewParquetReader(bytesReader)
	if err != nil {
		return err
	}
	defer parquetReader.Close()

	parquetSchema := parquetReader.MetaData().Schema
	fileReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 10, Parallel: true}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	arrowSchema, err := fileReader.Schema()
	if err != nil {
		return err
	}
	arrowFieldList := arrowSchema.Fields()

	// For on-disk optimization, don't load add/remove into memory
	inMemoryCols := make([]int, 0, 150)
	for i := 0; i < parquetSchema.NumColumns(); i++ {
		columnPath := parquetSchema.Column(i).ColumnPath().String()
		if !tableState.onDiskOptimization || (!strings.HasPrefix(columnPath, "add") && !strings.HasPrefix(columnPath, "remove")) {
			inMemoryCols = append(inMemoryCols, i)
		}
	}

	// Get mappings between struct member names and parquet/arrow names so we don't have to look them up repeatedly
	// during record assignments
	var fieldExclusions []string
	if tableState.onDiskOptimization {
		fieldExclusions = []string{"Add", "Remove"}
	}
	inMemoryIndexMappings, err := rfarrow.MapGoStructFieldNamesToArrowIndices[CheckpointEntry](arrowFieldList, fieldExclusions, true)
	if err != nil {
		return err
	}

	// Read all row groups and process in-memory actions
	var tbl arrow.Table
	if tableState.onDiskOptimization {
		rgs := []int{}
		for i := 0; i < parquetReader.NumRowGroups(); i++ {
			rgs = append(rgs, i)
		}
		tbl, err = fileReader.ReadRowGroups(context.Background(), inMemoryCols, rgs)
	} else {
		tbl, err = fileReader.ReadTable(context.Background())
	}
	if err != nil {
		return err
	}
	defer tbl.Release()

	tableReader := array.NewTableReader(tbl, 0)
	defer tableReader.Release()

	for tableReader.Next() {
		// the record contains a batch of rows
		record := tableReader.Record()

		entries := make([]*CheckpointEntry, record.NumRows())
		entryValues := make([]reflect.Value, record.NumRows())
		for j := int64(0); j < record.NumRows(); j++ {
			t := new(CheckpointEntry)
			entries[j] = t
			entryValues[j] = reflect.ValueOf(t)
		}

		err = rfarrow.SetGoStructsFromArrowArrays(entryValues, record.Columns(), inMemoryIndexMappings, 0)
		if err != nil {
			return err
		}
		for j := int64(0); j < record.NumRows(); j++ {
			err = processEntryAction(entries[j])
			if err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
	}

	// Save the part file for on disk optimization
	if tableState.onDiskOptimization {
		// The non-add, non-remove columns will be almost entirely nulls, so picking out just add and remove
		// slows us down here for a very minimal improvement in file size.
		// Instead we just write out the entire file.
		onDiskFile := storage.PathFromIter([]string{config.WorkingFolder.Raw, fmt.Sprintf("intermediate.%d.parquet", part)})
		config.WorkingStore.Put(onDiskFile, checkpointBytes)
		func() {
			tableState.concurrentUpdateMutex.Lock()
			defer tableState.concurrentUpdateMutex.Unlock()
			tableState.onDiskTempFiles = append(tableState.onDiskTempFiles, onDiskFile)
		}()

		// Store the number of add and remove records locally
		// These counts are required later for generating new checkpoints
		err = countAddsAndTombstones(tableState, checkpointBytes, arrowSchema, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

// Prepare the table state for checkpointing by updating tombstones
func (tableState *TableState) prepareStateForCheckpoint(config *OptimizeCheckpointConfiguration) error {
	if tableState.CurrentMetadata == nil {
		return ErrMissingMetadata
	}

	retentionTimestamp := time.Now().UnixMilli() - tableState.TombstoneRetention.Milliseconds()

	if tableState.onDiskOptimization {
		return tableState.prepareOnDiskStateForCheckpoint(retentionTimestamp, config)
	}

	// Don't keep expired tombstones
	// Also check if any of the non-expired Remove actions had ExtendedFileMetadata = false
	doNotUseExtendedFileMetadata := false
	unexpiredTombstones := make(map[string]Remove, len(tableState.Tombstones))
	for path, remove := range tableState.Tombstones {
		if remove.DeletionTimestamp == nil || *remove.DeletionTimestamp > retentionTimestamp {
			unexpiredTombstones[path] = remove
			doNotUseExtendedFileMetadata = doNotUseExtendedFileMetadata && (!remove.ExtendedFileMetadata)
		}
	}

	tableState.Tombstones = unexpiredTombstones

	// If any Remove has ExtendedFileMetadata = false, set all to false
	if doNotUseExtendedFileMetadata {
		for path, remove := range tableState.Tombstones {
			remove.ExtendedFileMetadata = false
			tableState.Tombstones[path] = remove
			// TODO - remove the extended fields (remove.size, remove.partitionValues) from the schema
		}
	}
	return nil
}

// Retrieve the next batch of checkpoint entries to write to Parquet
func checkpointRows(
	tableState *TableState, startOffset int, config *CheckpointConfiguration) ([]CheckpointEntry, error) {
	var maxRowCount int

	maxRowCount = 2 + len(tableState.AppTransactionVersion) + tableState.FileCount() + tableState.TombstoneCount()
	if config.MaxRowsPerPart < maxRowCount {
		maxRowCount = config.MaxRowsPerPart
	}
	checkpointRows := make([]CheckpointEntry, 0, maxRowCount)

	currentOffset := 0

	// Row 1: protocol
	if startOffset <= currentOffset {
		protocol := new(Protocol)
		protocol.MinReaderVersion = tableState.MinReaderVersion
		protocol.MinWriterVersion = tableState.MinWriterVersion
		checkpointRows = append(checkpointRows, CheckpointEntry{Protocol: protocol})
	}

	currentOffset++

	// Row 2: metadata
	if startOffset <= currentOffset && len(checkpointRows) < config.MaxRowsPerPart {
		metadata := tableState.CurrentMetadata.toMetaData()
		checkpointRows = append(checkpointRows, CheckpointEntry{MetaData: &metadata})
	}

	currentOffset++

	// Next, optional Txn entries per app id
	if startOffset < currentOffset+len(tableState.AppTransactionVersion) && len(tableState.AppTransactionVersion) > 0 && len(checkpointRows) < config.MaxRowsPerPart {
		keys := make([]string, 0, len(tableState.AppTransactionVersion))
		for k := range tableState.AppTransactionVersion {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, appID := range keys {
			if startOffset < currentOffset+i {
				txn := new(Txn)
				txn.AppId = appID
				version := tableState.AppTransactionVersion[appID]
				txn.Version = version
				checkpointRows = append(checkpointRows, CheckpointEntry{Txn: txn})

				if len(checkpointRows) >= config.MaxRowsPerPart {
					break
				}
			}
		}
	}

	currentOffset += len(tableState.AppTransactionVersion)

	// Tombstone / Remove entries
	tombstoneCount := tableState.TombstoneCount()
	if startOffset < currentOffset+tombstoneCount && tombstoneCount > 0 && len(checkpointRows) < config.MaxRowsPerPart {
		if tableState.onDiskOptimization {
			initialOffset := startOffset - currentOffset
			if initialOffset < 0 {
				initialOffset = 0
			}
			onDiskTombstoneCheckpointRows(tableState, initialOffset, &checkpointRows, config)
		} else {
			keys := make([]string, 0, tombstoneCount)
			for k := range tableState.Tombstones {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for i, path := range keys {
				if startOffset <= currentOffset+i {
					checkpointRemove := new(Remove)
					*checkpointRemove = tableState.Tombstones[path]
					checkpointRows = append(checkpointRows, CheckpointEntry{Remove: checkpointRemove})

					if len(checkpointRows) >= config.MaxRowsPerPart {
						break
					}
				}
			}
		}
	}
	currentOffset += tombstoneCount

	// Add entries
	fileCount := tableState.FileCount()
	if startOffset < currentOffset+fileCount && fileCount > 0 && len(checkpointRows) < config.MaxRowsPerPart {
		if tableState.onDiskOptimization {
			initialOffset := startOffset - currentOffset
			if initialOffset < 0 {
				initialOffset = 0
			}
			onDiskAddCheckpointRows(tableState, initialOffset, &checkpointRows, config)
		} else {
			keys := make([]string, 0, fileCount)
			for k := range tableState.Files {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for i, path := range keys {
				if startOffset <= currentOffset+i {
					add := tableState.Files[path]
					checkpointAdd, err := checkpointAdd(&add)
					if err != nil {
						return nil, errors.Join(ErrConvertingCheckpointAdd, err)
					}
					checkpointRows = append(checkpointRows, CheckpointEntry{Add: checkpointAdd})

					if len(checkpointRows) >= config.MaxRowsPerPart {
						break
					}
				}
			}
		}
	}

	return checkpointRows, nil
}
