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
	"reflect"
	"sort"
	"strconv"
	"time"
)

type DeltaTableState[RowType any, PartitionType any] struct {
	// current table version represented by this table state
	Version int64
	// A remove action should remain in the state of the table as a tombstone until it has expired.
	// A tombstone expires when the creation timestamp of the delta file exceeds the expiration
	Tombstones map[string]Remove
	// active files for table state
	Files map[string]AddPartitioned[RowType, PartitionType]
	// Information added to individual commits
	CommitInfos           []CommitInfo
	AppTransactionVersion map[string]int64
	MinReaderVersion      int32
	MinWriterVersion      int32
	// table metadata corresponding to current version
	CurrentMetadata *DeltaTableMetaData
	// retention period for tombstones as time.Duration (nanoseconds)
	TombstoneRetention time.Duration
	// retention period for log entries as time.Duration (nanoseconds)
	LogRetention            time.Duration
	EnableExpiredLogCleanup bool
}

var (
	ErrorMissingMetadata          error = errors.New("missing metadata")
	ErrorConvertingCheckpointAdd  error = errors.New("unable to generate checkpoint add")
	ErrorCDCNotSupported          error = errors.New("cdc is not supported")
	ErrorDeleteVectorNotSupported error = errors.New("delete vectors are not supported")
	ErrorGeneratingCheckpoint     error = errors.New("unable to write checkpoint to buffer")
	ErrorReadingCheckpoint        error = errors.New("unable to read checkpoint")
	ErrorVersionOutOfOrder        error = errors.New("versions out of order during update")
	ErrorUnexpectedSchemaFailure  error = errors.New("unexpected error converting schema")
)

// / Create an empty table state for the given version
func NewDeltaTableState[RowType any, PartitionType any](version int64) *DeltaTableState[RowType, PartitionType] {
	tableState := new(DeltaTableState[RowType, PartitionType])
	tableState.Version = version
	tableState.Files = make(map[string]AddPartitioned[RowType, PartitionType])
	tableState.Tombstones = make(map[string]Remove)
	tableState.AppTransactionVersion = make(map[string]int64)
	// Default 7 days
	tableState.TombstoneRetention = time.Hour * 24 * 7
	// Default 30 days
	tableState.LogRetention = time.Hour * 24 * 30
	tableState.EnableExpiredLogCleanup = false
	return tableState
}

// / Get a configuration value from the table state, or return the default value if the configuration option is not present
func (tableState *DeltaTableState[RowType, PartitionType]) ConfigurationOrDefault(configKey DeltaConfigKey, defaultValue string) string {
	if tableState.CurrentMetadata == nil || tableState.CurrentMetadata.Configuration == nil {
		return defaultValue
	}
	value, ok := tableState.CurrentMetadata.Configuration[string(configKey)]
	if !ok {
		return defaultValue
	}
	return value
}

// / Generate a table state from a specific commit version
func NewDeltaTableStateFromCommit[RowType any, PartitionType any](table *DeltaTable[RowType, PartitionType], version int64) (*DeltaTableState[RowType, PartitionType], error) {
	actions, err := table.ReadCommitVersion(version)
	if err != nil {
		return nil, err
	}
	return NewDeltaTableStateFromActions[RowType, PartitionType](actions, version)
}

// / Generate a table state from a list of actions
func NewDeltaTableStateFromActions[RowType any, PartitionType any](actions []Action, version int64) (*DeltaTableState[RowType, PartitionType], error) {
	tableState := NewDeltaTableState[RowType, PartitionType](version)
	for _, action := range actions {
		err := tableState.processAction(action)
		if err != nil {
			return nil, err
		}
	}
	return tableState, nil
}

// / Update the table state by applying a single action
func (tableState *DeltaTableState[RowType, PartitionType]) processAction(actionInterface Action) error {
	switch action := actionInterface.(type) {
	case *AddPartitioned[RowType, PartitionType]:
		tableState.Files[action.Path] = *action
	case *Add[RowType]:
		// We're using the AddPartitioned type for storing our list of added files, so need to translate the type here
		add := new(AddPartitioned[RowType, PartitionType])
		// Copy details
		add.fromAdd(action)
		tableState.Files[action.Path] = *add
	case *Remove:
		// TODO - do we need to decode as in delta-rs?
		tableState.Tombstones[action.Path] = *action
	case *MetaData:
		if action.Configuration != nil {
			// Parse the configuration options that we make use of
			option, ok := action.Configuration[string(DeletedFileRetentionDurationDeltaConfigKey)]
			if ok {
				duration, err := ParseInterval(option)
				if err != nil {
					return err
				}
				tableState.TombstoneRetention = duration
			}
			option, ok = action.Configuration[string(LogRetentionDurationDeltaConfigKey)]
			if ok {
				duration, err := ParseInterval(option)
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
				tableState.EnableExpiredLogCleanup = boolOption
			}
		}
		deltaTableMetadata, err := action.ToDeltaTableMetaData()
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
		return ErrorCDCNotSupported
	default:
		return errors.Join(ErrorActionUnknown, fmt.Errorf("unknown %v", action))
	}
	return nil
}

// / Merges new state information into our state
func (tableState *DeltaTableState[RowType, PartitionType]) merge(newTableState *DeltaTableState[RowType, PartitionType]) error {
	// Remove deleted files from existing added files
	for k := range newTableState.Tombstones {
		delete(tableState.Files, k)
	}

	// Add deleted file tombstones to state so they're available for vacuum
	for k, v := range newTableState.Tombstones {
		tableState.Tombstones[k] = v
	}

	// If files were deleted and then re-added, remove from updated tombstones
	for k := range newTableState.Files {
		delete(tableState.Tombstones, k)
	}

	for k, v := range newTableState.Files {
		tableState.Files[k] = v
	}

	if newTableState.MinReaderVersion > 0 {
		tableState.MinReaderVersion = newTableState.MinReaderVersion
		tableState.MinWriterVersion = newTableState.MinWriterVersion
	}

	if newTableState.CurrentMetadata != nil {
		tableState.TombstoneRetention = newTableState.TombstoneRetention
		tableState.LogRetention = newTableState.LogRetention
		tableState.EnableExpiredLogCleanup = newTableState.EnableExpiredLogCleanup
		tableState.CurrentMetadata = newTableState.CurrentMetadata
	}

	for k, v := range newTableState.AppTransactionVersion {
		tableState.AppTransactionVersion[k] = v
	}

	tableState.CommitInfos = append(tableState.CommitInfos, newTableState.CommitInfos...)

	if newTableState.Version <= tableState.Version {
		return ErrorVersionOutOfOrder
	}
	tableState.Version = newTableState.Version

	return nil
}

func stateFromCheckpoint[RowType any, PartitionType any](table *DeltaTable[RowType, PartitionType], checkpoint *CheckPoint) (*DeltaTableState[RowType, PartitionType], error) {
	newState := NewDeltaTableState[RowType, PartitionType](checkpoint.Version)
	checkpointDataPaths := table.GetCheckpointDataPaths(checkpoint)
	for _, location := range checkpointDataPaths {
		checkpointBytes, err := table.Store.Get(&location)
		if err != nil {
			return nil, err
		}
		if len(checkpointBytes) > 0 {
			err = processCheckpointBytes(checkpointBytes, newState, table)
			if err != nil {
				return nil, err
			}
		}
	}
	return newState, nil
}

func isPartitionTypeEmpty[PartitionType any]() bool {
	testPartitionItem := new(PartitionType)
	structType := reflect.TypeOf(*testPartitionItem)
	return structType.NumField() == 0
}

func processCheckpointBytes[RowType any, PartitionType any](checkpointBytes []byte, tableState *DeltaTableState[RowType, PartitionType], table *DeltaTable[RowType, PartitionType]) (returnErr error) {
	// Determine whether partitioned
	isPartitioned := !isPartitionTypeEmpty[PartitionType]()
	if isPartitioned {
		return processCheckpointBytesWithAddSpecified[RowType, PartitionType, AddPartitioned[RowType, PartitionType]](checkpointBytes, tableState, table)
	} else {
		return processCheckpointBytesWithAddSpecified[RowType, PartitionType, Add[RowType]](checkpointBytes, tableState, table)
	}
}

// / Update a table state with the contents of a checkpoint file
func processCheckpointBytesWithAddSpecified[RowType any, PartitionType any, AddType AddPartitioned[RowType, PartitionType] | Add[RowType]](checkpointBytes []byte, tableState *DeltaTableState[RowType, PartitionType], table *DeltaTable[RowType, PartitionType]) error {
	var processFunc = func(checkpointEntry *CheckpointEntry[RowType, PartitionType, AddType]) error {
		var action Action
		if checkpointEntry.Add != nil {
			action = checkpointEntry.Add
		}
		if checkpointEntry.MetaData != nil {
			action = checkpointEntry.MetaData
		}
		if checkpointEntry.Protocol != nil {
			action = checkpointEntry.Protocol
		}
		if checkpointEntry.Remove != nil {
			action = checkpointEntry.Remove
		}
		if checkpointEntry.Txn != nil {
			action = checkpointEntry.Txn
		}

		if action != nil {
			err := tableState.processAction(action)
			if err != nil {
				return err
			}
		} else {
			return errors.New("no action found in checkpoint record")
		}
		return nil
	}
	return readAndProcessStructsFromParquet(checkpointBytes, processFunc)
}

// / Prepare the table state for checkpointing by updating tombstones
func (tableState *DeltaTableState[RowType, PartitionType]) prepareStateForCheckpoint() error {
	if tableState.CurrentMetadata == nil {
		return ErrorMissingMetadata
	}

	// Don't keep expired tombstones
	// Also check if any of the non-expired Remove actions had ExtendedFileMetadata = false
	doNotUseExtendedFileMetadata := false
	retentionTimestamp := time.Now().UnixMilli() - tableState.TombstoneRetention.Milliseconds()
	unexpiredTombstones := make(map[string]Remove, len(tableState.Tombstones))
	for path, remove := range tableState.Tombstones {
		if remove.DeletionTimestamp == nil || *remove.DeletionTimestamp > retentionTimestamp {
			unexpiredTombstones[path] = remove
			doNotUseExtendedFileMetadata = doNotUseExtendedFileMetadata && (remove.ExtendedFileMetadata == nil || !*remove.ExtendedFileMetadata)
		}
	}

	tableState.Tombstones = unexpiredTombstones

	// If any Remove has ExtendedFileMetadata = false, set all to false
	removeExtendedFileMetadata := false
	if doNotUseExtendedFileMetadata {
		for path, remove := range tableState.Tombstones {
			remove.ExtendedFileMetadata = &removeExtendedFileMetadata
			tableState.Tombstones[path] = remove
			// TODO - do we need to remove the extra settings if it was true?
		}
	}
	return nil
}

// / Retrieve the next batch of checkpoint entries to write to Parquet
func checkpointRows[RowType any, PartitionType any, AddType AddPartitioned[RowType, PartitionType] | Add[RowType]](tableState *DeltaTableState[RowType, PartitionType], startOffset int, maxRows int) ([]CheckpointEntry[RowType, PartitionType, AddType], error) {
	maxRowCount := 2 + len(tableState.AppTransactionVersion) + len(tableState.Tombstones) + len(tableState.Files)
	if maxRows < maxRowCount {
		maxRowCount = maxRows
	}
	checkpointRows := make([]CheckpointEntry[RowType, PartitionType, AddType], 0, maxRowCount)

	currentOffset := 0

	// Row 1: protocol
	if startOffset <= currentOffset {
		protocol := new(Protocol)
		protocol.MinReaderVersion = tableState.MinReaderVersion
		protocol.MinWriterVersion = tableState.MinWriterVersion
		checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType, AddType]{Protocol: protocol})
	}

	currentOffset++

	// Row 2: metadata
	if startOffset <= currentOffset && len(checkpointRows) < maxRows {
		metadata := tableState.CurrentMetadata.ToMetaData()
		checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType, AddType]{MetaData: &metadata})
	}

	currentOffset++

	// Next, optional Txn entries per app id
	if startOffset < currentOffset+len(tableState.AppTransactionVersion) && len(tableState.AppTransactionVersion) > 0 && len(checkpointRows) < maxRows {
		keys := make([]string, 0, len(tableState.AppTransactionVersion))
		for k := range tableState.AppTransactionVersion {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, appId := range keys {
			if startOffset < currentOffset+i {
				txn := new(Txn)
				txn.AppId = appId
				version := tableState.AppTransactionVersion[appId]
				txn.Version = version
				checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType, AddType]{Txn: txn})

				if len(checkpointRows) >= maxRows {
					break
				}
			}
		}
	}

	currentOffset += len(tableState.AppTransactionVersion)

	// Tombstone / Remove entries
	if startOffset < currentOffset+len(tableState.Tombstones) && len(tableState.Tombstones) > 0 && len(checkpointRows) < maxRows {
		keys := make([]string, 0, len(tableState.Tombstones))
		for k := range tableState.Tombstones {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, path := range keys {
			if startOffset <= currentOffset+i {
				checkpointRemove := new(Remove)
				*checkpointRemove = tableState.Tombstones[path]
				checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType, AddType]{Remove: checkpointRemove})

				if len(checkpointRows) >= maxRows {
					break
				}
			}
		}
	}

	currentOffset += len(tableState.Tombstones)

	// Add entries
	if startOffset < currentOffset+len(tableState.Files) && len(tableState.Files) > 0 && len(checkpointRows) < maxRows {
		keys := make([]string, 0, len(tableState.Files))
		for k := range tableState.Files {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, path := range keys {
			if startOffset <= currentOffset+i {
				add := tableState.Files[path]
				checkpointAdd, err := checkpointAdd[RowType, PartitionType, AddType](&add)
				if err != nil {
					return nil, errors.Join(ErrorConvertingCheckpointAdd, err)
				}
				checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType, AddType]{Add: checkpointAdd})

				if len(checkpointRows) >= maxRows {
					break
				}
			}
		}
	}

	return checkpointRows, nil
}
