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
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/rivian/delta-go/state"
	"github.com/segmentio/parquet-go"
)

type DeltaTableState[RowType any, PartitionType any] struct {
	// current table version represented by this table state
	Version state.DeltaDataTypeVersion
	// A remove action should remain in the state of the table as a tombstone until it has expired.
	// A tombstone expires when the creation timestamp of the delta file exceeds the expiration
	Tombstones map[string]Remove
	// active files for table state
	Files map[string]Add[RowType, PartitionType]
	// Information added to individual commits
	CommitInfos []CommitInfo
	// TODO remove state.
	AppTransactionVersion map[string]state.DeltaDataTypeVersion
	MinReaderVersion      DeltaDataTypeInt
	MinWriterVersion      DeltaDataTypeInt
	// table metadata corresponding to current version
	CurrentMetadata *DeltaTableMetaData
	// retention period for tombstones in milli-seconds
	TombstoneRetention time.Duration
	// retention period for log entries in milli-seconds
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
)

// / Create an empty table state for the given version
func NewDeltaTableState[RowType any, PartitionType any](version state.DeltaDataTypeVersion) *DeltaTableState[RowType, PartitionType] {
	tableState := new(DeltaTableState[RowType, PartitionType])
	tableState.Version = version
	tableState.Files = make(map[string]Add[RowType, PartitionType])
	tableState.Tombstones = make(map[string]Remove)
	tableState.AppTransactionVersion = make(map[string]state.DeltaDataTypeVersion)
	// Default 7 days
	tableState.TombstoneRetention = time.Hour * 7 * 24
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
func NewDeltaTableStateFromCommit[RowType any, PartitionType any](table *DeltaTable[RowType, PartitionType], version state.DeltaDataTypeVersion) (*DeltaTableState[RowType, PartitionType], error) {
	actions, err := table.ReadCommitVersion(version)
	if err != nil {
		return nil, err
	}
	return NewDeltaTableStateFromActions[RowType, PartitionType](actions, version)
}

// / Generate a table state from a list of actions
func NewDeltaTableStateFromActions[RowType any, PartitionType any](actions []Action, version state.DeltaDataTypeVersion) (*DeltaTableState[RowType, PartitionType], error) {
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
	case *Add[RowType, PartitionType]:
		tableState.Files[action.Path] = *action
	case *Remove:
		// TODO - do we need to decode as in delta-rs?
		tableState.Tombstones[action.Path] = *action
	case *MetaData:
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
		deltaTableMetadata, err := action.ToDeltaTableMetaData()
		if err != nil {
			return err
		}
		tableState.CurrentMetadata = &deltaTableMetadata
	case *Txn:
		tableState.AppTransactionVersion[action.AppId] = state.DeltaDataTypeVersion(action.Version)
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

	if tableState.Version < newTableState.Version {
		tableState.Version = newTableState.Version
	}

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

// / Update a table state with the contents of a checkpoint file
func processCheckpointBytes[RowType any, PartitionType any](checkpointBytes []byte, tableState *DeltaTableState[RowType, PartitionType], table *DeltaTable[RowType, PartitionType]) (returnErr error) {
	reader := bytes.NewReader(checkpointBytes)
	// The parquet library will panic if the file is malformed or if it can't handle the RowType/PartitionType
	defer func() {
		if r := recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("%v", r)
			}
			returnErr = errors.Join(ErrorReadingCheckpoint, err)
		}
	}()
	parquetReader := parquet.NewGenericReader[CheckpointEntry[RowType, PartitionType]](reader)
	defer parquetReader.Close()
	for {
		rowBuffer := make([]CheckpointEntry[RowType, PartitionType], 10)
		count, err := parquetReader.Read(rowBuffer)
		doneReading := errors.Is(err, io.EOF)
		if err != nil && !doneReading {
			return err
		}
		var action Action
		for i := 0; i < count; i++ {
			row := rowBuffer[i]
			if row.Add != nil {
				action = row.Add
			}
			if row.Remove != nil {
				action = row.Remove
			}
			if row.MetaData != nil {
				action = row.MetaData
			}
			if row.Txn != nil {
				action = row.Txn
			}
			if row.Protocol != nil {
				action = row.Protocol
			}
			if row.Cdc != nil {
				return ErrorCDCNotSupported
			}
			err = tableState.processAction(action)
			if err != nil {
				return err
			}
		}
		if doneReading {
			break
		}
	}
	return nil
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
		if remove.DeletionTimestamp > DeltaDataTypeTimestamp(retentionTimestamp) {
			unexpiredTombstones[path] = remove
			doNotUseExtendedFileMetadata = doNotUseExtendedFileMetadata && !remove.ExtendedFileMetadata
		}
	}

	tableState.Tombstones = unexpiredTombstones

	// If any Remove has ExtendedFileMetadata = false, set all to false
	if doNotUseExtendedFileMetadata {
		for path, remove := range tableState.Tombstones {
			remove.ExtendedFileMetadata = false
			tableState.Tombstones[path] = remove
			// TODO - do we need to remove the extra settings if it was true?
		}
	}
	return nil
}

// / Retrieve the next batch of checkpoint entries to write to Parquet
func (tableState *DeltaTableState[RowType, PartitionType]) checkpointRows(startOffset int, maxRows int) ([]CheckpointEntry[RowType, PartitionType], error) {
	maxRowCount := 2 + len(tableState.AppTransactionVersion) + len(tableState.Tombstones) + len(tableState.Files)
	if maxRows < maxRowCount {
		maxRowCount = maxRows
	}
	checkpointRows := make([]CheckpointEntry[RowType, PartitionType], 0, maxRowCount)

	currentOffset := 0

	// Row 1: protocol
	if startOffset <= currentOffset {
		protocol := new(Protocol)
		protocol.MinReaderVersion = tableState.MinReaderVersion
		protocol.MinWriterVersion = tableState.MinWriterVersion
		checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType]{Protocol: protocol})
	}

	currentOffset++

	// Row 2: metadata
	if startOffset <= currentOffset && len(checkpointRows) < maxRows {
		metadata := tableState.CurrentMetadata.ToMetaData()
		checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType]{MetaData: &metadata})
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
				txn.Version = DeltaDataTypeVersion(tableState.AppTransactionVersion[appId])
				checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType]{Txn: txn})

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
				checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType]{Remove: checkpointRemove})

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
				checkpointAdd, err := checkpointAdd(&add)
				if err != nil {
					return nil, errors.Join(ErrorConvertingCheckpointAdd, err)
				}
				checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType]{Add: checkpointAdd})

				if len(checkpointRows) >= maxRows {
					break
				}
			}
		}
	}

	return checkpointRows, nil
}

// / Convert a slice of checkpoint entries into a checkpoint parquet file and return the bytes
func checkpointParquetBytes[RowType any, PartitionType any](checkpointRows []CheckpointEntry[RowType, PartitionType]) ([]byte, error) {
	// TODO configuration option for writer batch size?
	batchSize := 5000
	startRecord := 0
	totalRecords := len(checkpointRows)
	totalWritten := 0

	buf := new(bytes.Buffer)
	// TODO configuration option for compression type?
	writer := parquet.NewGenericWriter[CheckpointEntry[RowType, PartitionType]](buf, parquet.Compression(&parquet.Snappy))

	for totalWritten < totalRecords {
		endRecord := startRecord + batchSize
		if endRecord >= totalRecords {
			endRecord = totalRecords
		}
		written, err := writer.Write(checkpointRows[startRecord:endRecord])
		if err != nil {
			return nil, err
		}
		if written == 0 {
			return nil, ErrorGeneratingCheckpoint
		}
		totalWritten += written
		startRecord += written
	}

	writer.Flush()
	writer.Close()

	return buf.Bytes(), nil
}
