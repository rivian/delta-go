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
	CommitInfos           []CommitInfo
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
)

func (tableState *DeltaTableState[RowType, PartitionType]) WithVersion(version state.DeltaDataTypeVersion) {
	tableState.Version = version
	tableState.Files = make(map[string]Add[RowType, PartitionType])
	tableState.Tombstones = make(map[string]Remove)
}

func (tableState *DeltaTableState[RowType, PartitionType]) UnexpiredTombstones() map[string]Remove {
	retentionTimestamp := time.Now().UnixMilli() - tableState.TombstoneRetention.Milliseconds()
	unexpiredTombstones := make(map[string]Remove, len(tableState.Tombstones))
	for k, v := range tableState.Tombstones {
		if v.DeletionTimestamp > DeltaDataTypeTimestamp(retentionTimestamp) {
			unexpiredTombstones[k] = v
		}
	}
	return unexpiredTombstones
}

func DeltaTableStateFromCommit[RowType any, PartitionType any](table *DeltaTable[RowType, PartitionType], version state.DeltaDataTypeVersion) (*DeltaTableState[RowType, PartitionType], error) {
	actions, err := table.ReadCommitVersion(version)
	if err != nil {
		return nil, err
	}
	return DeltaTableStateFromActions[RowType, PartitionType](actions, version)
}

func DeltaTableStateFromActions[RowType any, PartitionType any](actions []Action, version state.DeltaDataTypeVersion) (*DeltaTableState[RowType, PartitionType], error) {
	tableState := new(DeltaTableState[RowType, PartitionType])
	tableState.WithVersion(version)
	for _, action := range actions {
		err := tableState.ProcessAction(action, true)
		if err != nil {
			return nil, err
		}
	}
	return tableState, nil
}

func (tableState *DeltaTableState[RowType, PartitionType]) ProcessAction(actionInterface Action, requireTombstones bool) error {
	switch action := actionInterface.(type) {
	case *Add[RowType, PartitionType]:
		tableState.Files[action.Path] = *action
	case *Remove:
		if requireTombstones {
			// TODO - do we need to decode as in delta-rs?
			tableState.Tombstones[action.Path] = *action
		}
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
		return fmt.Errorf("unknown %v", action)
	}
	return nil
}

// / Merges new state information into our state
func (tableState *DeltaTableState[RowType, PartitionType]) Merge(newTableState *DeltaTableState[RowType, PartitionType], requireTombstones bool) error {
	// Remove deleted files from existing added files
	for k := range newTableState.Tombstones {
		delete(tableState.Files, k)
	}

	if requireTombstones {
		// Add deleted file tombstones to state so they're available for vacuum
		for k, v := range newTableState.Tombstones {
			tableState.Tombstones[k] = v
		}

		// If files were deleted and then re-added, remove from updated tombstones
		for k := range newTableState.Files {
			delete(tableState.Tombstones, k)
		}
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

func StateFromCheckpoint[RowType any, PartitionType any](table *DeltaTable[RowType, PartitionType], checkpoint *CheckPoint) (*DeltaTableState[RowType, PartitionType], error) {
	newState := new(DeltaTableState[RowType, PartitionType])
	newState.Version = checkpoint.Version
	checkpointDataPaths := table.GetCheckpointDataPaths(checkpoint)
	for _, location := range checkpointDataPaths {
		checkpointBytes, err := table.Store.Get(&location)
		if err != nil {
			return nil, err
		}
		if len(checkpointBytes) == 0 {
			err = ProcessCheckpointBytes(checkpointBytes, newState, table)
			if err != nil {
				return nil, err
			}
		}
	}
	return newState, nil
}

func ProcessCheckpointBytes[RowType any, PartitionType any](checkpointBytes []byte, tableState *DeltaTableState[RowType, PartitionType], table *DeltaTable[RowType, PartitionType]) error {
	reader := bytes.NewReader(checkpointBytes)
	parquetReader := parquet.NewGenericReader[CheckpointEntry[RowType, PartitionType]](reader)
	defer parquetReader.Close()
	for {
		rowBuffer := make([]CheckpointEntry[RowType, PartitionType], 10)
		count, err := parquetReader.Read(rowBuffer)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
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
			if row.Cdc != nil {
				return ErrorCDCNotSupported
			}
			err = tableState.ProcessAction(action, table.Config.RequireTombstones)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (tableState *DeltaTableState[RowType, PartitionType]) GetCheckpointBytes() ([]byte, error) {
	if tableState.CurrentMetadata == nil {
		return nil, ErrorMissingMetadata
	}

	tombstones := tableState.UnexpiredTombstones()

	// If any Remove has ExtendedFileMetadata=false, set all to false
	allRemovesUseExtendedFileMetadata := true
	for _, remove := range tombstones {
		if !remove.ExtendedFileMetadata {
			allRemovesUseExtendedFileMetadata = false
			break
		}
	}
	if !allRemovesUseExtendedFileMetadata {
		for path, remove := range tombstones {
			remove.ExtendedFileMetadata = false
			tombstones[path] = remove
			// TODO - do we need to remove the extra settings if it was true?
		}
	}

	checkpointRows := make([]CheckpointEntry[RowType, PartitionType], 0, 2+len(tableState.AppTransactionVersion)+len(tombstones)+len(tableState.Files))

	protocol := new(Protocol)
	protocol.MinReaderVersion = tableState.MinReaderVersion
	protocol.MinWriterVersion = tableState.MinWriterVersion
	checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType]{Protocol: protocol})

	metadata := tableState.CurrentMetadata.ToMetaData()
	checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType]{MetaData: &metadata})

	for appId, version := range tableState.AppTransactionVersion {
		txn := new(Txn)
		txn.AppId = appId
		txn.Version = DeltaDataTypeVersion(version)
		checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType]{Txn: txn})
	}

	for _, remove := range tombstones {
		checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType]{Remove: &remove})
	}

	// Adds need some additional processing to get the parsed versions of partition values and stats
	for _, add := range tableState.Files {
		checkpointAdd, err := CheckpointAddFromState(&add)
		if err != nil {
			return nil, errors.Join(ErrorConvertingCheckpointAdd, err)
		}
		checkpointRows = append(checkpointRows, CheckpointEntry[RowType, PartitionType]{Add: checkpointAdd})
	}

	// TODO configuration option
	batchSize := 5000
	startRecord := 0
	totalRecords := len(checkpointRows)
	totalWritten := 0

	buf := new(bytes.Buffer)
	// TODO compression configuration option
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
