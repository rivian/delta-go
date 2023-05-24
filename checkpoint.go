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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/rivian/delta-go/state"
	"github.com/rivian/delta-go/storage"
)

// / Metadata for a checkpoint file
// / This gets written out to _last_checkpoint
type CheckPoint struct {
	/// Delta table version
	Version state.DeltaDataTypeVersion
	// 20 digits decimals
	Size DeltaDataTypeLong
	// 10 digits decimals
	Parts DeltaDataTypeInt
}

// / A single checkpoint entry in the checkpoint Parquet file
type CheckpointEntry[RowType any, PartitionType any] struct {
	Txn      *Txn                         `parquet:"txn"`
	Add      *Add[RowType, PartitionType] `parquet:"add"`
	Remove   *Remove                      `parquet:"remove"`
	MetaData *MetaData                    `parquet:"metaData"`
	Protocol *Protocol                    `parquet:"protocol"`
	Cdc      *Cdc                         `parquet:"-"`
}

// / Additional configuration for checkpointing
type CheckpointConfiguration struct {
	MaxRowsPerPart int
}

func NewCheckpointConfiguration() *CheckpointConfiguration {
	checkpointConfiguration := new(CheckpointConfiguration)
	// TODO try to find what Spark uses
	checkpointConfiguration.MaxRowsPerPart = 50000
	return checkpointConfiguration
}

var (
	ErrorCheckpointAlreadyExists    error = errors.New("checkpoint already exists")
	ErrorCheckpointRowCountMismatch error = errors.New("checkpoint generated with unexpected row count")
	ErrorCheckpointIncomplete       error = errors.New("checkpoint is missing parts")
	ErrorCheckpointInvalidFileName  error = errors.New("checkpoint file name is invalid")
)

func checkpointFromBytes(bytes []byte) (*CheckPoint, error) {
	checkpoint := new(CheckPoint)
	err := json.Unmarshal(bytes, checkpoint)
	if err != nil {
		return nil, err
	}
	return checkpoint, nil
}

func lastCheckpointPath() *storage.Path {
	path := storage.PathFromIter([]string{"_delta_log", "_last_checkpoint"})
	return &path
}

// / Return the checkpoint version and total parts, and the current part index if the URI is a valid checkpoint filename
// / If the checkpoint is single-part then part and checkpoint.Parts will both be zero
// / If the URI is not a valid checkpoint filename then checkpoint will be nil
func checkpointInfoFromURI(path *storage.Path) (checkpoint *CheckPoint, part DeltaDataTypeInt, parseErr error) {
	// Check for a single-part checkpoint
	groups := checkpointRegex.FindStringSubmatch(path.Raw)
	if len(groups) == 2 {
		checkpointVersionInt, err := strconv.ParseInt(groups[1], 10, 64)
		if err != nil {
			parseErr = err
			return
		}
		checkpoint = new(CheckPoint)
		checkpoint.Version = state.DeltaDataTypeVersion(checkpointVersionInt)
		checkpoint.Size = 0
		checkpoint.Parts = 0
		part = 0
		return
	}

	// Check for a multi part checkpoint
	groups = checkpointPartsRegex.FindStringSubmatch(path.Raw)
	if len(groups) == 4 {
		checkpointVersionInt, err := strconv.ParseInt(groups[1], 10, 64)
		if err != nil {
			parseErr = err
			return
		}
		partInt, err := strconv.ParseUint(groups[2], 10, 32)
		if err != nil {
			parseErr = err
			return
		}
		partsInt, err := strconv.ParseUint(groups[3], 10, 32)
		if err != nil {
			parseErr = err
			return
		}
		checkpoint = new(CheckPoint)
		checkpoint.Version = state.DeltaDataTypeVersion(checkpointVersionInt)
		checkpoint.Size = 0
		checkpoint.Parts = DeltaDataTypeInt(partsInt)
		part = DeltaDataTypeInt(partInt)
	}
	return
}

// / Check whether the given checkpoint version exists, either as a single- or multi-part checkpoint
func doesCheckpointVersionExist(store storage.ObjectStore, version DeltaDataTypeVersion, validateAllPartsExist bool) (bool, error) {
	// List all files starting with the version prefix.  This will also find commit logs and possible crc files
	str := fmt.Sprintf("%020d", version)
	path := storage.PathFromIter([]string{"_delta_log", str})
	possibleCheckpointFiles, err := store.List(&path, nil)
	if err != nil {
		return false, err
	}

	// Multi-part validation
	partsFound := make(map[DeltaDataTypeInt]bool, 10)
	totalParts := DeltaDataTypeInt(0)

	for _, possibleCheckpointFile := range possibleCheckpointFiles.Objects {
		checkpoint, currentPart, err := checkpointInfoFromURI(&possibleCheckpointFile.Location)
		if err != nil {
			return false, err
		}
		if checkpoint != nil {
			if checkpoint.Parts == 0 || !validateAllPartsExist {
				// If it's single-part or we're not validating multi-part, then we're done
				return true, nil
			}
			if totalParts > 0 && checkpoint.Parts != totalParts {
				return false, errors.Join(ErrorCheckpointInvalidFileName, fmt.Errorf("different number of total parts found between checkpoint files for version %d", version))
			}
			totalParts = checkpoint.Parts
			partsFound[currentPart] = true
		}
	}
	// Found a multi-part checkpoint and we want to validate that all parts exist
	if len(partsFound) > 0 {
		for i := DeltaDataTypeInt(0); i < totalParts; i++ {
			found, ok := partsFound[i+1]
			if !ok || !found {
				return false, ErrorCheckpointIncomplete
			}
		}
		return true, nil
	}
	return false, nil
}

// / Create a checkpoint for the given state in the given store
// / Assumes that checkpointing is locked such that no other process is currently trying to write a checkpoint for the same version
// / Applies tombstone expiration first
func createCheckpointFor[RowType any, PartitionType any](tableState *DeltaTableState[RowType, PartitionType], store storage.ObjectStore, checkpointConfiguration *CheckpointConfiguration) error {
	checkpointExists, err := doesCheckpointVersionExist(store, DeltaDataTypeVersion(tableState.Version), false)
	if err != nil {
		return err
	}
	if checkpointExists {
		return ErrorCheckpointAlreadyExists
	}

	tableState.prepareStateForCheckpoint()

	totalRows := len(tableState.Files) + len(tableState.Tombstones) + len(tableState.AppTransactionVersion) + 2
	numParts := ((totalRows - 1) / checkpointConfiguration.MaxRowsPerPart) + 1

	// From https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoints:
	// When writing multi-part checkpoints, the data must be clustered (either through hash or range partitioning)
	// by the 'path' of an added or removed file, or null otherwise. This ensures deterministic content in each
	// part file in case of multiple attempts to write the files.
	//
	// We are not doing this, because we are using a separate checkpointing lock so only one writer can checkpoint
	// at a time. (Note that this does not apply to external writers such as Spark.)
	// We are however sorting all entries, so the results should still be deterministic, except for the possibility
	// of tombstones expiring between different calls to the function.

	var parquetBytes []byte

	reportedParts := numParts
	if reportedParts == 1 {
		// Single part checkpoints are written as having 0 parts
		reportedParts = 0
	}

	offsetRow := 0
	for part := 0; part < numParts; part++ {
		records, err := tableState.checkpointRows(offsetRow, checkpointConfiguration.MaxRowsPerPart)
		if err != nil {
			return err
		}
		offsetRow += len(records)

		parquetBytes, err := checkpointParquetBytes(records)
		if err != nil {
			return err
		}
		var checkpointFileName string
		if numParts == 1 {
			checkpointFileName = fmt.Sprintf("%020d.checkpoint.parquet", tableState.Version)
		} else {
			checkpointFileName = fmt.Sprintf("%020d.checkpoint.%010d.%010d.parquet", tableState.Version, part+1, numParts)
		}
		checkpointPath := storage.PathFromIter([]string{"_delta_log", checkpointFileName})
		_, err = store.Head(&checkpointPath)
		if !errors.Is(err, storage.ErrorObjectDoesNotExist) {
			return ErrorCheckpointAlreadyExists
		}
		err = store.Put(&checkpointPath, parquetBytes)
		if err != nil {
			return err
		}
	}
	if offsetRow != totalRows {
		return ErrorCheckpointRowCountMismatch
	}

	checkpoint := CheckPoint{Version: tableState.Version, Size: DeltaDataTypeLong(len(parquetBytes)), Parts: DeltaDataTypeInt(reportedParts)}
	checkpointBytes, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	err = store.Put(lastCheckpointPath(), checkpointBytes)
	if err != nil {
		return err
	}
	return nil
}

// / Generate an Add action for a checkpoint (with additional fields) from a basic Add action
func checkpointAdd[RowType any, PartitionType any](add *Add[RowType, PartitionType]) (*Add[RowType, PartitionType], error) {
	checkpointAdd := new(Add[RowType, PartitionType])
	*checkpointAdd = *add
	checkpointAdd.DataChange = false

	stats, err := StatsFromJson([]byte(add.Stats))
	if err != nil {
		return nil, err
	}
	parsedStats, err := statsAsGenericStats[RowType](stats)
	if err != nil {
		return nil, err
	}
	checkpointAdd.StatsParsed = *parsedStats

	partitionValuesParsed, err := partitionValuesAsGeneric[PartitionType](add.PartitionValues)
	if err != nil {
		return nil, err
	}
	checkpointAdd.PartitionValuesParsed = *partitionValuesParsed

	return checkpointAdd, nil
}
