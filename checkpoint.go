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
	"time"

	"github.com/rivian/delta-go/state"
	"github.com/rivian/delta-go/storage"
)

// / Metadata for a checkpoint file
// / This gets written out to _last_checkpoint
type CheckPoint struct {
	/// Delta table version
	Version state.DeltaDataTypeVersion `json:"version"`
	// The number of actions in the checkpoint. -1 if not available.
	Size DeltaDataTypeLong `json:"size"`
	// The number of parts if the checkpoint has multiple parts.  Omit if single part.
	Parts *DeltaDataTypeInt `json:"parts,omitempty"`
	// Size of the checkpoint in bytes
	SizeInBytes   DeltaDataTypeLong `json:"sizeInBytes"`
	NumOfAddFiles DeltaDataTypeLong `json:"numOfAddFiles"`
}

// / A single checkpoint entry in the checkpoint Parquet file
type CheckpointEntry[RowType any, PartitionType any, AddType AddPartitioned[RowType, PartitionType] | Add[RowType]] struct {
	Txn      *Txn      `parquet:"name=txn"`
	Add      *AddType  `parquet:"name=add"`
	Remove   *Remove   `parquet:"name=remove"`
	MetaData *MetaData `parquet:"name=metaData"`
	Protocol *Protocol `parquet:"name=protocol"`
	// CDC not implemented yet
	Cdc *Cdc
}

// / Additional configuration for checkpointing
type CheckpointConfiguration struct {
	// Maximum numbers of rows to include in each multi-part checkpoint part
	// Current default 50k
	MaxRowsPerPart int
	// Allow checkpointing even if the table reader version or writer version is greater than supported
	// by this client. Defaults to false.
	// **WARNING** If you set this to true and the table being checkpointed uses features that are not supported by this
	// client, the resulting checkpoint might fail unpredictably and silently; this could cause data loss or corruption
	UnsafeIgnoreUnsupportedReaderWriterVersionErrors bool
	// Disable any cleanup after checkpointing, even if it was enabled in the table configuration.
	// Defaults to false.
	DisableCleanup bool
}

func NewCheckpointConfiguration() *CheckpointConfiguration {
	checkpointConfiguration := new(CheckpointConfiguration)
	// TODO try to find what Spark uses
	checkpointConfiguration.MaxRowsPerPart = 50000
	checkpointConfiguration.UnsafeIgnoreUnsupportedReaderWriterVersionErrors = false
	checkpointConfiguration.DisableCleanup = false
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
	groups := checkpointRegex.FindStringSubmatch(path.Base())
	if len(groups) == 2 {
		checkpointVersionInt, err := strconv.ParseInt(groups[1], 10, 64)
		if err != nil {
			parseErr = err
			return
		}
		checkpoint = new(CheckPoint)
		checkpoint.Version = state.DeltaDataTypeVersion(checkpointVersionInt)
		checkpoint.Size = 0
		part = 0
		return
	}

	// Check for a multi part checkpoint
	groups = checkpointPartsRegex.FindStringSubmatch(path.Base())
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
		partsDeltaInt := DeltaDataTypeInt(partsInt)
		checkpoint.Parts = &partsDeltaInt
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
			if checkpoint.Parts == nil || !validateAllPartsExist {
				// If it's single-part or we're not validating multi-part, then we're done
				return true, nil
			}
			if totalParts > 0 && *checkpoint.Parts != totalParts {
				return false, errors.Join(ErrorCheckpointInvalidFileName, fmt.Errorf("different number of total parts found between checkpoint files for version %d", version))
			}
			totalParts = *checkpoint.Parts
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

func createCheckpointFor[RowType any, PartitionType any](tableState *DeltaTableState[RowType, PartitionType], store storage.ObjectStore, checkpointConfiguration *CheckpointConfiguration) error {
	// Determine whether partitioned
	isPartitioned := !isPartitionTypeEmpty[PartitionType]()
	if isPartitioned {
		return createCheckpointWithAddType[RowType, PartitionType, AddPartitioned[RowType, PartitionType]](tableState, store, checkpointConfiguration)
	} else {
		return createCheckpointWithAddType[RowType, PartitionType, Add[RowType]](tableState, store, checkpointConfiguration)
	}
}

// / Create a checkpoint for the given state in the given store
// / Assumes that checkpointing is locked such that no other process is currently trying to write a checkpoint for the same version
// / Applies tombstone expiration first
func createCheckpointWithAddType[RowType any, PartitionType any, AddType AddPartitioned[RowType, PartitionType] | Add[RowType]](tableState *DeltaTableState[RowType, PartitionType], store storage.ObjectStore, checkpointConfiguration *CheckpointConfiguration) error {
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

	var totalBytes int64 = 0
	offsetRow := 0
	for part := 0; part < numParts; part++ {
		records, err := checkpointRows[RowType, PartitionType, AddType](tableState, offsetRow, checkpointConfiguration.MaxRowsPerPart)
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
		totalBytes += int64(len(parquetBytes))
	}
	if offsetRow != totalRows {
		return ErrorCheckpointRowCountMismatch
	}

	var reportedParts *DeltaDataTypeInt
	if numParts > 1 {
		// Only multipart checkpoints list the parts
		partsDeltaInt := DeltaDataTypeInt(numParts)
		reportedParts = &partsDeltaInt
	}

	checkpoint := CheckPoint{
		Version:       tableState.Version,
		Size:          DeltaDataTypeLong(totalRows),
		SizeInBytes:   DeltaDataTypeLong(totalBytes),
		Parts:         reportedParts,
		NumOfAddFiles: DeltaDataTypeLong(len(tableState.Files)),
	}
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
func checkpointAdd[RowType any, PartitionType any, AddType AddPartitioned[RowType, PartitionType] | Add[RowType]](add *AddPartitioned[RowType, PartitionType]) (*AddType, error) {
	stats, err := StatsFromJson([]byte(add.Stats))
	if err != nil {
		return nil, err
	}
	parsedStats, err := statsAsGenericStats[RowType](stats)
	if err != nil {
		return nil, err
	}

	checkpointAdd := new(AddType)
	switch typedAdd := any(checkpointAdd).(type) {
	case *AddPartitioned[RowType, PartitionType]:
		*typedAdd = *add
		typedAdd.DataChange = false
		typedAdd.StatsParsed = *parsedStats
		partitionValuesParsed, err := partitionValuesAsGeneric[PartitionType](add.PartitionValues)
		if err != nil {
			return checkpointAdd, err
		}
		typedAdd.PartitionValuesParsed = *partitionValuesParsed
	case *Add[RowType]:
		typedAdd.DataChange = false
		typedAdd.ModificationTime = add.ModificationTime
		typedAdd.PartitionValues = add.PartitionValues
		typedAdd.Path = add.Path
		typedAdd.Size = add.Size
		typedAdd.Stats = add.Stats
		typedAdd.Tags = add.Tags
		typedAdd.StatsParsed = *parsedStats
	}
	return checkpointAdd, nil
}

type DeletionCandidate struct {
	Version state.DeltaDataTypeVersion
	Meta    storage.ObjectMeta
}

// / If the maybeToDelete files are safe to delete, delete them.  Otherwise, clear them
// / "Safe to delete" is determined by the version and timestamp of the last file in the maybeToDelete list.
// / For more details see BufferingLogDeletionIterator() in https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/DeltaHistoryManager.scala
// / Returns the number of files deleted.
func flushDeleteFiles(store storage.ObjectStore, maybeToDelete []DeletionCandidate, beforeVersion state.DeltaDataTypeVersion, maxTimestamp time.Time) (int, error) {
	deleted := 0

	if len(maybeToDelete) > 0 {
		lastMaybeToDelete := maybeToDelete[len(maybeToDelete)-1]
		if lastMaybeToDelete.Version < beforeVersion && lastMaybeToDelete.Meta.LastModified.UnixMilli() <= maxTimestamp.UnixMilli() {
			for _, deleteFile := range maybeToDelete {
				err := store.Delete(&deleteFile.Meta.Location)
				if err != nil {
					return deleted, err
				}
				deleted++
			}
		}
	}

	return deleted, nil
}

// / *** The caller MUST validate that there is a checkpoint at or after beforeVersion before calling this ***
// / Remove any logs and checkpoints that have a last updated date before maxTimestamp and a version before beforeVersion
// / Last updated timestamps are required to be monotonically increasing, so there may be some time adjustment required
// / For more detail see BufferingLogDeletionIterator() in https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/DeltaHistoryManager.scala
func removeExpiredLogsAndCheckpoints(beforeVersion state.DeltaDataTypeVersion, maxTimestamp time.Time, store storage.ObjectStore) (int, error) {
	if !store.IsListOrdered() {
		// Currently all object stores return list results sorted
		return 0, errors.Join(ErrorNotImplemented, errors.New("removing expired logs is not implemented for this object store"))
	}

	candidatesForDeletion := make([]DeletionCandidate, 0, 200)

	logIterator := storage.NewListIterator(BaseCommitUri(), store)

	// First collect all the logs/checkpoints that might be eligible for deletion
	for {
		meta, err := logIterator.Next()
		if errors.Is(err, storage.ErrorObjectDoesNotExist) {
			break
		}
		isValid, version := CommitOrCheckpointVersionFromUri(&meta.Location)
		// Spark and Rust clients also use the file's last updated timestamp rather than opening the commit and using internal state
		if isValid && version < beforeVersion && meta.LastModified.Before(maxTimestamp) {
			candidatesForDeletion = append(candidatesForDeletion, DeletionCandidate{Version: version, Meta: *meta})
		}
		if version >= beforeVersion {
			break
		}
	}

	// Now look for actually deletable ones based on adjusted timestamp
	maybeToDelete := make([]DeletionCandidate, 0, len(candidatesForDeletion))
	deletedCount := 0

	var lastFile, currentFile DeletionCandidate

	if len(candidatesForDeletion) > 0 {
		lastFile = candidatesForDeletion[0]
		candidatesForDeletion = candidatesForDeletion[1:]
		maybeToDelete = append(maybeToDelete, lastFile)
	}

	for {
		if len(candidatesForDeletion) == 0 {
			deleted, err := flushDeleteFiles(store, maybeToDelete, beforeVersion, maxTimestamp)
			deletedCount += deleted
			return deletedCount, err
		}

		currentFile = candidatesForDeletion[0]
		candidatesForDeletion = candidatesForDeletion[1:]

		if lastFile.Version < currentFile.Version && lastFile.Meta.LastModified.UnixMilli() >= currentFile.Meta.LastModified.UnixMilli() {
			// The last version is earlier than the current, but the last timestamp is >= current: current needs time adjustment
			currentFile = DeletionCandidate{Version: currentFile.Version, Meta: storage.ObjectMeta{
				Location:     currentFile.Meta.Location,
				Size:         currentFile.Meta.Size,
				LastModified: lastFile.Meta.LastModified.Add(1 * time.Millisecond)}}
			// Then stick it on the end of the "maybe" list
			maybeToDelete = append(maybeToDelete, currentFile)
		} else {
			// No time adjustment needed.  Delete the contents of maybeToDelete if we can.
			// There is always at least one file in maybeToDelete here.
			deleted, err := flushDeleteFiles(store, maybeToDelete, beforeVersion, maxTimestamp)
			deletedCount += deleted
			if err != nil {
				return deletedCount, err
			}
			// If we were not able to delete the contents of maybeToDelete then we are done
			if deleted == 0 {
				return deletedCount, nil
			}
			maybeToDelete = maybeToDelete[:0]
			maybeToDelete = append(maybeToDelete, currentFile)
		}
		lastFile = currentFile
	}
}
