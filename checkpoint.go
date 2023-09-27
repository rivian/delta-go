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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/chelseajonesr/rfarrow"
	"github.com/google/uuid"
	"github.com/rivian/delta-go/storage"
	"golang.org/x/sync/errgroup"
)

// CheckPoint holds the metadata for a checkpoint file.
// This gets written out to _last_checkpoint.
type CheckPoint struct {
	/// Delta table version
	Version int64 `json:"version"`
	// The number of actions in the checkpoint. -1 if not available.
	Size int64 `json:"size"`
	// The number of parts if the checkpoint has multiple parts.  Omit if single part.
	Parts *int32 `json:"parts,omitempty"`
	// Size of the checkpoint in bytes
	SizeInBytes   int64 `json:"sizeInBytes"`
	NumOfAddFiles int64 `json:"numOfAddFiles"`
}

// CheckpointEntry contains a single entry in the checkpoint Parquet file
// All but one of the pointers should be nil
type CheckpointEntry struct {
	Txn      *Txn      `parquet:"name=txn"`
	Add      *Add      `parquet:"name=add"`
	Remove   *Remove   `parquet:"name=remove"`
	MetaData *MetaData `parquet:"name=metaData"`
	Protocol *Protocol `parquet:"name=protocol"`
	Cdc      *Cdc      `parquet:"-"` // CDC not implemented yet
}

// CheckpointConfiguration contains additional configuration for checkpointing
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
	// Configure use of on-disk intermediate storage to reduce memory requirements
	ReadWriteConfiguration OptimizeCheckpointConfiguration
}

// OptimizeCheckpointConfiguration holds settings for optimizing checkpoint read and write operations
type OptimizeCheckpointConfiguration struct {
	// Use an intermediate on-disk storage location to reduce memory
	OnDiskOptimization bool
	WorkingStore       storage.ObjectStore
	WorkingFolder      storage.Path
	// If these are > 1, checkpoint read and write operations will use this many goroutines
	ConcurrentCheckpointRead  int
	ConcurrentCheckpointWrite int
}

// NewCheckpointConfiguration returns the default configuration for creating checkpoints
func NewCheckpointConfiguration() *CheckpointConfiguration {
	checkpointConfiguration := new(CheckpointConfiguration)
	// From inspection of Spark generated checkpoint files
	checkpointConfiguration.MaxRowsPerPart = 50000
	checkpointConfiguration.UnsafeIgnoreUnsupportedReaderWriterVersionErrors = false
	checkpointConfiguration.DisableCleanup = false
	return checkpointConfiguration
}

// NewOptimizeCheckpointConfiguration returns a default enabled optimization configuration
// with a working folder in the table store's _delta_log/.tmp/ folder
// but no concurrency enabled
func NewOptimizeCheckpointConfiguration(table *Table, version int64) (*OptimizeCheckpointConfiguration, error) {
	optimizeCheckpointConfiguration := new(OptimizeCheckpointConfiguration)
	optimizeCheckpointConfiguration.OnDiskOptimization = true
	optimizeCheckpointConfiguration.WorkingStore = table.Store
	optimizeCheckpointConfiguration.WorkingFolder = storage.NewPath(fmt.Sprintf("_delta_log/.tmp/checkpoint-v%d-%s", version, uuid.NewString()))
	return optimizeCheckpointConfiguration, nil
}

var (
	// ErrCheckpointAlreadyExists is returned when trying to create a checkpoint but it already exists
	ErrCheckpointAlreadyExists error = errors.New("checkpoint already exists")
	// ErrCheckpointRowCountMismatch is returned when the checkpoint is generated with a different row count
	// than expected from the table state.  This indicates an internal error.
	ErrCheckpointRowCountMismatch error = errors.New("checkpoint generated with unexpected row count")
	// ErrCheckpointIncomplete is returned when trying to read a multi-part checkpoint but not all parts exist
	ErrCheckpointIncomplete error = errors.New("checkpoint is missing parts")
	// ErrCheckpointInvalidMultipartFileName is returned when a multi-part checkpoint file has the wrong number of parts in the filename
	ErrCheckpointInvalidMultipartFileName error = errors.New("checkpoint file name is invalid")
	// ErrCheckpointAddZeroSize is returned if there is an Add action with size 0
	// because including this would cause subsequent Optimize operations to fail.
	ErrCheckpointAddZeroSize error = errors.New("zero size in add not allowed")
	// ErrCheckpointEntryMultipleActions is returned if a checkpoint entry has more than one non-null action
	ErrCheckpointEntryMultipleActions error = errors.New("checkpoint entry contains multiple actions")
	// ErrWorkingFolder is returned if there is a problem with the optimization working folder
	ErrCheckpointOptimizationWorkingFolder error = errors.New("error using checkpoint optimization working folder")
)

func checkpointFromBytes(bytes []byte) (*CheckPoint, error) {
	checkpoint := new(CheckPoint)
	err := json.Unmarshal(bytes, checkpoint)
	if err != nil {
		return nil, err
	}
	return checkpoint, nil
}

func lastCheckpointPath() storage.Path {
	path := storage.PathFromIter([]string{"_delta_log", "_last_checkpoint"})
	return path
}

// / Return the checkpoint version and total parts, and the current part index if the URI is a valid checkpoint filename
// / If the checkpoint is single-part then part and checkpoint.Parts will both be zero
// / If the URI is not a valid checkpoint filename then checkpoint will be nil
func checkpointInfoFromURI(path storage.Path) (checkpoint *CheckPoint, part int32, parseErr error) {
	// Check for a single-part checkpoint
	groups := checkpointRegex.FindStringSubmatch(path.Base())
	if len(groups) == 2 {
		var version int64
		version, parseErr = strconv.ParseInt(groups[1], 10, 64)
		if parseErr != nil {
			return
		}
		checkpoint = new(CheckPoint)
		checkpoint.Version = version
		checkpoint.Size = 0
		part = 0
		return
	}

	// Check for a multi part checkpoint
	groups = checkpointPartsRegex.FindStringSubmatch(path.Base())
	if len(groups) == 4 {
		var version int64
		version, parseErr = strconv.ParseInt(groups[1], 10, 64)
		if parseErr != nil {
			return
		}
		var partInt64 int64
		var partsInt64 int64
		partInt64, parseErr = strconv.ParseInt(groups[2], 10, 32)
		if parseErr != nil {
			return
		}
		part = int32(partInt64)
		partsInt64, parseErr = strconv.ParseInt(groups[3], 10, 32)
		if parseErr != nil {
			return
		}
		parts := int32(partsInt64)

		checkpoint = new(CheckPoint)
		checkpoint.Version = version
		checkpoint.Size = 0
		checkpoint.Parts = &parts
	}
	return
}

// DoesCheckpointVersionExist returns true if the given checkpoint version exists, either as a single- or multi-part checkpoint
func DoesCheckpointVersionExist(store storage.ObjectStore, version int64, validateAllPartsExist bool) (bool, error) {
	// List all files starting with the version prefix.  This will also find commit logs and possible crc files
	str := fmt.Sprintf("%020d", version)
	path := storage.PathFromIter([]string{"_delta_log", str})
	possibleCheckpointFiles, err := store.List(path, nil)
	if err != nil {
		return false, err
	}

	// Multi-part validation
	partsFound := make(map[int32]bool, 10)
	totalParts := int32(0)

	for _, possibleCheckpointFile := range possibleCheckpointFiles.Objects {
		checkpoint, currentPart, err := checkpointInfoFromURI(possibleCheckpointFile.Location)
		if err != nil {
			return false, err
		}
		if checkpoint != nil {
			if checkpoint.Parts == nil || !validateAllPartsExist {
				// If it's single-part or we're not validating multi-part, then we're done
				return true, nil
			}
			if totalParts > 0 && *checkpoint.Parts != totalParts {
				return false, errors.Join(ErrCheckpointInvalidMultipartFileName, fmt.Errorf("different number of total parts found between checkpoint files for version %d", version))
			}
			totalParts = *checkpoint.Parts
			partsFound[currentPart] = true
		}
	}
	// Found a multi-part checkpoint and we want to validate that all parts exist
	if len(partsFound) > 0 {
		for i := int32(0); i < totalParts; i++ {
			found, ok := partsFound[i+1]
			if !ok || !found {
				return false, ErrCheckpointIncomplete
			}
		}
		return true, nil
	}
	return false, nil
}

// / Create a checkpoint for the given state in the given store
// / Assumes that checkpointing is locked such that no other process is currently trying to write a checkpoint for the same version
// / Applies tombstone expiration first
func createCheckpointFor(tableState *TableState, store storage.ObjectStore, checkpointConfiguration *CheckpointConfiguration) error {
	checkpointExists, err := DoesCheckpointVersionExist(store, tableState.Version, false)
	if err != nil {
		return err
	}
	if checkpointExists {
		return ErrCheckpointAlreadyExists
	}

	tableState.prepareStateForCheckpoint(&checkpointConfiguration.ReadWriteConfiguration)

	totalRows := tableState.FileCount() + tableState.TombstoneCount() + len(tableState.AppTransactionVersion) + 2
	numParts := int32(((totalRows - 1) / checkpointConfiguration.MaxRowsPerPart) + 1)

	// From https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoints:
	// When writing multi-part checkpoints, the data must be clustered (either through hash or range partitioning)
	// by the 'path' of an added or removed file, or null otherwise. This ensures deterministic content in each
	// part file in case of multiple attempts to write the files.
	//
	// We are not doing this, because we are using a separate checkpointing lock so only one writer can checkpoint
	// at a time. (Note that this does not apply to external writers such as Spark.)

	var totalBytes int64 = 0
	var rowsWritten int32 = 0

	generatePart := func(part int) error {
		partOffsetRow := part * checkpointConfiguration.MaxRowsPerPart
		checkpointEntries, err := checkpointRows(tableState, partOffsetRow, checkpointConfiguration)
		if err != nil {
			return err
		}
		atomic.AddInt32(&rowsWritten, int32(len(checkpointEntries)))

		buf := new(bytes.Buffer)
		props := parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Snappy),
		)
		err = rfarrow.WriteGoStructsToParquet(checkpointEntries, buf, props)
		if err != nil {
			return err
		}
		parquetBytes := buf.Bytes()
		var checkpointFileName string
		if numParts == 1 {
			checkpointFileName = fmt.Sprintf("%020d.checkpoint.parquet", tableState.Version)
		} else {
			checkpointFileName = fmt.Sprintf("%020d.checkpoint.%010d.%010d.parquet", tableState.Version, part+1, numParts)
		}
		checkpointPath := storage.PathFromIter([]string{"_delta_log", checkpointFileName})
		_, err = store.Head(checkpointPath)
		if !errors.Is(err, storage.ErrObjectDoesNotExist) {
			return errors.Join(ErrCheckpointAlreadyExists, fmt.Errorf("checkpoint file %s", checkpointPath.Raw))
		}
		err = store.Put(checkpointPath, parquetBytes)
		if err != nil {
			return err
		}
		atomic.AddInt64(&totalBytes, int64(len(parquetBytes)))
		return nil
	}

	// Optional concurrency support
	if checkpointConfiguration.ReadWriteConfiguration.ConcurrentCheckpointWrite > 1 {
		g, ctx := errgroup.WithContext(context.Background())
		partIndexChannel := make(chan int)

		for i := 0; i < checkpointConfiguration.ReadWriteConfiguration.ConcurrentCheckpointWrite; i++ {
			g.Go(func() error {
				for part := range partIndexChannel {
					err := generatePart(part)
					if err != nil {
						return err
					}
				}
				return nil
			})
		}
		g.Go(func() error {
			defer close(partIndexChannel)
			done := ctx.Done()
			for part := 0; part < int(numParts); part++ {
				if err := ctx.Err(); err != nil {
					return err
				}
				select {
				case partIndexChannel <- part:
					continue
				case <-done:
					return ctx.Err()
				}
			}
			return ctx.Err()
		})
		err := g.Wait()
		if err != nil {
			return err
		}
	} else {
		for part := 0; part < int(numParts); part++ {
			err := generatePart(part)
			if err != nil {
				return err
			}
		}
	}
	if int(rowsWritten) != totalRows {
		return errors.Join(ErrCheckpointRowCountMismatch, fmt.Errorf("expected %d rows, got %d rows", totalRows, rowsWritten))
	}

	var reportedParts *int32
	if numParts > 1 {
		// Only multipart checkpoints list the parts
		reportedParts = &numParts
	}

	checkpoint := CheckPoint{
		Version:       tableState.Version,
		Size:          int64(totalRows),
		SizeInBytes:   totalBytes,
		Parts:         reportedParts,
		NumOfAddFiles: int64(tableState.FileCount()),
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
// / Note that parsed stats and partition have been removed during the parquet library change.
// / TODO add them back
func checkpointAdd(add *Add) (*Add, error) {
	// stats, err := StatsFromJson([]byte(add.Stats))
	// if err != nil {
	// 	return nil, err
	// }
	// parsedStats, err := statsAsGenericStats[RowType](stats)
	// if err != nil {
	// 	return nil, err
	// }

	addDataChange := false
	checkpointAdd := new(Add)
	switch typedAdd := any(checkpointAdd).(type) {
	case *Add:
		// *typedAdd = *add
		typedAdd.DataChange = addDataChange
		typedAdd.ModificationTime = add.ModificationTime
		typedAdd.PartitionValues = add.PartitionValues
		typedAdd.Path = add.Path
		typedAdd.Size = add.Size
		if typedAdd.Size == 0 {
			return nil, errors.Join(ErrCheckpointAddZeroSize, fmt.Errorf("zero size add for path %s", add.Path))
		}
		typedAdd.Stats = add.Stats
		typedAdd.Tags = add.Tags
		// typedAdd.StatsParsed = *parsedStats
		// partitionValuesParsed, err := partitionValuesAsGeneric[PartitionType](add.PartitionValues)
		// if err != nil {
		// 	return checkpointAdd, err
		// }
		// typedAdd.PartitionValuesParsed = *partitionValuesParsed
	}
	return checkpointAdd, nil
}

type deletionCandidate struct {
	Version int64
	Meta    storage.ObjectMeta
}

// / If the maybeToDelete files are safe to delete, delete them.  Otherwise, clear them
// / "Safe to delete" is determined by the version and timestamp of the last file in the maybeToDelete list.
// / For more details see BufferingLogDeletionIterator() in https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/DeltaHistoryManager.scala
// / Returns the number of files deleted.
func flushDeleteFiles(store storage.ObjectStore, maybeToDelete []deletionCandidate, beforeVersion int64, maxTimestamp time.Time) (int, error) {
	deleted := 0

	if len(maybeToDelete) > 0 {
		lastMaybeToDelete := maybeToDelete[len(maybeToDelete)-1]
		if lastMaybeToDelete.Version < beforeVersion && lastMaybeToDelete.Meta.LastModified.UnixMilli() <= maxTimestamp.UnixMilli() {
			for _, deleteFile := range maybeToDelete {
				err := store.Delete(deleteFile.Meta.Location)
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
func removeExpiredLogsAndCheckpoints(beforeVersion int64, maxTimestamp time.Time, store storage.ObjectStore) (int, error) {
	if !store.IsListOrdered() {
		// Currently all object stores return list results sorted
		return 0, errors.Join(ErrNotImplemented, errors.New("removing expired logs is not implemented for this object store"))
	}

	candidatesForDeletion := make([]deletionCandidate, 0, 200)

	logIterator := storage.NewListIterator(BaseCommitURI(), store)

	// First collect all the logs/checkpoints that might be eligible for deletion
	for {
		meta, err := logIterator.Next()
		if errors.Is(err, storage.ErrObjectDoesNotExist) {
			break
		}
		isValid, version := CommitOrCheckpointVersionFromURI(meta.Location)
		// Spark and Rust clients also use the file's last updated timestamp rather than opening the commit and using internal state
		if isValid && version < beforeVersion && meta.LastModified.Before(maxTimestamp) {
			candidatesForDeletion = append(candidatesForDeletion, deletionCandidate{Version: version, Meta: *meta})
		}
		if version >= beforeVersion {
			break
		}
	}

	// Now look for actually deletable ones based on adjusted timestamp
	maybeToDelete := make([]deletionCandidate, 0, len(candidatesForDeletion))
	deletedCount := 0

	var lastFile, currentFile deletionCandidate

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
			currentFile = deletionCandidate{Version: currentFile.Version, Meta: storage.ObjectMeta{
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
