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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/apache/arrow/go/v14/parquet/schema"
	"github.com/chelseajonesr/rfarrow"
	"github.com/rivian/delta-go/storage"
	"golang.org/x/sync/errgroup"
)

// This file manages on-disk optimization of table state during checkpoint
// read and generation

// OnDiskTableState contains information about the table state that is stored on disk
// instead of in memory
type OnDiskTableState struct {
	// Add and remove actions that have been written to disk
	onDiskTempFiles []storage.Path
	// Mutexes for concurrent table state updates
	concurrentUpdateMutex *sync.Mutex
	// Count of adds
	onDiskFileCount int
	// Count of removes
	onDiskTombstoneCount int
	// And per-part counts of adds and removes, needed for checkpoint generation
	onDiskFileCountsPerPart      []int
	onDiskTombstoneCountsPerPart []int
	// Whether to remove extended file metadata from all tombstones in checkpoint generation
	onDiskRemoveExtendedFileMetadata bool
}

// Merge the current in-memory table state with existing on-disk state
// Do not merge newTableState. After this returns it will be added to the in-memory state.
func (ts *TableState) mergeOnDiskState(newTableState *TableState, maxRowsPerPart int, config *OptimizeCheckpointConfiguration, finalMerge bool) error {
	// Try to batch file updates before applying them to the on-disk files as that process can be slow
	// If we have incoming adds and existing removes, or vice versa, or if we have too many pending updates, then process the pending updates
	if finalMerge ||
		(len(ts.Files) > 0 && len(newTableState.Tombstones) > 0) ||
		(len(ts.Tombstones) > 0 && len(newTableState.Files) > 0) ||
		(len(ts.Files)+len(ts.Tombstones) > maxRowsPerPart) {
		appended := false

		mergeSinglePart := func(part int) error {
			tryAppend := part == len(ts.onDiskTempFiles)-1
			didAppend, addsDiff, tombstonesDiff, err := mergeNewAddsAndRemovesToOnDiskPartState(config.WorkingStore, ts.onDiskTempFiles[part], ts.Files, ts.Tombstones, maxRowsPerPart, tryAppend)
			if err != nil {
				return err
			}
			// Only one call to updateOnDiskState() will try to append, so only one (at most) goroutine will set appended here
			if didAppend {
				appended = true
			}
			// This is threadsafe
			if addsDiff != 0 || tombstonesDiff != 0 {
				ts.updateOnDiskCounts(addsDiff, tombstonesDiff)
			}
			return nil
		}
		// Optional concurrency support
		if config.ConcurrentCheckpointRead > 1 {
			g, ctx := errgroup.WithContext(context.Background())
			fileIndexChannel := make(chan int)

			for i := 0; i < config.ConcurrentCheckpointRead; i++ {
				g.Go(func() error {
					for part := range fileIndexChannel {
						err := mergeSinglePart(part)
						if err != nil {
							return err
						}
					}
					return nil
				})
			}
			g.Go(func() error {
				defer close(fileIndexChannel)
				done := ctx.Done()
				for i := range ts.onDiskTempFiles {
					if err := ctx.Err(); err != nil {
						return err
					}
					select {
					case fileIndexChannel <- i:
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
			// non-concurrent
			for part := range ts.onDiskTempFiles {
				err := mergeSinglePart(part)
				if err != nil {
					return err
				}
			}
		}

		if !appended {
			// Didn't append so create a new file instead
			exampleRecord, err := newCheckpointEntryRecord(0)
			if err != nil {
				return err
			}
			defer exampleRecord.Release()
			schemaDetails := new(tempFileSchemaDetails)
			err = schemaDetails.setFromArrowSchema(exampleRecord.Schema(), nil)
			if err != nil {
				return err
			}
			newRecord, err := newRecordForAddsAndRemoves(ts.Files, ts.Tombstones, schemaDetails.addFieldIndex, schemaDetails.removeFieldIndex)
			if err != nil {
				return err
			}
			defer newRecord.Release()
			(*schemaDetails).schema = newRecord.Schema()

			onDiskFile := storage.PathFromIter([]string{config.WorkingFolder.Raw, fmt.Sprintf("intermediate.%d.parquet", len(ts.onDiskTempFiles))})
			err = writeRecords(config.WorkingStore, onDiskFile, schemaDetails.schema, []arrow.Record{newRecord})
			if err != nil {
				return err
			}
			ts.onDiskTempFiles = append(ts.onDiskTempFiles, onDiskFile)
			ts.updateOnDiskCounts(len(ts.Files), len(ts.Tombstones))
		}
		// Reset the pending files and tombstones
		ts.Files = make(map[string]Add, 10000)
		ts.Tombstones = make(map[string]Remove, 10000)
	}
	return nil
}

// Merge new adds and removes with a single on-disk temp file
func mergeNewAddsAndRemovesToOnDiskPartState(
	store storage.ObjectStore, path storage.Path,
	newAdds map[string]Add, newRemoves map[string]Remove,
	maxRowsPerPart int, tryAppend bool) (bool, int, int, error) {
	var addDiffCount, tombstoneDiffCount int
	appended := false

	getRowsToNull := func(record arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, addRowsToNull *[]int64, removeRowsToNull *[]int64) {
		addPathArray := record.Column(arrowSchemaDetails.addFieldIndex).(*array.Struct).Field(arrowSchemaDetails.addPathFieldIndex).(*array.String)
		removePathArray := record.Column(arrowSchemaDetails.removeFieldIndex).(*array.Struct).Field(arrowSchemaDetails.removePathFieldIndex).(*array.String)
		// Note that although record.NumRows() returns an int64, both the IsNull() and Value() functions accept ints
		for row := 0; row < int(record.NumRows()); row++ {
			// Is there an add action in this row
			if !addPathArray.IsNull(row) {
				// If the file is now in tombstones, it needs to be removed from the add file list
				_, ok := newRemoves[addPathArray.Value(row)]
				if ok {
					*addRowsToNull = append(*addRowsToNull, int64(row))
				}
			}
			// Is there a remove action in this row
			if !removePathArray.IsNull(row) {
				// If the file has been re-added, it needs to be removed from the tombstones
				_, ok := newAdds[removePathArray.Value(row)]
				if ok {
					*removeRowsToNull = append(*removeRowsToNull, int64(row))
				}
			}
		}
		addDiffCount -= len(*addRowsToNull)
		tombstoneDiffCount -= len(*removeRowsToNull)
	}

	appendRows := func(records *[]arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, rowCount int) (bool, error) {
		// If we want to write out the new values, see if they fit in this file
		if tryAppend && (rowCount+len(newAdds)+len(newRemoves) < maxRowsPerPart) {
			// Existing checkpoint file schema may not match if generated from a different client; check first
			exampleRecord, err := newCheckpointEntryRecord(0)
			if err != nil {
				return false, err
			}
			defer exampleRecord.Release()
			if exampleRecord.Schema().Equal(arrowSchemaDetails.schema) {
				// newRecordForAddsAndRemoves returns a retained record, so we don't need to retain it again here
				newRecord, err := newRecordForAddsAndRemoves(newAdds, newRemoves, arrowSchemaDetails.addFieldIndex, arrowSchemaDetails.removeFieldIndex)
				if err != nil {
					return false, err
				}
				*records = append(*records, newRecord)
				addDiffCount += len(newAdds)
				tombstoneDiffCount += len(newRemoves)
				appended = true
				return true, nil
			}
		}
		return false, nil
	}

	err := updateOnDiskPartState(store, path, getRowsToNull, appendRows, nil)
	return appended, addDiffCount, tombstoneDiffCount, err
}

// Update the on-disk state in the given file with the new adds and removes
// This may be called concurrently on multiple checkpoint parts
func updateOnDiskPartState(
	store storage.ObjectStore, path storage.Path,
	getRowsToNull func(record arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, addRowsToNull *[]int64, removeRowsToNull *[]int64),
	appendRows func(records *[]arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, rowCount int) (bool, error),
	allocator memory.Allocator) error {

	changed := false

	tableReader, arrowSchemaDetails, deferFuncs, err := openFileForTableReader(store, path, nil)
	for _, d := range deferFuncs {
		defer d()
	}
	if err != nil {
		return err
	}

	// The initial tables will have a single record each
	// As we start appending new records while iterating the commit logs, we can expect multiple chunks per table
	rowCount := 0
	records := make([]arrow.Record, 0, 10)
	for tableReader.Next() {
		record := tableReader.Record()
		record.Retain()
		rowCount += int(record.NumRows())
		// Locate changes to the add and remove columns
		addRowsToNull := make([]int64, 0, rowCount)
		removeRowsToNull := make([]int64, 0, rowCount)
		getRowsToNull(record, arrowSchemaDetails, &addRowsToNull, &removeRowsToNull)

		// Copy the add and remove columns, including children, nulling the changed rows as we go
		var changedAdd arrow.Array
		var changedRemove arrow.Array
		if len(addRowsToNull) > 0 {
			changedAdd, err = copyArrowArrayWithNulls(record.Column(arrowSchemaDetails.addFieldIndex), addRowsToNull, allocator)
			if err != nil {
				return err
			}
			defer changedAdd.Release()
			// SetColumn returns a new record
			defer record.Release()
			record, err = record.SetColumn(arrowSchemaDetails.addFieldIndex, changedAdd)
			if err != nil {
				return err
			}
			changed = true
		}
		if len(removeRowsToNull) > 0 {
			changedRemove, err = copyArrowArrayWithNulls(record.Column(arrowSchemaDetails.removeFieldIndex), removeRowsToNull, allocator)
			if err != nil {
				return err
			}
			defer changedRemove.Release()
			defer record.Release()
			record, err = record.SetColumn(arrowSchemaDetails.removeFieldIndex, changedRemove)
			if err != nil {
				return err
			}
			changed = true
		}

		records = append(records, record)
	}

	// Callback to append additional rows
	appended, err := appendRows(&records, arrowSchemaDetails, rowCount)
	if err != nil {
		return err
	}
	changed = changed || appended

	if changed {
		err := writeRecords(store, path, arrowSchemaDetails.schema, records)
		if err != nil {
			return err
		}
	}

	for _, record := range records {
		record.Release()
	}
	return nil
}

// Count the adds and tombstones in a checkpoint file and add them to the state total
func countAddsAndTombstones(tableState *TableState, checkpointBytes []byte, arrowSchema *arrow.Schema, allocator memory.Allocator) (returnErr error) {
	if allocator == nil {
		allocator = memory.DefaultAllocator
	}
	arrowSchemaDetails := new(tempFileSchemaDetails)
	err := arrowSchemaDetails.setFromArrowSchema(arrowSchema, nil)
	if err != nil {
		return err
	}

	bytesReader := bytes.NewReader(checkpointBytes)
	parquetReader, err := file.NewParquetReader(bytesReader)
	if err != nil {
		return err
	}
	defer func() {
		if err := parquetReader.Close(); err != nil {
			returnErr = errors.Join(errors.New("failed to close Parquet reader"), err)
		}
	}()

	arrowRdr, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{Parallel: true, BatchSize: 10}, allocator)
	if err != nil {
		return err
	}

	tbl, err := arrowRdr.ReadTable(context.TODO())
	if err != nil {
		return err
	}
	defer tbl.Release()

	tableReader := array.NewTableReader(tbl, 0)
	defer tableReader.Release()

	rowCount := 0
	for tableReader.Next() {
		record := tableReader.Record()
		rowCount += int(record.NumRows())
		addPathArray := record.Column(arrowSchemaDetails.addFieldIndex).(*array.Struct).Field(arrowSchemaDetails.addPathFieldIndex).(*array.String)
		removePathArray := record.Column(arrowSchemaDetails.removeFieldIndex).(*array.Struct).Field(arrowSchemaDetails.removePathFieldIndex).(*array.String)
		tableState.updateOnDiskCounts(addPathArray.Len()-addPathArray.NullN(), removePathArray.Len()-removePathArray.NullN())
	}
	return nil
}

// Additional information used in updating a temporary on-disk checkpoint file
type tempFileSchemaDetails struct {
	schema                          *arrow.Schema
	addFieldIndex                   int
	addPathFieldIndex               int
	removeFieldIndex                int
	removePathFieldIndex            int
	removeExtendedFileMetadataIndex int
	removeDeletionTimestampIndex    int
}

// This only supports top level checkpoint column skipping, by using excludePrefixes
func (d *tempFileSchemaDetails) setFromArrowSchema(arrowSchema *arrow.Schema, excludePrefixes []string) error {
	d.schema = arrowSchema

	currentNonExcludedIdx := 0
	var addFieldIndexWithoutExclusions, removeFieldIndexWithoutExclusions int
	for i, field := range arrowSchema.Fields() {
		excluded := false
		for _, prefix := range excludePrefixes {
			if strings.HasPrefix(field.Name, prefix) {
				excluded = true
				break
			}
		}
		if field.Name == "add" {
			addFieldIndexWithoutExclusions = i
			d.addFieldIndex = currentNonExcludedIdx
		} else if field.Name == "remove" {
			removeFieldIndexWithoutExclusions = i
			d.removeFieldIndex = currentNonExcludedIdx
		}
		if !excluded {
			currentNonExcludedIdx++
		}
	}
	addStruct := arrowSchema.Field(addFieldIndexWithoutExclusions).Type.(*arrow.StructType)
	// Locate required field locations
	index, ok := addStruct.FieldIdx("path")
	if !ok {
		return errors.Join(ErrReadingCheckpoint, errors.New("temporary checkpoint file schema has invalid add.path column index"))
	}
	d.addPathFieldIndex = index
	removeStruct := arrowSchema.Field(removeFieldIndexWithoutExclusions).Type.(*arrow.StructType)
	index, ok = removeStruct.FieldIdx("path")
	if !ok {
		return errors.Join(ErrReadingCheckpoint, errors.New("temporary checkpoint file schema has invalid remove.path column index"))
	}
	d.removePathFieldIndex = index
	index, ok = removeStruct.FieldIdx("extendedFileMetadata")
	if !ok {
		return errors.Join(ErrReadingCheckpoint, errors.New("temporary checkpoint file schema has invalid remove.extendedFileMetadata column index"))
	}
	d.removeExtendedFileMetadataIndex = index
	index, ok = removeStruct.FieldIdx("deletionTimestamp")
	if !ok {
		return errors.Join(ErrReadingCheckpoint, errors.New("temporary checkpoint file schema has invalid remove.deletionTimestamp column index"))
	}
	d.removeDeletionTimestampIndex = index

	return nil
}

// Get a new record containing the adds and removes
// The record returned needs to be released
func newRecordForAddsAndRemoves(newAdds map[string]Add, newRemoves map[string]Remove, addFieldIndex int, removeFieldIndex int) (arrow.Record, error) {
	newRecord, err := newCheckpointEntryRecord(len(newAdds) + len(newRemoves))
	if err != nil {
		return nil, err
	}
	if len(newAdds) > 0 {
		addsSlice := make([]Add, len(newAdds))
		i := 0
		for _, ap := range newAdds {
			addsSlice[i] = ap
			i++
		}
		newAddsArray, err := newColumnArray(addsSlice, 0, len(newRemoves))
		defer newAddsArray.Release()
		if err != nil {
			return nil, err
		}
		// SetColumn returns a new record
		defer newRecord.Release()
		newRecord, err = newRecord.SetColumn(addFieldIndex, newAddsArray)
		if err != nil {
			return nil, err
		}
	}
	if len(newRemoves) > 0 {
		removesSlice := make([]Remove, len(newRemoves))
		i := 0
		for _, r := range newRemoves {
			removesSlice[i] = r
			i++
		}
		newRemovesArray, err := newColumnArray(removesSlice, len(newAdds), 0)
		defer newRemovesArray.Release()
		if err != nil {
			return nil, err
		}
		// SetColumn returns a new record
		defer newRecord.Release()
		newRecord, err = newRecord.SetColumn(removeFieldIndex, newRemovesArray)
		if err != nil {
			return nil, err
		}
	}
	return newRecord, nil
}

// The returned Array needs to be released
func newColumnArray[T any](newColumn []T, nullsBefore int, nullsAfter int) (arrow.Array, error) {
	columnBuilder, _, err := rfarrow.NewStructBuilderFromStructsWithAdditionalNullRows(newColumn, nullsBefore, nullsAfter)
	if err != nil {
		return nil, err
	}
	defer columnBuilder.Release()
	columnArray := columnBuilder.NewArray()
	return columnArray, nil
}

// The returned record needs to be released
func newCheckpointEntryRecord(count int) (arrow.Record, error) {
	defaultValue := new(CheckpointEntry)
	parquetSchema, err := schema.NewSchemaFromStruct(defaultValue)

	if err != nil {
		return nil, err
	}
	arrowSchema, err := pqarrow.FromParquet(parquetSchema, nil, nil)
	if err != nil {
		return nil, err
	}

	checkpointEntryBuilder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer checkpointEntryBuilder.Release()
	for _, field := range checkpointEntryBuilder.Fields() {
		field.AppendNulls(count)
	}
	record := checkpointEntryBuilder.NewRecord()
	return record, nil
}

// Copy arrow array data, setting any rows in nullRows to null
// The returned array needs to released
func copyArrowArrayWithNulls(in arrow.Array, nullRows []int64, allocator memory.Allocator) (arrow.Array, error) {
	if allocator == nil {
		allocator = memory.DefaultAllocator
	}
	recordBuilder := array.NewStructBuilder(allocator, in.DataType().(*arrow.StructType))
	var copiedComponents []arrow.Array = make([]arrow.Array, 0, len(nullRows)+1)
	lastIndexCopied := int64(-1)
	for _, nullIndex := range nullRows {
		// Copy any uncopied records before this null record
		if nullIndex > lastIndexCopied+1 {
			copiedSlice := array.NewSlice(in, lastIndexCopied+1, nullIndex)
			defer copiedSlice.Release()
			copiedComponents = append(copiedComponents, copiedSlice)
		}
		// Create a single length array that's all null
		recordBuilder.AppendNull()
		// NewStructArray() resets the StructBuilder so we don't have to
		nullArray := recordBuilder.NewStructArray()
		defer nullArray.Release()
		copiedComponents = append(copiedComponents, nullArray)
		lastIndexCopied = nullIndex
	}
	// Copy any uncopied records after the last null
	if in.Len() > int(lastIndexCopied)+1 {
		copiedSlice := array.NewSlice(in, lastIndexCopied+1, int64(in.Len()))
		defer copiedSlice.Release()
		copiedComponents = append(copiedComponents, copiedSlice)
	}
	return array.Concatenate(copiedComponents, allocator)
}

// Apply checkpoint parts to the table state concurrently
func (ts *TableState) applyCheckpointConcurrently(store storage.ObjectStore, checkpointDataPaths []storage.Path, config *OptimizeCheckpointConfiguration) error {
	var taskChannel chan checkpointProcessingTask
	g, ctx := errgroup.WithContext(context.Background())
	taskChannel = make(chan checkpointProcessingTask)

	for i := 0; i < config.ConcurrentCheckpointRead; i++ {
		g.Go(func() error {
			for t := range taskChannel {
				if err := stateFromCheckpointPart(t); err != nil {
					return err
				} else if err := ctx.Err(); err != nil {
					return err
				}
			}
			return nil
		})
	}
	g.Go(func() error {
		defer close(taskChannel)
		done := ctx.Done()
		for i, location := range checkpointDataPaths {
			if err := ctx.Err(); err != nil {
				return err
			}
			task := checkpointProcessingTask{store: store, location: location, state: ts, part: i, config: config}
			select {
			case taskChannel <- task:
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
	return nil
}

// Threadsafe: update state with changes to the number of adds/removes on disk
func (ts *TableState) updateOnDiskCounts(addsDiff int, tombstonesDiff int) {
	ts.concurrentUpdateMutex.Lock()
	defer ts.concurrentUpdateMutex.Unlock()
	ts.onDiskFileCount += addsDiff
	ts.onDiskTombstoneCount += tombstonesDiff
}

// Threadsafe: mark that tombstones were found without extended metadata
func (ts *TableState) setTombstoneWithoutExtendedMetadata() {
	ts.concurrentUpdateMutex.Lock()
	defer ts.concurrentUpdateMutex.Unlock()
	ts.onDiskRemoveExtendedFileMetadata = true
}

// Find out whether there are any non-extended metadata tombstones and null out any expired tombstones
// Also count adds and removes in each on disk temp file
func (ts *TableState) prepareOnDiskStateForCheckpoint(retentionTimestamp int64, config *OptimizeCheckpointConfiguration) error {
	ts.onDiskFileCountsPerPart = make([]int, len(ts.onDiskTempFiles))
	ts.onDiskTombstoneCountsPerPart = make([]int, len(ts.onDiskTempFiles))

	prepareSinglePart := func(part int) error {
		tombstoneWithoutExtendedMetadata, tombstonesDiff, partFileCount, partTombstoneCount, err := prepareOnDiskPartStateForCheckpoint(config.WorkingStore, ts.onDiskTempFiles[part], retentionTimestamp)
		if err != nil {
			return err
		}
		ts.onDiskFileCountsPerPart[part] = partFileCount
		ts.onDiskTombstoneCountsPerPart[part] = partTombstoneCount

		// These updates are threadsafe
		if tombstoneWithoutExtendedMetadata {
			ts.setTombstoneWithoutExtendedMetadata()
		}
		if tombstonesDiff != 0 {
			ts.updateOnDiskCounts(0, tombstonesDiff)
		}
		return nil
	}

	// Optional concurrency support
	if config.ConcurrentCheckpointRead > 1 {
		g, ctx := errgroup.WithContext(context.Background())
		fileIndexChannel := make(chan int)

		for i := 0; i < config.ConcurrentCheckpointRead; i++ {
			g.Go(func() error {
				for part := range fileIndexChannel {
					err := prepareSinglePart(part)
					if err != nil {
						return err
					}
				}
				return nil
			})
		}
		g.Go(func() error {
			defer close(fileIndexChannel)
			done := ctx.Done()
			for i := range ts.onDiskTempFiles {
				if err := ctx.Err(); err != nil {
					return err
				}
				select {
				case fileIndexChannel <- i:
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
		// non-concurrent
		for part := range ts.onDiskTempFiles {
			err := prepareSinglePart(part)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Find out whether there are any non-extended metadata tombstones and null out any expired tombstones
// This may be called concurrently on multiple checkpoint parts
func prepareOnDiskPartStateForCheckpoint(store storage.ObjectStore, path storage.Path, retentionTimestamp int64) (bool, int, int, int, error) {
	tombstoneDiffCount := 0
	tombstoneWithoutExtendedMetadata := false
	tombstoneCount := 0
	fileCount := 0
	getRowsToNull := func(record arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, addRowsToNull *[]int64, removeRowsToNull *[]int64) {
		removeArray := record.Column(arrowSchemaDetails.removeFieldIndex).(*array.Struct)
		addArray := record.Column(arrowSchemaDetails.addFieldIndex).(*array.Struct)
		removeExtendedFileMetadataArray := removeArray.Field(arrowSchemaDetails.removeExtendedFileMetadataIndex).(*array.Boolean)
		removeDeletionTimestampArray := removeArray.Field(arrowSchemaDetails.removeDeletionTimestampIndex).(*array.Int64)
		// Note that although record.NumRows() returns an int64, both the IsNull() and Value() functions accept ints
		for row := 0; row < int(record.NumRows()); row++ {
			// Is there a remove action in this row
			if removeArray.IsValid(row) {
				efm := removeExtendedFileMetadataArray.IsValid(row) && removeExtendedFileMetadataArray.Value(row)
				tombstoneWithoutExtendedMetadata = tombstoneWithoutExtendedMetadata || !efm

				if removeDeletionTimestampArray.IsValid(row) && removeDeletionTimestampArray.Value(row) <= retentionTimestamp {
					*removeRowsToNull = append(*removeRowsToNull, int64(row))
				}
			}
		}
		// Number of valid tombstones in the file: length of the array, minus length of nulls, minus length of rows we're about to null
		tombstoneCount += (removeArray.Len() - removeArray.NullN()) - len(*removeRowsToNull)
		fileCount += addArray.Len() - addArray.NullN()
		tombstoneDiffCount -= len(*removeRowsToNull)
	}
	appendRows := func(records *[]arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, rowCount int) (bool, error) {
		return false, nil
	}

	err := updateOnDiskPartState(store, path, getRowsToNull, appendRows, nil)
	return tombstoneWithoutExtendedMetadata, tombstoneDiffCount, fileCount, tombstoneCount, err
}

func onDiskRows(
	tableState *TableState, initialOffset int, checkpointRows *[]CheckpointEntry, config *CheckpointConfiguration,
	partRowCountArray []int, structFieldExclusions []string, arrowFieldExclusions []string, validityIndex func(*tempFileSchemaDetails) int) error {

	// Figure out which part file
	partRowsProcessed := 0
	for part, f := range tableState.onDiskTempFiles {
		currentPartRows := partRowCountArray[part]
		// We want to skip past "initialOffset" rows
		if initialOffset < partRowsProcessed+currentPartRows && currentPartRows > 0 {
			// Retrieve rows from this part

			// Use a function to make per-file defer cleanup simpler
			err := func() error {
				tableReader, schemaDetails, deferFuncs, err := openFileForTableReader(config.ReadWriteConfiguration.WorkingStore, f, arrowFieldExclusions)
				for _, d := range deferFuncs {
					defer d()
				}
				if err != nil {
					return err
				}

				// Which row to start on in this part
				partRowOffset := initialOffset - partRowsProcessed
				if partRowOffset < 0 {
					partRowOffset = 0
				}

				// How many rows to read in this part
				expectedRows := currentPartRows - partRowOffset
				if len(*checkpointRows)+expectedRows > config.MaxRowsPerPart {
					expectedRows = config.MaxRowsPerPart - len(*checkpointRows)
				}

				// Index mappings need to be generated each time because checkpoint temp files may not have the same schema
				schemaIndexMappings, err := rfarrow.MapGoStructFieldNamesToArrowIndices[CheckpointEntry](schemaDetails.schema.Fields(), structFieldExclusions, true)
				if err != nil {
					return err
				}

				// Allocate our new checkpoint entry rows
				entries := make([]*CheckpointEntry, expectedRows)
				for j := 0; j < expectedRows; j++ {
					t := new(CheckpointEntry)
					entries[j] = t
				}
				entryCount := 0
				columnIdx := validityIndex(schemaDetails)

				for tableReader.Next() && entryCount < expectedRows {
					record := tableReader.Record()
					requiredStructArray := record.Column(columnIdx).(*array.Struct)
					skippedRows := 0
					for row := 0; row < int(record.NumRows()) && entryCount < expectedRows; row++ {
						// Is there a required action in this row
						if requiredStructArray.IsValid(row) {
							if skippedRows >= partRowOffset {
								// Convert the action to a Go checkpoint entry
								// TODO - as a further optimization, skip converting to Go and back again.
								// However, the incoming checkpoint schema doesn't necessarily match our schema, so this will require
								// schema conversion inside Arrow.
								err = rfarrow.SetGoStructsFromArrowArrays([]reflect.Value{reflect.ValueOf(entries[entryCount])}, record.Columns(), schemaIndexMappings, row)
								if err != nil {
									return err
								}
								entryCount++
							} else {
								skippedRows++
							}
						}
					}
				}

				// Append the new checkpoint rows
				for j := 0; j < expectedRows; j++ {
					*checkpointRows = append(*checkpointRows, *entries[j])
				}
				return nil
			}()
			if err != nil {
				return err
			}
			if len(*checkpointRows) >= config.MaxRowsPerPart {
				return nil
			}
		}
		partRowsProcessed += currentPartRows
	}
	return nil
}

func onDiskTombstoneCheckpointRows(
	tableState *TableState, initialOffset int, checkpointRows *[]CheckpointEntry,
	config *CheckpointConfiguration) error {
	initialLength := len(*checkpointRows)
	err := onDiskRows(
		tableState, initialOffset, checkpointRows, config, tableState.onDiskTombstoneCountsPerPart,
		[]string{"Txn", "Add", "MetaData", "Protocol"},
		[]string{"txn", "add", "metaData", "protocol"},
		func(t *tempFileSchemaDetails) int { return t.removeFieldIndex })
	if err != nil {
		return err
	}
	if tableState.onDiskRemoveExtendedFileMetadata {
		for i := initialLength; i < len(*checkpointRows); i++ {
			if (*checkpointRows)[i].Remove != nil {
				(*checkpointRows)[i].Remove.ExtendedFileMetadata = false
			}
		}
	}
	return nil
}

func onDiskAddCheckpointRows(
	tableState *TableState, initialOffset int, checkpointRows *[]CheckpointEntry,
	config *CheckpointConfiguration) error {
	return onDiskRows(
		tableState, initialOffset, checkpointRows, config, tableState.onDiskFileCountsPerPart,
		[]string{"Txn", "Remove", "MetaData", "Protocol"},
		[]string{"txn", "remove", "metaData", "protocol"},
		func(t *tempFileSchemaDetails) int { return t.addFieldIndex })
}

// The slice of functions returned contains cleanup functions that should be immediately called with defer by the caller,
// even if an error is also returned.
// Also, the cleanup functions do cleanup for everything including the returned TableReader
// If excludePrefixes is set, the parquet schema will be adjusted to skip reading any columns starting with that prefix
func openFileForTableReader(store storage.ObjectStore, path storage.Path, excludePrefixes []string) (tableReader *array.TableReader, schemaDetails *tempFileSchemaDetails, deferFuncs []func(), returnErr error) {
	deferFuncs = make([]func(), 0, 3)
	checkpointBytes, err := store.Get(path)
	if err != nil {
		return nil, nil, deferFuncs, err
	}
	bytesReader := bytes.NewReader(checkpointBytes)
	parquetReader, err := file.NewParquetReader(bytesReader)
	if err != nil {
		return nil, nil, deferFuncs, err
	}
	closeIgnoreErr := func() {
		if err := parquetReader.Close(); err != nil {
			returnErr = err
		}
	}
	deferFuncs = append(deferFuncs, closeIgnoreErr)

	arrowRdr, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{Parallel: true, BatchSize: 10}, memory.DefaultAllocator)
	if err != nil {
		return nil, nil, deferFuncs, err
	}

	schemaDetails = new(tempFileSchemaDetails)
	arrowSchema, err := arrowRdr.Schema()
	if err != nil {
		return nil, nil, deferFuncs, err
	}
	err = schemaDetails.setFromArrowSchema(arrowSchema, excludePrefixes)
	if err != nil {
		return nil, nil, deferFuncs, err
	}

	var tbl arrow.Table

	if len(excludePrefixes) > 0 {
		pqSchema := parquetReader.MetaData().Schema
		allowedCols := make([]int, 0, 150)
	checkColumn:
		for i := 0; i < pqSchema.NumColumns(); i++ {
			columnPath := pqSchema.Column(i).ColumnPath().String()
			for _, prefix := range excludePrefixes {
				if strings.HasPrefix(columnPath, prefix) {
					continue checkColumn
				}
			}
			allowedCols = append(allowedCols, i)
		}
		rowgroups := make([]int, arrowRdr.ParquetReader().NumRowGroups())
		for i := 0; i < arrowRdr.ParquetReader().NumRowGroups(); i++ {
			rowgroups[i] = i
		}
		tbl, err = arrowRdr.ReadRowGroups(context.TODO(), allowedCols, rowgroups)
	} else {
		tbl, err = arrowRdr.ReadTable(context.TODO())
	}
	if err != nil {
		return nil, nil, deferFuncs, err
	}
	deferFuncs = append(deferFuncs, tbl.Release)

	tableReader = array.NewTableReader(tbl, 0)
	deferFuncs = append(deferFuncs, tableReader.Release)

	return tableReader, schemaDetails, deferFuncs, nil
}

// Write the given records to the store
func writeRecords(store storage.ObjectStore, path storage.Path, arrschema *arrow.Schema, records []arrow.Record) (returnErr error) {
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
	)
	// Spark uses int96 timestamps so we will too
	// This is currently used by stats_parsed, which we are stripping out of the final checkpoints,
	// so this is only needed to make sure that intermediate checkpoint parts are also readable by Spark.
	// However once we start writing our own stats_parsed we'll need to make use of this option in other write locations too.
	arrprops := pqarrow.NewArrowWriterProperties(pqarrow.WithDeprecatedInt96Timestamps(true))

	var w io.Writer
	var buf *bytes.Buffer

	if store.SupportsWriter() {
		wr, closeFunc, err := store.Writer(path, os.O_CREATE|os.O_TRUNC)
		if err != nil {
			return err
		}
		defer func() {
			if err := closeFunc(); err != nil {
				returnErr = err
			}
		}()
		w = wr
	} else {
		buf = new(bytes.Buffer)
		w = buf
	}

	var writer *pqarrow.FileWriter
	// Put the writer inside a function to simplify the defer Close() call;
	// we need writer.Close() to be called before store.Put()
	err := func() error {
		var innerErr error
		writer, innerErr = pqarrow.NewFileWriter(arrschema, w, props, arrprops)
		if innerErr != nil {
			return innerErr
		}

		defer func() {
			if err := writer.Close(); err != nil {
				returnErr = err
			}
		}()
		for _, record := range records {
			innerErr = writer.Write(record)
			if innerErr != nil {
				return innerErr
			}
		}
		return nil
	}()
	if err != nil {
		return err
	}

	if !store.SupportsWriter() {
		if err := store.Put(path, buf.Bytes()); err != nil {
			return errors.Join(errors.New("failed to add data to object at storage path"), err)
		}
	}
	return nil
}
