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
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/chelseajonesr/rfarrow"
	"github.com/google/uuid"
	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/filestore"
)

// Return a test struct array: [{‘joe’, 1}, {null, 2}, null, {‘mark’, 4}]
// Copied from Arrow example_test.go
// The returned array needs to be released
func getStructArray(t *testing.T, pool memory.Allocator) arrow.Array {
	t.Helper()
	dtype := arrow.StructOf([]arrow.Field{
		{Name: "f1", Type: arrow.ListOf(arrow.PrimitiveTypes.Uint8)},
		{Name: "f2", Type: arrow.PrimitiveTypes.Int32},
	}...)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()

	f1b := sb.FieldBuilder(0).(*array.ListBuilder)
	f1vb := f1b.ValueBuilder().(*array.Uint8Builder)
	f2b := sb.FieldBuilder(1).(*array.Int32Builder)

	sb.Reserve(4)
	f1vb.Reserve(7)
	f2b.Reserve(3)

	sb.Append(true)
	f1b.Append(true)
	f1vb.AppendValues([]byte("joe"), nil)
	f2b.Append(1)

	sb.Append(true)
	f1b.AppendNull()
	f2b.Append(2)

	sb.AppendNull()

	sb.Append(true)
	f1b.Append(true)
	f1vb.AppendValues([]byte("mark"), nil)
	f2b.Append(4)

	arr := sb.NewArray().(*array.Struct)
	return arr
}

func TestCopyArrowArrayWithNulls(t *testing.T) {
	nullRows := [][]int64{
		{},
		{0},
		{1},
		{2},
		{0, 1},
		{0, 2},
		{1, 2, 3},
		{3},
	}
	nullResults := [][]bool{
		{false, false, true, false},
		{true, false, true, false},
		{false, true, true, false},
		{false, false, true, false},
		{true, true, true, false},
		{true, false, true, false},
		{false, true, true, true},
		{false, false, true, true},
	}

	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	arr := getStructArray(t, pool)
	defer arr.Release()

	for i, nulls := range nullRows {
		withNulls, err := copyArrowArrayWithNulls(arr, nulls, pool)
		if err != nil {
			t.Error(err)
		}
		defer withNulls.Release()
		if withNulls.Len() != arr.Len() {
			t.Errorf("array length changed: %d to %d", arr.Len(), withNulls.Len())
		} else {
			for row := 0; row < 4; row++ {
				if nullResults[i][row] != withNulls.IsNull(row) {
					t.Errorf("expected null %t at test %d row %d", nullResults[i][row], i, row)
				}
			}
		}
	}
}

func TestUpdateOnDiskPartState(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	store, _, _, _ := setupCheckpointTest(t, "testdata/checkpoints/singlepart")
	path := storage.NewPath("_delta_log/00000000000000000010.checkpoint.parquet")
	b, err := store.Get(path)
	if err != nil {
		t.Error(err)
	}
	expectedEntries, err := rfarrow.ReadGoStructsFromParquet[CheckpointEntry](bytes.NewReader(b))
	if err != nil {
		t.Error(err)
	}
	expectedEntries[1].Add = nil
	expectedEntries[2].Add = nil
	expectedEntries[8].Add = nil
	expectedEntries[11].Remove = nil

	getRowsToNull := func(record arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, addRowsToNull *[]int64, removeRowsToNull *[]int64) {
		*addRowsToNull = append(*addRowsToNull, 1)
		*addRowsToNull = append(*addRowsToNull, 2)
		*addRowsToNull = append(*addRowsToNull, 8)
		*removeRowsToNull = append(*removeRowsToNull, 11)
	}
	appendRows := func(records *[]arrow.Record, arrowSchemaDetails *tempFileSchemaDetails, rowCount int) (bool, error) {
		return false, nil
	}
	err = updateOnDiskPartState(store, path, getRowsToNull, appendRows, pool)
	if err != nil {
		t.Error(err)
	}

	b, err = store.Get(path)
	if err != nil {
		t.Error(err)
	}
	modifiedEntries, err := rfarrow.ReadGoStructsFromParquet[CheckpointEntry](bytes.NewReader(b))
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(expectedEntries, modifiedEntries) {
		t.Error("entries do not match")
	}
}

func TestCountAddsAndTombstones(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	store, _, _, _ := setupCheckpointTest(t, "testdata/checkpoints/singlepart")
	path := storage.NewPath("_delta_log/00000000000000000010.checkpoint.parquet")

	b, err := store.Get(path)
	if err != nil {
		t.Error(err)
	}

	state := NewTableState(-1)

	bytesReader := bytes.NewReader(b)
	parquetReader, err := file.NewParquetReader(bytesReader)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := parquetReader.Close(); err != nil {
			t.Errorf("Failed to close Parquet reader: %v", err)
		}
	}()

	fileReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 10, Parallel: true}, pool)
	if err != nil {
		t.Fatal(err)
	}
	arrowSchema, err := fileReader.Schema()
	if err != nil {
		t.Fatal(err)
	}

	err = countAddsAndTombstones(state, b, arrowSchema, pool)
	if err != nil {
		t.Fatal(err)
	}
	if state.onDiskFileCount != 9 {
		t.Errorf("expected onDiskFileCount 9, found %d", state.onDiskFileCount)
	}
	if state.onDiskTombstoneCount != 1 {
		t.Errorf("expected onDiskTombstoneCount 1, found %d", state.onDiskTombstoneCount)
	}
}

func TestSetupOnDiskOptimization(t *testing.T) {
	store, _, _, _ := setupCheckpointTest(t, "")

	tableState := NewTableState(-1)
	config := OptimizeCheckpointConfiguration{OnDiskOptimization: false}

	err := setupOnDiskOptimization(nil, tableState, 0)
	if err != nil {
		t.Error(err)
	}
	if tableState.onDiskOptimization {
		t.Error("optimization should be disabled")
	}

	tableState = NewTableState(-1)
	err = setupOnDiskOptimization(&config, tableState, 0)
	if err != nil {
		t.Error(err)
	}
	if tableState.onDiskOptimization {
		t.Error("optimization should be disabled")
	}

	config.OnDiskOptimization = true
	config.WorkingFolder = storage.NewPath(".tmp")
	tableState = NewTableState(-1)
	err = setupOnDiskOptimization(&config, tableState, 0)
	if !errors.Is(err, ErrCheckpointOptimizationWorkingFolder) {
		t.Errorf("expected error setting optimization with no working store/folder but returned %v", err)
	}
	if tableState.onDiskOptimization {
		t.Error("optimization should be disabled")
	}

	config.WorkingStore = store
	tableState = NewTableState(-1)
	err = setupOnDiskOptimization(&config, tableState, 0)
	if err != nil {
		t.Error(err)
	}
	if !tableState.onDiskOptimization {
		t.Error("optimization should be enabled")
	}
	if cap(tableState.onDiskTempFiles) != 0 || len(tableState.onDiskTempFiles) != 0 {
		t.Error("temp files length and capacity should be 0")
	}

	tableState = NewTableState(-1)
	err = setupOnDiskOptimization(&config, tableState, 7)
	if err != nil {
		t.Error(err)
	}
	if !tableState.onDiskOptimization {
		t.Error("optimization should be enabled")
	}
	if cap(tableState.onDiskTempFiles) != 7 || len(tableState.onDiskTempFiles) != 0 {
		t.Error("temp files length should be 0 and capacity should be 7")
	}

	tableState = NewTableState(-1)

	filePath := storage.NewPath(filepath.Join(config.WorkingFolder.Raw, "test.txt"))
	if err := store.Put(filePath, []byte{1, 2, 3}); err != nil {
		t.Errorf("Failed to add file %s to file system", filePath)
	}

	err = setupOnDiskOptimization(&config, tableState, 0)
	if !errors.Is(err, ErrCheckpointOptimizationWorkingFolder) {
		t.Errorf("expected error setting optimization with non empty working store/folder but returned %v", err)
	}
	if tableState.onDiskOptimization {
		t.Error("optimization should be disabled")
	}
}

func TestOnDiskTombstoneCheckpointRows(t *testing.T) {
	rootDir := "testdata/checkpoints/ondiskstate"
	store := filestore.New(storage.NewPath(rootDir))
	ts := NewTableState(2)
	ts.onDiskTempFiles = []storage.Path{storage.NewPath("intermediate.1.parquet"), storage.NewPath("intermediate.2.parquet")}
	ts.onDiskFileCountsPerPart = []int{3, 5}
	ts.onDiskTombstoneCountsPerPart = []int{11, 10}
	ts.onDiskFileCount = 8
	ts.onDiskTombstoneCount = 21
	ts.onDiskOptimization = true

	config := NewCheckpointConfiguration()
	config.MaxRowsPerPart = 10

	optimizeConfig, err := NewOptimizeCheckpointConfiguration(store, 2)
	if err != nil {
		t.Fatal(err)
	}
	optimizeConfig.WorkingFolder = storage.NewPath("")
	config.ReadWriteConfiguration = *optimizeConfig

	checkpointRows := make([]CheckpointEntry, 0, 10)
	checkpointRows = append(checkpointRows, CheckpointEntry{MetaData: &MetaData{ID: uuid.New()}})
	checkpointRows = append(checkpointRows, CheckpointEntry{Protocol: &Protocol{MinReaderVersion: 1, MinWriterVersion: 1}})

	err = onDiskTombstoneCheckpointRows(ts, 0, &checkpointRows, config)

	// Test: fill in remainder after the first 2 rows, and all removes are in intermediate.1
	if err != nil {
		t.Error(err)
	} else if len(checkpointRows) != 10 {
		t.Errorf("expected 10 rows, got %d", len(checkpointRows))
	} else {
		// Expected results
		if checkpointRows[0].MetaData == nil {
			t.Error("expected metadata in row 0")
		}
		if checkpointRows[1].Protocol == nil {
			t.Error("expected protocol in row 1")
		}
		for i := 0; i < 8; i++ {
			if checkpointRows[i+2].Remove == nil || (checkpointRows[i+2].Remove.Path != fmt.Sprintf("r%d", i+1)) {
				t.Errorf("expected remove r%d in row %d, got %v", i+1, i+2, checkpointRows[i+2].Remove.Path)
			}
		}
	}

	// Test: fill in 10 rows from across both files, with an initial offset
	checkpointRows = make([]CheckpointEntry, 0, 10)
	err = onDiskTombstoneCheckpointRows(ts, 8, &checkpointRows, config)
	if err != nil {
		t.Error(err)
	} else if len(checkpointRows) != 10 {
		t.Errorf("expected 10 rows, got %d", len(checkpointRows))
	} else {
		for i := 0; i < 10; i++ {
			if checkpointRows[i].Remove == nil || (checkpointRows[i].Remove.Path != fmt.Sprintf("r%d", i+9)) {
				t.Errorf("expected remove r%d in row %d, got %v", i+9, i, checkpointRows[i].Remove.Path)
			}
		}
	}

	// Test: fill in 3 rows from second file, with an initial offset
	checkpointRows = make([]CheckpointEntry, 0, 10)
	err = onDiskTombstoneCheckpointRows(ts, 18, &checkpointRows, config)
	if err != nil {
		t.Error(err)
	} else if len(checkpointRows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(checkpointRows))
	} else {
		for i := 0; i < 3; i++ {
			if checkpointRows[i].Remove == nil || (checkpointRows[i].Remove.Path != fmt.Sprintf("r%d", i+19)) {
				t.Errorf("expected remove r%d in row %d, got %v", i+19, i, checkpointRows[i].Remove.Path)
			}
		}
	}

	// Test: start reading rows at the beginning of the second file
	checkpointRows = make([]CheckpointEntry, 0, 10)
	err = onDiskTombstoneCheckpointRows(ts, 11, &checkpointRows, config)
	if err != nil {
		t.Error(err)
	} else if len(checkpointRows) != 10 {
		t.Errorf("expected 10 rows, got %d", len(checkpointRows))
	} else {
		for i := 0; i < 10; i++ {
			if checkpointRows[i].Remove == nil || (checkpointRows[i].Remove.Path != fmt.Sprintf("r%d", i+12)) {
				t.Errorf("expected remove r%d in row %d, got %v", i+12, i, checkpointRows[i].Remove.Path)
			}
		}
	}
}

func TestOnDiskAddCheckpointRows(t *testing.T) {
	rootDir := "testdata/checkpoints/ondiskstate"
	store := filestore.New(storage.NewPath(rootDir))
	ts := NewTableState(2)
	ts.onDiskTempFiles = []storage.Path{storage.NewPath("intermediate.2.parquet"), storage.NewPath("intermediate.1.parquet")}
	ts.onDiskFileCountsPerPart = []int{5, 2}
	ts.onDiskTombstoneCountsPerPart = []int{10, 11}
	ts.onDiskFileCount = 7
	ts.onDiskTombstoneCount = 21
	ts.onDiskOptimization = true

	config := NewCheckpointConfiguration()
	config.MaxRowsPerPart = 5

	optimizeConfig, err := NewOptimizeCheckpointConfiguration(store, 2)
	if err != nil {
		t.Fatal(err)
	}
	optimizeConfig.WorkingFolder = storage.NewPath("")
	config.ReadWriteConfiguration = *optimizeConfig

	checkpointRows := make([]CheckpointEntry, 0, 10)
	checkpointRows = append(checkpointRows, CheckpointEntry{MetaData: &MetaData{ID: uuid.New()}})
	checkpointRows = append(checkpointRows, CheckpointEntry{Protocol: &Protocol{MinReaderVersion: 1, MinWriterVersion: 1}})

	err = onDiskAddCheckpointRows(ts, 0, &checkpointRows, config)

	// Test: fill in remainder after the first 2 rows, and all adds are in intermediate.2 (first file)
	if err != nil {
		t.Error(err)
	} else if len(checkpointRows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(checkpointRows))
	} else {
		// Expected results
		if checkpointRows[0].MetaData == nil {
			t.Error("expected metadata in row 0")
		}
		if checkpointRows[1].Protocol == nil {
			t.Error("expected protocol in row 1")
		}
		for i := 0; i < 3; i++ {
			if checkpointRows[i+2].Add == nil || (checkpointRows[i+2].Add.Path != fmt.Sprintf("a%d", i+1)) {
				t.Errorf("expected add a%d in row %d, got %v", i+1, i+2, checkpointRows[i+2].Add.Path)
			}
		}
	}

	// Test: fill in 5 rows, and all adds are in intermediate.2 (first file)
	checkpointRows = make([]CheckpointEntry, 0, 10)
	err = onDiskAddCheckpointRows(ts, 0, &checkpointRows, config)
	if err != nil {
		t.Error(err)
	} else if len(checkpointRows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(checkpointRows))
	} else {
		// Expected results
		for i := 0; i < 5; i++ {
			if checkpointRows[i].Add == nil || (checkpointRows[i].Add.Path != fmt.Sprintf("a%d", i+1)) {
				t.Errorf("expected add a%d in row %d, got %v", i+1, i, checkpointRows[i].Add.Path)
			}
		}
	}

	// Test: fill in all rows from across both files
	checkpointRows = make([]CheckpointEntry, 0, 5)
	config.MaxRowsPerPart = 10
	err = onDiskAddCheckpointRows(ts, 0, &checkpointRows, config)
	if err != nil {
		t.Error(err)
	} else if len(checkpointRows) != 7 {
		t.Errorf("expected 7 rows, got %d", len(checkpointRows))
	} else {
		for i := 0; i < 7; i++ {
			if checkpointRows[i].Add == nil || (checkpointRows[i].Add.Path != fmt.Sprintf("a%d", i+1)) {
				t.Errorf("expected add a%d in row %d, got %v", i+1, i, checkpointRows[i].Add.Path)
			}
		}
	}

	// Test: fill in 2 rows from second file, with an initial offset
	checkpointRows = make([]CheckpointEntry, 0, 10)
	err = onDiskAddCheckpointRows(ts, 5, &checkpointRows, config)
	if err != nil {
		t.Error(err)
	} else if len(checkpointRows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(checkpointRows))
	} else {
		for i := 0; i < 2; i++ {
			if checkpointRows[i].Add == nil || (checkpointRows[i].Add.Path != fmt.Sprintf("a%d", i+6)) {
				t.Errorf("expected add a%d in row %d, got %v", i+6, i, checkpointRows[i].Add.Path)
			}
		}
	}
}
