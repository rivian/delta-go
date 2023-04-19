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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rivian/delta-go/lock/filelock"
	"github.com/rivian/delta-go/state/filestate"
	"github.com/segmentio/parquet-go"

	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/filestore"
)

func TestDeltaTransactionPrepareCommit(t *testing.T) {
	store := filestore.FileObjectStore{BaseURI: &storage.Path{Raw: "tmp/"}}
	deltaTable := DeltaTable[EmptyTestStruct, EmptyTestStruct]{Store: &store, LockClient: &filelock.FileLock{Key: "tmp/_delta_log/_commit.lock"}}
	options := DeltaTransactionOptions{MaxRetryCommitAttempts: 3}
	os.MkdirAll("tmp/_delta_log/", 0700)
	defer os.RemoveAll("tmp/_delta_log/")
	transaction := NewDeltaTransaction(&deltaTable, &options)
	add := Add[EmptyTestStruct, EmptyTestStruct]{
		Path:             "part-00000-80a9bb40-ec43-43b6-bb8a-fc66ef7cd768-c000.snappy.parquet",
		Size:             984,
		ModificationTime: DeltaDataTypeTimestamp(time.Now().UnixMilli()),
	}
	transaction.AddAction(add)
	operation := Write{Mode: Overwrite}
	appMetaData := make(map[string]any)
	appMetaData["test"] = 123
	commit, err := transaction.PrepareCommit(operation, appMetaData)
	if err != nil {
		t.Error("should be locked")
	}

	if commit.URI.Ext() != ".tmp" {
		t.Errorf("extension should be .tmp, has %s", commit.URI.Ext())
	}

	commitFullPath := filepath.Join(store.BaseURI.Base(), commit.URI.Raw)
	exists := fileExists(commitFullPath)
	if !exists {
		t.Error("commit file does not exist")
	}

	b, err := os.ReadFile(commitFullPath)
	if err != nil {
		t.Error("should be locked")
	}
	if !strings.Contains(string(b), "part-00000-80a9bb40-ec43-43b6-bb8a-fc66ef7cd768-c000.snappy.parquet") {
		t.Errorf("File should contain add part file")
	}
}

func TestDeltaTableReadCommitVersion(t *testing.T) {
	table, _, _ := setupTest(t)
	table.Create(DeltaTableMetaData{}, Protocol{}, CommitInfo{}, []Add[testData, EmptyTestStruct]{})
	transaction, operation, appMetaData := setupTransaction(t, table, nil)
	commit, err := transaction.PrepareCommit(operation, appMetaData)
	if err != nil {
		t.Fatal(err)
	}
	err = transaction.TryCommit(&commit)
	if err != nil {
		t.Fatal(err)
	}

	actions, err := table.ReadCommitVersion(0)
	if err != nil {
		t.Fatal(err)
	}

	if len(actions) != 3 {
		t.Fatal("Expected 3 actions")
	}
	_, ok := actions[0].(*CommitInfo)
	if !ok {
		t.Error("Expected CommitInfo for first action")
	}
	_, ok = actions[1].(*Protocol)
	if !ok {
		t.Error("Expected Protocol for second action")
	}
	_, ok = actions[2].(*MetaData)
	if !ok {
		t.Error("Expected MetaData for third action")
	}
}

func TestDeltaTableReadCommitVersionWithAddStats(t *testing.T) {
	table, _, _ := setupTest(t)

	firstFieldMetaData := make(map[string]any)
	firstFieldMetaData["hasMetaData"] = true
	fields := []SchemaField{
		{Name: "first_column", Nullable: true, Metadata: firstFieldMetaData},
		{Name: "second_column", Nullable: false},
		{Name: "third_field", Nullable: true},
	}
	schema := SchemaTypeStruct{Fields: fields}
	format := new(Format).Default()
	config := make(map[string]string)
	config["appendOnly"] = "true"
	metadata := NewDeltaTableMetaData("Test Table", "", format, schema, []string{}, config)
	protocol := Protocol{MinReaderVersion: 2, MinWriterVersion: 6}
	stats := Stats{NumRecords: 1, MinValues: map[string]any{"first_column": 1}}
	add := Add[testData, EmptyTestStruct]{
		Path:             "part-123.snappy.parquet",
		Size:             984,
		ModificationTime: DeltaDataTypeTimestamp(time.Now().UnixMilli()),
		Stats:            string(stats.Json()),
	}
	commitInfo := make(map[string]any)
	commitInfo["test"] = 123
	err := table.Create(*metadata, protocol, commitInfo, []Add[testData, EmptyTestStruct]{add})
	if err != nil {
		t.Error(err)
	}

	transaction, operation, appMetaData := setupTransaction(t, table, nil)
	commit, err := transaction.PrepareCommit(operation, appMetaData)
	if err != nil {
		t.Fatal(err)
	}
	err = transaction.TryCommit(&commit)
	if err != nil {
		t.Fatal(err)
	}

	actions, err := table.ReadCommitVersion(0)
	if err != nil {
		t.Fatal(err)
	}

	if len(actions) != 4 {
		t.Error("Expected 3 actions")
	}
	_, ok := actions[0].(*CommitInfo)
	if !ok {
		t.Error("Expected CommitInfo for first action")
	}
	_, ok = actions[1].(*Protocol)
	if !ok {
		t.Error("Expected Protocol for second action")
	}
	_, ok = actions[2].(*MetaData)
	if !ok {
		t.Error("Expected MetaData for third action")
	}
	a, ok := actions[3].(*Add[testData, EmptyTestStruct])
	if !ok {
		t.Error("Expected Add for fourth action")
	}
	if a.Path != "part-123.snappy.parquet" {
		t.Error("Add path not deserialized properly")
	}
	var s Stats
	err = json.Unmarshal([]byte(a.Stats), &s)
	if err != nil {
		t.Fatal(err)
	}
	if s.NumRecords != 1 {
		t.Error("Add path stats NumRecords incorrect")
	}
	if s.MinValues["first_column"].(float64) != 1 {
		t.Error("Add path stats MinValues incorrect")
	}
}

func TestDeltaTableTryCommitTransaction(t *testing.T) {
	table, _, _ := setupTest(t)
	table.Create(DeltaTableMetaData{}, Protocol{}, CommitInfo{}, []Add[testData, EmptyTestStruct]{})
	transaction, operation, appMetaData := setupTransaction(t, table, nil)
	commit, err := transaction.PrepareCommit(operation, appMetaData)
	if err != nil {
		t.Error(err)
	}
	err = transaction.TryCommit(&commit)
	if err != nil {
		t.Error(err)
	}
	if transaction.DeltaTable.State.Version != 1 {
		t.Errorf("want version = 1,has version = %d", transaction.DeltaTable.State.Version)
	}

	//try again with the same version
	err = transaction.TryCommit(&commit)
	if !errors.Is(err, storage.ErrorObjectDoesNotExist) {
		t.Error(err)
	}
	if transaction.DeltaTable.State.Version != 2 {
		t.Errorf("want version = 1,has version = %d", transaction.DeltaTable.State.Version)
	}

	//prpare a new commit and try on the next version
	commit, _ = transaction.PrepareCommit(operation, appMetaData)
	err = transaction.TryCommit(&commit)
	if err != nil {
		t.Error(err)
	}
	if transaction.DeltaTable.State.Version != 3 {
		t.Errorf("want version = 2,has version = %d", transaction.DeltaTable.State.Version)
	}

}

func TestTryCommitWithExistingLock(t *testing.T) {

	tmpDir := t.TempDir()
	fileLockKey := filepath.Join(tmpDir, "_delta_log/_commit.lock")
	os.MkdirAll(filepath.Dir(fileLockKey), 0700)

	tmpPath := storage.NewPath(tmpDir)
	//Create w0 commit lock
	w0lockClient := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.LockOptions{TTL: 10 * time.Second})
	w0lockClient.TryLock()
	//try_commit while lock is held by w0
	lockClient := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.LockOptions{TTL: 60 * time.Second})
	// lockClient.Unlock()

	store := filestore.New(tmpPath)
	state := filestate.New(tmpPath, "_delta_log/_commit.state")

	table := NewDeltaTable[testData, EmptyTestStruct](store, lockClient, state)

	transaction, operation, appMetaData := setupTransaction(t, table, &DeltaTransactionOptions{MaxRetryCommitAttempts: 3})
	version, err := transaction.Commit(operation, appMetaData)
	if !errors.Is(err, ErrorExceededCommitRetryAttempts) {
		t.Error(err)
	}
	if version != -1 {
		t.Errorf("version stored in table.State.Version was never synced so is -1")
	}

	commitData, _ := state.Get()

	if commitData.Version != 0 {
		t.Errorf("commitData.Version is set as table.State.Version+1")
	}

	//try_commit after w0.Unlock()
	w0lockClient.Unlock()
	time.Sleep(time.Second)
	version, err = transaction.Commit(operation, appMetaData)
	if version != 1 {
		t.Errorf("version stored in table.State.Version was synced to remote state to 1")
	}
	if err != nil {
		t.Error(err)
	}

	commitData, _ = state.Get()
	if commitData.Version != 1 {
		t.Errorf("Final Version in lock should be 1")
	}
}
func TestDeltaTableCreate(t *testing.T) {
	table, state, _ := setupTest(t)
	//schema
	firstFieldMetaData := make(map[string]any)
	firstFieldMetaData["hasMetaData"] = true
	fields := []SchemaField{
		{Name: "first_column", Nullable: true, Metadata: firstFieldMetaData},
		{Name: "second_column", Nullable: false},
		{Name: "third_field", Nullable: true},
	}
	schema := SchemaTypeStruct{Fields: fields}
	format := new(Format).Default()
	config := make(map[string]string)
	config["appendOnly"] = "true"
	metadata := NewDeltaTableMetaData("Test Table", "", format, schema, []string{}, config)
	protocol := Protocol{MinReaderVersion: 2, MinWriterVersion: 7}
	add := Add[testData, EmptyTestStruct]{
		Path:             "part-00000-80a9bb40-ec43-43b6-bb8a-fc66ef7cd768-c000.snappy.parquet",
		Size:             984,
		ModificationTime: DeltaDataTypeTimestamp(time.Now().UnixMilli()),
	}
	commitInfo := make(map[string]any)
	commitInfo["test"] = 123
	err := table.Create(*metadata, protocol, commitInfo, []Add[testData, EmptyTestStruct]{add})
	if err != nil {
		t.Error(err)
	}
	// operation := Write{Mode: Overwrite}

	data, err := state.Get()
	if err != nil {
		t.Error(err)
	}
	if data.Version != 0 {
		t.Errorf("Final Version in lock should be 0")
	}

}

func TestDeltaTableExists(t *testing.T) {
	table, state, tmpDir := setupTest(t)

	tableExists, err := table.Exists()
	if err != nil {
		t.Error(err)
	}

	if tableExists {
		t.Errorf("table should not exist")
	}
	metadata := NewDeltaTableMetaData("Test Table", "", new(Format).Default(), SchemaTypeStruct{}, []string{}, make(map[string]string))

	err = table.Create(*metadata, Protocol{}, make(map[string]any), []Add[testData, EmptyTestStruct]{})
	if err != nil {
		t.Error(err)
	}

	tableExists, err = table.Exists()
	if err != nil {
		t.Error(err)
	}

	if !tableExists {
		t.Errorf("table should exist")
	}
	// operation := Write{Mode: Overwrite}

	data, err := state.Get()
	if err != nil {
		t.Error(err)
	}
	if data.Version != 0 {
		t.Errorf("Final Version in lock should be 0")
	}

	// Create another version file
	transaction, operation, appMetaData := setupTransaction(t, table, nil)
	commit, err := transaction.PrepareCommit(operation, appMetaData)
	if err != nil {
		t.Error(err)
	}
	err = transaction.TryCommit(&commit)
	if err != nil {
		t.Error(err)
	}
	// Delete original version file
	commitPath := filepath.Join(tmpDir, table.CommitUriFromVersion(0).Raw)
	err = os.Remove(commitPath)
	if err != nil {
		t.Error(err)
	}

	// Exists() should still return true
	tableExists, err = table.Exists()
	if err != nil {
		t.Error(err)
	}

	if !tableExists {
		t.Errorf("table should exist")
	}

	// Move the new version file to a backup folder that starts with _delta_log
	commitPath = filepath.Join(tmpDir, table.CommitUriFromVersion(1).Raw)
	os.MkdirAll(filepath.Join(tmpDir, "_delta_log.bak"), 0700)
	fakeCommitPath := filepath.Join(tmpDir, "_delta_log.bak/00000000000000000000.json")
	err = os.Rename(commitPath, fakeCommitPath)
	if err != nil {
		t.Error(err)
	}

	// Exists() should return false
	tableExists, err = table.Exists()
	if err != nil {
		t.Error(err)
	}

	if tableExists {
		t.Errorf("table should not exist")
	}
}

func TestDeltaTableTryCommitLoop(t *testing.T) {
	table, _, _ := setupTest(t)

	transaction, operation, appMetaData := setupTransaction(t, table, &DeltaTransactionOptions{})
	commit, err := transaction.PrepareCommit(operation, appMetaData)
	if err != nil {
		t.Error(err)
	}

	err = transaction.TryCommit(&commit)
	if err != nil {
		t.Error(err)
	}

	newCommit, err := transaction.PrepareCommit(operation, appMetaData)
	if err != nil {
		t.Error(err)
	}

	err = transaction.TryCommitLoop(&newCommit)
	if err != nil {
		t.Error(err)
	}
}

func TestDeltaTableTryCommitLoopWithCommitExists(t *testing.T) {
	table, _, tmpDir := setupTest(t)
	table.Create(DeltaTableMetaData{}, Protocol{}, CommitInfo{}, []Add[testData, EmptyTestStruct]{})
	transaction, operation, appMetaData := setupTransaction(t, table, &DeltaTransactionOptions{MaxRetryCommitAttempts: 5, RetryWaitDuration: time.Second})
	commit, err := transaction.PrepareCommit(operation, appMetaData)
	if err != nil {
		t.Error(err)
	}
	//commit_001.json
	err = transaction.TryCommit(&commit)
	if err != nil {
		t.Error(err)
	}

	//Some other process writes commit 0002.json
	fakeCommit2 := filepath.Join(tmpDir, table.CommitUriFromVersion(2).Raw)
	os.WriteFile(fakeCommit2, []byte("temp commit data"), 0700)

	//Some other process writes commit 0003.json
	fakeCommit3 := filepath.Join(tmpDir, table.CommitUriFromVersion(3).Raw)
	os.WriteFile(fakeCommit3, []byte("temp commit data"), 0700)

	//create the next commit, should be 004.json after trying 003.json
	newCommit, err := transaction.PrepareCommit(operation, appMetaData)
	if err != nil {
		t.Error(err)
	}

	//Should exceed attempt retry count
	err = transaction.TryCommitLoop(&newCommit)
	if err != nil {
		t.Error(err)
	}

	if table.State.Version != 4 {
		t.Errorf("want table.State.Version=4, has %d", table.State.Version)
	}

	commitState, _ := table.StateStore.Get()
	if commitState.Version != 4 {
		t.Errorf("want table.State.Version=4, has %d", table.State.Version)
	}

	if !fileExists(filepath.Join(tmpDir, table.CommitUriFromVersion(4).Raw)) {
		t.Errorf("File %s should exist", table.CommitUriFromVersion(4).Raw)
	}

}

func TestCommitConcurrent(t *testing.T) {
	// log.SetLevel(log.DebugLevel)
	table, state, tmpDir := setupTest(t)
	metadata := NewDeltaTableMetaData("Test Table", "", new(Format).Default(), SchemaTypeStruct{}, []string{}, make(map[string]string))
	err := table.Create(*metadata, Protocol{}, CommitInfo{}, []Add[testData, EmptyTestStruct]{})
	if err != nil {
		t.Error(err)
	}

	numLocks := int32(0)
	numThreads := 100
	wg := new(sync.WaitGroup)
	errs := make(chan error, numThreads)
	for i := 0; i < numThreads; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			wait := rand.Int63n(int64(10 * time.Millisecond))
			time.Sleep(time.Duration(wait))

			store := filestore.New(storage.NewPath(tmpDir))
			state := filestate.New(storage.NewPath(tmpDir), "_delta_log/_commit.state")
			lock := filelock.New(storage.NewPath(tmpDir), "_delta_log/_commit.state", filelock.LockOptions{})

			//Lock needs to be instantiated for each worker because it is passed by reference, so if it is not created different instances of tables would share the same lock
			table := NewDeltaTable[testData, EmptyTestStruct](store, lock, state)
			transaction, operation, appMetaData := setupTransaction(t, table, NewDeltaTransactionOptions())
			_, err := transaction.Commit(operation, appMetaData)
			if err != nil {
				errs <- err
			} else {
				atomic.AddInt32(&numLocks, 1)
			}
		}()
	}
	wg.Wait()

	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if exp, got := 100, int(numLocks); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}

	commitState, err := state.Get()
	for err != nil {
		t.Error(err)
	}
	if commitState.Version != 100 {
		t.Errorf("Final Version in lock should be 100")
	}

	lastCommitFile := filepath.Join(tmpDir, table.CommitUriFromVersion(100).Raw)
	if !fileExists(lastCommitFile) {
		t.Errorf("File should exist")
	}
}

func TestCommitConcurrentWithParquet(t *testing.T) {
	table, state, tmpDir := setupTest(t)

	// First write
	fileName := fmt.Sprintf("part-%s.snappy.parquet", uuid.New().String())
	filePath := filepath.Join(tmpDir, fileName)

	//Make some data
	data := makeTestData(5)
	stats := makeTestDataStats(data)
	schema := data[0].getSchema()
	p, err := writeParquet(data, filePath)
	if err != nil {
		t.Error(err)
	}

	add := Add[testData, EmptyTestStruct]{
		Path:             fileName,
		Size:             DeltaDataTypeLong(p.Size),
		DataChange:       true,
		ModificationTime: DeltaDataTypeTimestamp(time.Now().UnixMilli()),
		Stats:            string(stats.Json()),
		PartitionValues:  make(map[string]string),
	}

	metadata := NewDeltaTableMetaData("Test Table", "test description", new(Format).Default(), schema, []string{}, make(map[string]string))
	err = table.Create(*metadata, Protocol{}, CommitInfo{}, []Add[testData, EmptyTestStruct]{add})
	if err != nil {
		t.Error(err)
	}

	numLocks := int32(0)
	numThreads := 100
	wg := new(sync.WaitGroup)
	errs := make(chan error, numThreads)
	for i := 0; i < numThreads; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			wait := rand.Int63n(int64(10 * time.Millisecond))
			time.Sleep(time.Duration(wait))

			store := filestore.New(storage.NewPath(tmpDir))
			state := filestate.New(storage.NewPath(tmpDir), "_delta_log/_commit.state")
			lock := filelock.New(storage.NewPath(tmpDir), "_delta_log/_commit.state", filelock.LockOptions{})

			//Lock needs to be instantiated for each worker because it is passed by reference, so if it is not created different instances of tables would share the same lock
			table := NewDeltaTable[testData, EmptyTestStruct](store, lock, state)
			transaction := table.CreateTransaction(NewDeltaTransactionOptions())

			//Make some data
			data := makeTestData(rand.Intn(50))
			stats := makeTestDataStats(data)
			fileName := fmt.Sprintf("part-%s.snappy.parquet", uuid.New().String())
			filePath := filepath.Join(tmpDir, fileName)
			p, err := writeParquet(data, filePath)
			if err != nil {
				t.Error(err)
			}

			add := Add[testData, EmptyTestStruct]{
				Path:             fileName,
				Size:             DeltaDataTypeLong(p.Size),
				DataChange:       true,
				ModificationTime: DeltaDataTypeTimestamp(time.Now().UnixMilli()),
				Stats:            string(stats.Json()),
				PartitionValues:  make(map[string]string),
			}

			transaction.AddAction(add)
			operation := Write{Mode: Overwrite}
			appMetaData := make(map[string]any)
			appMetaData["test"] = 123

			_, err = transaction.Commit(operation, appMetaData)
			if err != nil {
				errs <- err
			} else {
				atomic.AddInt32(&numLocks, 1)
			}
		}()
	}
	wg.Wait()

	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if exp, got := 100, int(numLocks); exp != got {
		t.Fatalf("expected %v, got %v", exp, got)
	}

	commitState, err := state.Get()
	for err != nil {
		t.Error(err)
	}
	if commitState.Version != 100 {
		t.Errorf("Final Version in lock should be 100")
	}

	lastCommitFile := filepath.Join(tmpDir, table.CommitUriFromVersion(100).Raw)
	if !fileExists(lastCommitFile) {
		t.Errorf("File should exist")
	}

}

func TestCreateWithParquet(t *testing.T) {
	table, _, tmpDir := setupTest(t)

	// First write
	fileName := fmt.Sprintf("part-%s.snappy.parquet", uuid.New().String())
	filePath := filepath.Join(tmpDir, fileName)

	//Make some data
	data := makeTestData(5)
	stats := makeTestDataStats(data)
	schema := data[0].getSchema()
	p, err := writeParquet(data, filePath)
	if err != nil {
		t.Error(err)
	}

	add := Add[testData, EmptyTestStruct]{
		Path:             fileName,
		Size:             DeltaDataTypeLong(p.Size),
		DataChange:       true,
		ModificationTime: DeltaDataTypeTimestamp(time.Now().UnixMilli()),
		Stats:            string(stats.Json()),
		PartitionValues:  make(map[string]string),
	}

	metadata := NewDeltaTableMetaData("Test Table", "test description", new(Format).Default(), schema, []string{}, make(map[string]string))
	err = table.Create(*metadata, Protocol{}, CommitInfo{}, []Add[testData, EmptyTestStruct]{add})
	if err != nil {
		t.Error(err)
	}
	//		`{"type":"struct","nullable":false,"fields":[{"name":"id","type":"integer","nullable":true},{"name":"label","type":"string","nullable":true},{"name":"value","type":"string","nullable":true}]}`
	//	    `{"type":"struct","fields":[{"name":"letter","type":"string","nullable":true,"metadata":{}},{"name":"number","type":"long","nullable":true,"metadata":{}},{"name":"a_float","type":"double","nullable":true,"metadata":{}}]}"`
}

func TestMakeCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()
	tmpPath := storage.NewPath(tmpDir)
	store := filestore.New(tmpPath)
	state := filestate.New(storage.NewPath(tmpDir), "_delta_log/_commit.state")
	lock := filelock.New(tmpPath, "_delta_log/_commit.state", filelock.LockOptions{})
	table := NewDeltaTable[testData, EmptyTestStruct](store, lock, state)
	var allTestData = make([]testData, 50)

	for i := 0; i < 10; i++ {
		fileName := fmt.Sprintf("part-%s.snappy.parquet", uuid.New().String())
		filePath := filepath.Join(tmpDir, fileName)

		//Make some data
		data := makeTestData(5)
		allTestData = append(allTestData, data...)
		stats := makeTestDataStats(data)
		schema := data[0].getSchema()
		p, err := writeParquet(data, filePath)
		if err != nil {
			t.Error(err)
		}

		add := Add[testData, EmptyTestStruct]{
			Path:             fileName,
			Size:             DeltaDataTypeLong(p.Size),
			DataChange:       true,
			ModificationTime: DeltaDataTypeTimestamp(time.Now().UnixMilli()),
			Stats:            string(stats.Json()),
			PartitionValues:  make(map[string]string),
		}

		if i == 0 {
			metadata := NewDeltaTableMetaData("Test Table", "test description", new(Format).Default(), schema, []string{}, make(map[string]string))
			err = table.Create(*metadata, Protocol{}, CommitInfo{}, []Add[testData, EmptyTestStruct]{add})
			if err != nil {
				t.Error(err)
			}
		} else {
			transaction := table.CreateTransaction(NewDeltaTransactionOptions())
			transaction.AddAction(add)
			operation := Write{Mode: Overwrite}
			_, err = transaction.Commit(operation, make(map[string]any))
			if err != nil {
				t.Error(err)
			}
		}
	}

	table, err := OpenTable[testData, EmptyTestStruct](store, lock, state)
	if err != nil {
		t.Fatal(err)
	}
	err = table.CreateCheckpoint()
	if err != nil {
		t.Error(err)
	}

	// load checkpoint and validate

}

type testData struct {
	Id     int64     `json:"id" parquet:"id,snappy"`
	T1     int64     `json:"t1" parquet:"t1,timestamp(microsecond)"`
	T2     time.Time `json:"t2" parquet:"t2,timestamp"`
	Label  string    `json:"label" parquet:"label,dict,snappy"`
	Value1 float64   `json:"value1" parquet:"value1,snappy" nullable:"false"`
	Value2 *float64  `json:"value2" parquet:"value2,snappy" nullable:"true"`
	Data   []byte    `json:"data" parquet:"data,plain,snappy" nullable:"true"`
}

func (this *testData) UnmarshalJSON(data []byte) error {
	var f interface{}
	err := json.Unmarshal(data, &f)
	if err != nil {
		return err
	}
	m := f.(map[string]interface{})
	for k, v := range m {
		switch k {
		case "id":
			this.Id = int64(v.(float64))
		case "t1":
			this.T1 = int64(v.(float64))
		case "t2":
			this.T2 = time.Unix(int64(v.(float64)), 0)
		case "label":
			this.Label = v.(string)
		case "value1":
			this.Value1 = v.(float64)
		case "value2":
			val := v.(float64)
			this.Value2 = &val
		case "data":
			this.Data = v.([]byte)
		}
	}
	return nil
}

func (data *testData) getSchema() SchemaTypeStruct {

	// schema := GetSchema(data)
	schema := SchemaTypeStruct{
		Fields: []SchemaField{
			{Name: "id", Type: Long, Nullable: false, Metadata: make(map[string]any)},
			{Name: "t1", Type: Timestamp, Nullable: false, Metadata: make(map[string]any)},
			{Name: "t2", Type: Timestamp, Nullable: false, Metadata: make(map[string]any)},
			{Name: "label", Type: String, Nullable: false, Metadata: make(map[string]any)},
			{Name: "value1", Type: Double, Nullable: false, Metadata: make(map[string]any)},
			{Name: "value2", Type: Double, Nullable: true, Metadata: make(map[string]any)},
			{Name: "data", Type: Binary, Nullable: true, Metadata: make(map[string]any)},
		},
	}
	return schema
}

func makeTestData(n int) []testData {
	id := rand.Int()
	var data []testData
	for i := 0; i < n; i++ {
		v := rand.Float64()
		var v2 *float64
		if rand.Float64() > 0.5 {
			v2 = &v
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(rand.Int()))
		row := testData{
			Id:     int64(id),
			T1:     time.Now().UnixMicro(),
			T2:     time.Now(),
			Label:  uuid.NewString(),
			Value1: v,
			Value2: v2,
			Data:   b,
		}
		data = append(data, row)
	}
	return data
}

func makeTestDataStats(data []testData) Stats {
	stats := Stats{}
	for _, row := range data {
		stats.NumRecords++
		UpdateStats(&stats, "id", &row.Id)
		UpdateStats(&stats, "t1", &row.T1)
		i := row.T2.UnixMicro()
		UpdateStats(&stats, "t2", &i)
		UpdateStats(&stats, "label", &row.Label)
		UpdateStats(&stats, "value1", &row.Value1)
		UpdateStats(&stats, "value2", row.Value2)
	}
	return stats
}

type payload struct {
	File *os.File
	Size int64
}

func writeParquet[T any](data []T, filename string) (*payload, error) {

	p := new(payload)

	// if err := parquet.WriteFile(filename, data); err != nil {
	// 	return p, err
	// }

	file, err := os.Create(filename)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	writer := parquet.NewGenericWriter[T](file)

	i, err := writer.Write(data)
	println(i)
	if err != nil {
		fmt.Println(err)
	}
	if err := writer.Close(); err != nil {
		fmt.Println(err)
	}
	info, _ := file.Stat()
	p.Size = info.Size()
	p.File = file
	return p, nil
}

// / Helper function to set up test state
func setupTest(t *testing.T) (table *DeltaTable[testData, EmptyTestStruct], state *filestate.FileStateStore, tmpDir string) {
	t.Helper()

	tmpDir = t.TempDir()
	tmpPath := storage.NewPath(tmpDir)
	store := filestore.New(tmpPath)
	state = filestate.New(storage.NewPath(tmpDir), "_delta_log/_commit.state")
	lock := filelock.New(tmpPath, "_delta_log/_commit.state", filelock.LockOptions{})
	table = NewDeltaTable[testData, EmptyTestStruct](store, lock, state)
	return
}

// / Helper function to set up a basic transaction
func setupTransaction(t *testing.T, table *DeltaTable[testData, EmptyTestStruct], options *DeltaTransactionOptions) (transaction *DeltaTransaction[testData, EmptyTestStruct], operation DeltaOperation, appMetaData map[string]any) {
	t.Helper()

	transaction = table.CreateTransaction(options)
	add := Add[testData, EmptyTestStruct]{
		Path:             "part-00000-80a9bb40-ec43-43b6-bb8a-fc66ef7cd768-c000.snappy.parquet",
		Size:             984,
		ModificationTime: DeltaDataTypeTimestamp(time.Now().UnixMilli()),
	}
	transaction.AddAction(add)
	operation = Write{Mode: Overwrite}
	appMetaData = make(map[string]any)
	appMetaData["test"] = 123
	return
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
