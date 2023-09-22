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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/chelseajonesr/rfarrow"
	"github.com/google/uuid"
	"github.com/rivian/delta-go/internal/s3utils"
	"github.com/rivian/delta-go/lock"
	"github.com/rivian/delta-go/lock/filelock"
	"github.com/rivian/delta-go/lock/nillock"
	"github.com/rivian/delta-go/state/filestate"
	"github.com/rivian/delta-go/state/localstate"

	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/filestore"
	"github.com/rivian/delta-go/storage/s3store"
)

func TestDeltaTransactionPrepareCommit(t *testing.T) {
	store := filestore.FileObjectStore{BaseURI: storage.Path{Raw: "tmp/"}}
	l := filelock.New(storage.NewPath(""), "tmp/_delta_log/_commit.lock", filelock.Options{})
	deltaTable := DeltaTable{Store: &store, LockClient: l}
	options := DeltaTransactionOptions{MaxRetryCommitAttempts: 3}
	os.MkdirAll("tmp/_delta_log/", 0700)
	defer os.RemoveAll("tmp/_delta_log/")
	transaction := NewDeltaTransaction(&deltaTable, &options)
	add := Add{
		Path:             "part-00000-80a9bb40-ec43-43b6-bb8a-fc66ef7cd768-c000.snappy.parquet",
		Size:             984,
		ModificationTime: time.Now().UnixMilli(),
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
	if !strings.Contains(string(b), add.Path) {
		t.Errorf("File should contain add part file")
	}
}

func TestDeltaTableReadCommitVersion(t *testing.T) {
	table, _, _ := setupTest(t)
	table.Create(DeltaTableMetaData{}, Protocol{}, CommitInfo{}, []Add{})
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
	config[string(AppendOnlyDeltaConfigKey)] = "true"
	metadata := NewDeltaTableMetaData("Test Table", "", format, schema, []string{}, config)
	protocol := new(Protocol).Default()
	stats := Stats{NumRecords: 1, MinValues: map[string]any{"first_column": 1}}
	path := "part-123.snappy.parquet"
	add := Add{
		Path:             path,
		Size:             984,
		ModificationTime: time.Now().UnixMilli(),
		Stats:            string(stats.Json()),
	}
	commitInfo := make(map[string]any)
	commitInfo["test"] = 123
	err := table.Create(*metadata, protocol, commitInfo, []Add{add})
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
	a, ok := actions[3].(*Add)
	if !ok {
		t.Error("Expected Add for fourth action")
	}
	if a.Path != path {
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
	table.Create(DeltaTableMetaData{}, Protocol{}, CommitInfo{}, []Add{})
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
	if !errors.Is(err, storage.ErrObjectDoesNotExist) {
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
	w0lockClient := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{TTL: 10 * time.Second})
	w0lockClient.TryLock()
	//try_commit while lock is held by w0
	lockClient := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{TTL: 60 * time.Second})
	// lockClient.Unlock()

	store := filestore.New(tmpPath)
	state := filestate.New(tmpPath, "_delta_log/_commit.state")

	table := NewDeltaTable(store, lockClient, state)

	transaction, operation, appMetaData := setupTransaction(t, table, &DeltaTransactionOptions{MaxRetryCommitAttempts: 3})
	version, err := transaction.Commit(operation, appMetaData)
	if !errors.Is(err, ErrExceededCommitRetryAttempts) {
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
		t.Errorf("Version is %d, final Version in lock should be 1", commitData.Version)
	}
}

type testBrokenUnlockLocker struct {
	filelock.FileLock
}

func (l *testBrokenUnlockLocker) Unlock() error {
	return lock.ErrUnableToUnlock
}

func TestCommitUnlockFailure(t *testing.T) {
	tmpDir := t.TempDir()
	fileLockKey := filepath.Join(tmpDir, "_delta_log/_commit.lock")
	os.MkdirAll(filepath.Dir(fileLockKey), 0700)

	tmpPath := storage.NewPath(tmpDir)

	lockClient := testBrokenUnlockLocker{*filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{TTL: 60 * time.Second})}

	store := filestore.New(tmpPath)
	state := filestate.New(tmpPath, "_delta_log/_commit.state")

	table := NewDeltaTable(store, &lockClient, state)

	transaction, operation, appMetaData := setupTransaction(t, table, &DeltaTransactionOptions{MaxRetryCommitAttempts: 3})
	_, err := transaction.Commit(operation, appMetaData)
	if !errors.Is(err, lock.ErrUnableToUnlock) {
		t.Error(err)
	}
}

// / Test using local state and empty lock; this scenario is for local scripts and testing
func TestTryCommitWithNilLockAndLocalState(t *testing.T) {
	tmpDir := t.TempDir()
	tmpPath := storage.NewPath(tmpDir)
	store := filestore.New(tmpPath)
	storeState := localstate.New(-1)
	lock := nillock.New()
	table := NewDeltaTable(store, lock, storeState)

	table.Create(DeltaTableMetaData{}, Protocol{}, CommitInfo{}, []Add{})
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
		t.Errorf("want version %d, has version = %d", 1, transaction.DeltaTable.State.Version)
	}

	commit, _ = transaction.PrepareCommit(operation, appMetaData)
	err = transaction.TryCommit(&commit)
	if err != nil {
		t.Error(err)
	}
	if transaction.DeltaTable.State.Version != 2 {
		t.Errorf("want version %d, has version = %d", 2, transaction.DeltaTable.State.Version)
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
	config[string(AppendOnlyDeltaConfigKey)] = "true"
	metadata := NewDeltaTableMetaData("Test Table", "", format, schema, []string{}, config)
	protocol := new(Protocol).Default()
	add := Add{
		Path:             "part-00000-80a9bb40-ec43-43b6-bb8a-fc66ef7cd768-c000.snappy.parquet",
		Size:             984,
		ModificationTime: time.Now().UnixMilli(),
	}
	commitInfo := make(map[string]any)
	commitInfo["test"] = 123
	err := table.Create(*metadata, protocol, commitInfo, []Add{add})
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

	err = table.Create(*metadata, Protocol{}, make(map[string]any), []Add{})
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
	commitPath := filepath.Join(tmpDir, CommitUriFromVersion(0).Raw)
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
	commitPath = filepath.Join(tmpDir, CommitUriFromVersion(1).Raw)
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

	// Test a checkpoint file
	err = os.Rename(fakeCommitPath, filepath.Join(tmpDir, "_delta_log/00000000000000031000.checkpoint.0000000001.0000000003.parquet"))
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
}

// Do a test with a lot of random files in the folder
func TestDeltaTableExistsManyTempFiles(t *testing.T) {
	// Need pagination so use S3 mock client
	baseURI := storage.NewPath("s3://test-bucket/test-delta-table")
	mockClient, err := s3utils.NewMockClient(t, baseURI)
	if err != nil {
		t.Fatalf("Error occurred setting up for tests %e.", err)
	}

	mockClient.PaginateListResults = true

	s3Store, err := s3store.New(mockClient, baseURI)
	if err != nil {
		t.Fatalf("Error occurred setting up for tests %e.", err)
	}

	tmpDir := os.TempDir()
	tmpPath := storage.NewPath(tmpDir)
	state := filestate.New(tmpPath, "_delta_log/_commit.state")
	lock := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{})
	table := NewDeltaTable(s3Store, lock, state)

	metadata := NewDeltaTableMetaData("Test Table", "", new(Format).Default(), SchemaTypeStruct{}, []string{}, make(map[string]string))

	err = table.Create(*metadata, Protocol{}, make(map[string]any), []Add{})
	if err != nil {
		t.Error(err)
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
	err = s3Store.Delete(CommitUriFromVersion(0))
	if err != nil {
		t.Error(err)
	}

	tableExists, err := table.Exists()
	if err != nil {
		t.Error(err)
	}

	if !tableExists {
		t.Errorf("table should exist")
	}

	for i := 0; i < 1100; i++ {
		tempFilePath := fmt.Sprintf("_delta_log/.fake_optimize_%d", i)
		table.Store.Put(storage.NewPath(tempFilePath), []byte{})
	}

	tableExists, err = table.Exists()
	if err != nil {
		t.Error(err)
	}

	if !tableExists {
		t.Errorf("table should exist")
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
	table.Create(DeltaTableMetaData{}, Protocol{}, CommitInfo{}, []Add{})
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
	fakeCommit2 := filepath.Join(tmpDir, CommitUriFromVersion(2).Raw)
	os.WriteFile(fakeCommit2, []byte("temp commit data"), 0700)

	//Some other process writes commit 0003.json
	fakeCommit3 := filepath.Join(tmpDir, CommitUriFromVersion(3).Raw)
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

	if !fileExists(filepath.Join(tmpDir, CommitUriFromVersion(4).Raw)) {
		t.Errorf("File %s should exist", CommitUriFromVersion(4).Raw)
	}

}

func TestDeltaTableTryCommitLoopStateStoreOutOfSync(t *testing.T) {
	table, _, tmpDir := setupTest(t)
	table.Create(DeltaTableMetaData{}, Protocol{}, CommitInfo{}, []Add{})
	transaction, operation, appMetaData := setupTransaction(t, table, &DeltaTransactionOptions{
		MaxRetryCommitAttempts:                5, //Should break on max attempts if the latest version logic fails
		RetryWaitDuration:                     time.Second,
		RetryCommitAttemptsBeforeLoadingTable: 2, //Should get the latest version after 2 attempts
	})

	for i := 1; i < 10; i++ {
		commit, err := transaction.PrepareCommit(operation, appMetaData)
		if err != nil {
			t.Error(err)
		}
		//commit_001.json
		err = transaction.TryCommit(&commit)
		if err != nil {
			t.Error(err)
		}
	}

	// Write 0 to state
	commitState, _ := table.StateStore.Get()
	commitState.Version = 0
	table.StateStore.Put(commitState)
	table.State.Version = 0

	//create the next commit, should be 010.json after finding that 000.json exists and loading the tabel state
	newCommit, err := transaction.PrepareCommit(operation, appMetaData)
	if err != nil {
		t.Error(err)
	}

	//Should exceed attempt retry count
	err = transaction.TryCommitLoop(&newCommit)
	if err != nil {
		t.Error(err)
	}

	if table.State.Version != 10 {
		t.Errorf("want table.State.Version=4, has %d", table.State.Version)
	}

	commitState, _ = table.StateStore.Get()
	if commitState.Version != 10 {
		t.Errorf("want table.State.Version=4, has %d", table.State.Version)
	}

	if !fileExists(filepath.Join(tmpDir, CommitUriFromVersion(10).Raw)) {
		t.Errorf("File %s should exist", CommitUriFromVersion(10).Raw)
	}

}

func TestCommitConcurrent(t *testing.T) {
	// log.SetLevel(log.DebugLevel)
	table, state, tmpDir := setupTest(t)
	metadata := NewDeltaTableMetaData("Test Table", "", new(Format).Default(), SchemaTypeStruct{}, []string{}, make(map[string]string))
	err := table.Create(*metadata, Protocol{}, CommitInfo{}, []Add{})
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
			lock := filelock.New(storage.NewPath(tmpDir), "_delta_log/_commit.lock", filelock.Options{})

			//Lock needs to be instantiated for each worker because it is passed by reference, so if it is not created different instances of tables would share the same lock
			table := NewDeltaTable(store, lock, state)
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

	lastCommitFile := filepath.Join(tmpDir, CommitUriFromVersion(100).Raw)
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

	add := Add{
		Path:             fileName,
		Size:             p.Size,
		DataChange:       true,
		ModificationTime: time.Now().UnixMilli(),
		Stats:            string(stats.Json()),
		PartitionValues:  make(map[string]string),
	}

	metadata := NewDeltaTableMetaData("Test Table", "test description", new(Format).Default(), schema, []string{}, make(map[string]string))
	err = table.Create(*metadata, new(Protocol).Default(), CommitInfo{}, []Add{add})
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
			lock := filelock.New(storage.NewPath(tmpDir), "_delta_log/_commit.lock", filelock.Options{})

			//Lock needs to be instantiated for each worker because it is passed by reference, so if it is not created different instances of tables would share the same lock
			table := NewDeltaTable(store, lock, state)
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

			add := Add{
				Path:             fileName,
				Size:             p.Size,
				DataChange:       true,
				ModificationTime: time.Now().UnixMilli(),
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

	lastCommitFile := filepath.Join(tmpDir, CommitUriFromVersion(100).Raw)
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

	add := Add{
		Path:             fileName,
		Size:             p.Size,
		DataChange:       true,
		ModificationTime: time.Now().UnixMilli(),
		Stats:            string(stats.Json()),
		PartitionValues:  make(map[string]string),
	}

	metadata := NewDeltaTableMetaData("Test Table", "test description", new(Format).Default(), schema, []string{}, make(map[string]string))
	err = table.Create(*metadata, new(Protocol).Default(), CommitInfo{}, []Add{add})
	if err != nil {
		t.Error(err)
	}

	//		`{"type":"struct","nullable":false,"fields":[{"name":"id","type":"integer","nullable":true},{"name":"label","type":"string","nullable":true},{"name":"value","type":"string","nullable":true}]}`
	//	    `{"type":"struct","fields":[{"name":"letter","type":"string","nullable":true,"metadata":{}},{"name":"number","type":"long","nullable":true,"metadata":{}},{"name":"a_float","type":"double","nullable":true,"metadata":{}}]}"`
}

type testData struct {
	Id        int64    `json:"id" parquet:"name=id"`
	Timestamp int64    `json:"timestamp" parquet:"name=timestamp, converted=timestamp_micros"`
	Label     string   `json:"label" parquet:"name=label, converted=UTF8"`
	Value1    float64  `json:"value1" parquet:"name=value1"`
	Value2    *float64 `json:"value2" parquet:"name=value2"`
	Data      []byte   `json:"data" parquet:"name=data"`
}

func (t *testData) UnmarshalJSON(data []byte) error {
	var f interface{}
	err := json.Unmarshal(data, &f)
	if err != nil {
		return err
	}
	m := f.(map[string]interface{})
	for k, v := range m {
		switch k {
		case "id":
			t.Id = int64(v.(float64))
		case "timestamp":
			t.Timestamp = int64(v.(float64))
		case "label":
			t.Label = v.(string)
		case "value1":
			t.Value1 = v.(float64)
		case "value2":
			val := v.(float64)
			t.Value2 = &val
		case "data":
			t.Data = v.([]byte)
		}
	}
	return nil
}

func (data *testData) getSchema() SchemaTypeStruct {
	// schema := GetSchema(data)
	schema := SchemaTypeStruct{
		Fields: []SchemaField{
			{Name: "id", Type: Long, Nullable: false, Metadata: make(map[string]any)},
			{Name: "timestamp", Type: Timestamp, Nullable: false, Metadata: make(map[string]any)},
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
			Id:        int64(id),
			Timestamp: time.Now().UnixMicro(),
			Label:     uuid.NewString(),
			Value1:    v,
			Value2:    v2,
			Data:      b,
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
		UpdateStats(&stats, "timestamp", &row.Timestamp)
		// i := row.T2.UnixMicro()
		// UpdateStats(&stats, "t2", &i)
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

	file, err := os.Create(filename)
	if err != nil {
		fmt.Println(err)
	}
	buf := new(bytes.Buffer)
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	err = rfarrow.WriteGoStructsToParquet(data, buf, props)
	if err != nil {
		return p, err
	}
	i, err := file.Write(buf.Bytes())
	println(i)
	if err != nil {
		return p, err
	}

	info, _ := file.Stat()
	p.Size = info.Size()
	p.File = file

	if err := file.Close(); err != nil {
		return p, err
	}
	return p, nil
}

// / Helper function to set up test state
func setupTest(t *testing.T) (table *DeltaTable, state *filestate.FileStateStore, tmpDir string) {
	t.Helper()

	tmpDir = t.TempDir()
	tmpPath := storage.NewPath(tmpDir)
	store := filestore.New(tmpPath)
	state = filestate.New(tmpPath, "_delta_log/_commit.state")
	lock := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{})
	table = NewDeltaTable(store, lock, state)
	return
}

// / Helper function to set up a basic transaction
func setupTransaction(t *testing.T, table *DeltaTable, options *DeltaTransactionOptions) (transaction *DeltaTransaction, operation DeltaOperation, appMetaData map[string]any) {
	t.Helper()

	transaction = table.CreateTransaction(options)
	add := Add{
		Path:             "part-00000-80a9bb40-ec43-43b6-bb8a-fc66ef7cd768-c000.snappy.parquet",
		Size:             984,
		ModificationTime: time.Now().UnixMilli(),
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

func TestCommitUriFromVersion(t *testing.T) {
	type test struct {
		input int64
		want  string
	}

	tests := []test{
		{input: 0, want: "_delta_log/00000000000000000000.json"},
		{input: 1, want: "_delta_log/00000000000000000001.json"},
		{input: 10, want: "_delta_log/00000000000000000010.json"},
		{input: 999999, want: "_delta_log/00000000000000999999.json"},
		{input: 1234567890123456789, want: "_delta_log/01234567890123456789.json"},
	}

	for _, tc := range tests {
		got := CommitUriFromVersion(tc.input)
		if got.Raw != tc.want {
			t.Errorf("expected %s, got %s", tc.want, got)
		}
	}
}

func TestIsValidCommitUri(t *testing.T) {
	type test struct {
		input string
		want  bool
	}

	tests := []test{
		{input: "_delta_log/00000000000000000000.json", want: true},
		{input: "_delta_log/00000000000000000001.json", want: true},
		{input: "_delta_log/01234567890123456789.json", want: true},
		{input: "_delta_log/00000000000000000001.checkpoint.parquet", want: false},
		{input: "_delta_log/_commit_aabbccdd-eeff-1122-3344-556677889900.json.tmp", want: false},
	}

	for _, tc := range tests {
		got := IsValidCommitUri(storage.NewPath(tc.input))
		if got != tc.want {
			t.Errorf("expected %t, got %t", tc.want, got)
		}
	}
}

func TestCommitVersionFromUri(t *testing.T) {
	type test struct {
		input       string
		wantMatch   bool
		wantVersion int64
	}

	tests := []test{
		{input: "_delta_log/00000000000000000000.json", wantMatch: true, wantVersion: 0},
		{input: "_delta_log/00000000000000000001.json", wantMatch: true, wantVersion: 1},
		{input: "_delta_log/01234567890123456789.json", wantMatch: true, wantVersion: 1234567890123456789},
		{input: "_delta_log/00000000000000000001.checkpoint.parquet", wantMatch: false},
		{input: "_delta_log/_commit_aabbccdd-eeff-1122-3344-556677889900.json.tmp", wantMatch: false},
	}

	for _, tc := range tests {
		match, got := CommitVersionFromUri(storage.NewPath(tc.input))
		if match != tc.wantMatch {
			t.Errorf("expected %t, got %t", tc.wantMatch, match)
		}
		if match == tc.wantMatch && match {
			if got != tc.wantVersion {
				t.Errorf("expected %d, got %d", tc.wantVersion, got)
			}
		}
	}
}

func TestCommitOrCheckpointVersionFromUri(t *testing.T) {
	type test struct {
		input       string
		wantMatch   bool
		wantVersion int64
	}

	tests := []test{
		{input: "_delta_log/00000000000000000000.json", wantMatch: true, wantVersion: 0},
		{input: "_delta_log/00000000000000000001.json", wantMatch: true, wantVersion: 1},
		{input: "_delta_log/01234567890123456789.json", wantMatch: true, wantVersion: 1234567890123456789},
		{input: "_delta_log/00000000000000000001.checkpoint.parquet", wantMatch: true, wantVersion: 1},
		{input: "_delta_log/00000000000000094451.checkpoint.0000000002.0000000061.parquet", wantMatch: true, wantVersion: 94451},
		{input: "_delta_log/_commit_aabbccdd-eeff-1122-3344-556677889900.json.tmp", wantMatch: false},
	}

	for _, tc := range tests {
		match, got := CommitOrCheckpointVersionFromUri(storage.NewPath(tc.input))
		if match != tc.wantMatch {
			t.Errorf("expected %t, got %t", tc.wantMatch, match)
		}
		if match == tc.wantMatch && match {
			if got != tc.wantVersion {
				t.Errorf("expected %d, got %d", tc.wantVersion, got)
			}
		}
	}
}

func TestLatestVersion(t *testing.T) {
	uri := storage.NewPath("s3://test-bucket/test-delta-table")

	client, err := s3utils.NewMockClient(t, uri)
	if err != nil {
		t.Fatalf("Failed to create new S3 mock client: %v", err)
	}
	store, err := s3store.New(client, uri)
	if err != nil {
		t.Fatalf("Failed to create new S3 object store: %v", err)
	}

	var (
		dir      = os.TempDir()
		path     = storage.NewPath(dir)
		state    = filestate.New(path, "_delta_log/_commit.state")
		lock     = filelock.New(path, "_delta_log/_commit.lock", filelock.Options{})
		table    = NewDeltaTable(store, lock, state)
		metadata = NewDeltaTableMetaData("test", "", new(Format).Default(), SchemaTypeStruct{}, nil, make(map[string]string))
	)

	if _, err := table.LatestVersion(); !errors.Is(err, ErrNotATable) { // if NO error
		t.Errorf("Expected: %v", ErrNotATable)
	}

	for i := 0; i < 2000; i++ {
		fileName := fmt.Sprintf(")_%s.json", uuid.New().String())
		filePath := storage.PathFromIter([]string{"_delta_log", fileName})

		table.Store.Put(filePath, nil)
	}

	if _, err := table.LatestVersion(); !errors.Is(err, ErrNotATable) { // if NO error
		t.Errorf("Expected: %v", ErrNotATable)
	}

	if err := table.Create(*metadata, Protocol{}, make(map[string]any), nil); err != nil {
		t.Errorf("Failed to create table: %v", err)
	}

	version, err := table.LatestVersion()
	if err != nil {
		t.Errorf("Failed to get latest version: %v", err)
	}
	if version != 0 {
		t.Errorf("After adding first commit: LatestVersion() = %v, want %v", version, 0)
	}

	transaction, operation, appMetadata := setupTransaction(t, table, nil)
	commit, err := transaction.PrepareCommit(operation, appMetadata)
	if err != nil {
		t.Errorf("Failed to prepare commit: %v", err)
	}
	if err := transaction.TryCommit(&commit); err != nil {
		t.Errorf("Failed to try commit: %v", err)
	}

	version, err = table.LatestVersion()
	if err != nil {
		t.Errorf("Failed to get latest version: %v", err)
	}
	if version != 1 {
		t.Errorf("After adding second commit: LatestVersion() = %v, want %v", version, 1)
	}

	for i := 0; i < 2100; i++ {
		fileName := fmt.Sprintf("_commit_%s.json.tmp", uuid.New().String())
		filePath := storage.PathFromIter([]string{"_delta_log", fileName})

		table.Store.Put(filePath, nil)
	}

	version, err = table.LatestVersion()
	if err != nil {
		t.Errorf("Failed to get latest version: %v", err)
	}
	if version != 1 {
		t.Errorf("After adding temp commits: LatestVersion() = %v, want %v", version, 1)
	}

	for version := 2; version < 2100; version++ {
		filePath := CommitUriFromVersion(int64(version))

		table.Store.Put(filePath, nil)
	}

	version, err = table.LatestVersion()
	if err != nil {
		t.Errorf("Failed to get latest version: %v", err)
	}
	if version != 2099 {
		t.Errorf("After adding many commits: LatestVersion() = %v, want %v", version, 2099)
	}

	for version := 1000; version < 2100; version = version + 10 {
		fileName := fmt.Sprintf("%020d", version) + ".checkpoint.parquet"
		filePath := storage.PathFromIter([]string{"_delta_log", fileName})

		table.Store.Put(filePath, nil)
	}

	version, err = table.LatestVersion()
	if err != nil {
		t.Errorf("Failed to get latest version: %v", err)
	}
	if version != 2099 {
		t.Errorf("After adding checkpoints: LatestVersion() = %v, want %v", version, 2099)
	}

	for version := 0; version < 1000; version++ {
		filePath := CommitUriFromVersion(int64(version))

		table.Store.Delete(filePath)
	}

	version, err = table.LatestVersion()
	if err != nil {
		t.Errorf("Failed to get latest version: %v", err)
	}
	if version != 2099 {
		t.Errorf("After deleting many commits: LatestVersion() = %v, want %v", version, 2099)
	}

	for version := 2100; version < 4100; version++ {
		filePath := CommitUriFromVersion(int64(version))

		table.Store.Put(filePath, nil)
	}

	checkpoint := CheckPoint{
		Version: 3000,
	}
	bytes, err := json.Marshal(checkpoint)
	if err != nil {
		t.Errorf("Failed to marshal checkpoint: %v", err)
	}
	if err := store.Put(lastCheckpointPath(), bytes); err != nil {
		t.Errorf("Failed to put checkpoint bytes: %v", err)
	}

	version, err = table.LatestVersion()
	if err != nil {
		t.Errorf("Failed to get latest version: %v", err)
	}
	if version != 4099 {
		t.Errorf("After adding last checkpoint file: LatestVersion() = %v, want %v", version, 4099)
	}
}

func BenchmarkLatestVersion(b *testing.B) {
	uri := storage.NewPath("s3://test-bucket/test-delta-table")

	var (
		dir       = b.TempDir()
		path      = storage.NewPath(dir)
		fileStore = filestore.FileObjectStore{BaseURI: path}
		client    = new(s3utils.MockS3Client)
	)

	client.SetFileStore(fileStore)

	baseURL, err := uri.ParseURL()
	if err != nil {
		b.Fatalf("Failed to parse URL: %v", err)
	}

	if strings.HasSuffix(baseURL.Path, "/") {
		client.SetS3StorePath(baseURL.Path)
	} else {
		client.SetS3StorePath(baseURL.Path + "/")
	}

	s3Store, err := s3store.New(client, uri)
	if err != nil {
		b.Fatalf("Failed to create new S3 object store: %v", err)
	}

	var (
		state = filestate.New(path, "_delta_log/_commit.state")
		lock  = filelock.New(path, "_delta_log/_commit.lock", filelock.Options{})
		table = NewDeltaTable(s3Store, lock, state)
	)

	for version := 0; version < 1000; version++ {
		filePath := CommitUriFromVersion(int64(version))

		table.Store.Put(filePath, nil)
	}

	table.LatestVersion()
}

func TestLoadVersion(t *testing.T) {
	// Use setupCheckpointTest() to copy testdata commits
	store, stateStore, lock, _ := setupCheckpointTest(t, "testdata/checkpoints/simple")

	// Load version 2
	table := NewDeltaTable(store, lock, stateStore)
	var version int64 = 2
	err := table.LoadVersion(&version)
	if err != nil {
		t.Error(err)
	}
	// Check contents
	// Set up the expected state based on the commits we are reading
	expectedState := NewTableState(version)
	operationParams := make(map[string]interface{}, 4)
	operationParams["isManaged"] = "false"
	operationParams["description"] = nil
	operationParams["partitionBy"] = "[\"date\"]"
	operationParams["properties"] = "{}"
	expectedState.CommitInfos = append(expectedState.CommitInfos, CommitInfo{"timestamp": float64(1627668675695), "operation": "CREATE TABLE", "operationParameters": operationParams, "isolationLevel": "SnapshotIsolation", "isBlindAppend": true, "operationMetrics": make(map[string]interface{})})
	operationParams = make(map[string]interface{}, 2)
	operationParams["mode"] = "Append"
	operationParams["partitionBy"] = "[]"
	operationMetrics := make(map[string]interface{}, 3)
	operationMetrics["numFiles"] = "1"
	operationMetrics["numOutputBytes"] = "1502"
	operationMetrics["numOutputRows"] = "1"
	expectedState.CommitInfos = append(expectedState.CommitInfos, CommitInfo{"timestamp": float64(1627668685528), "operation": "WRITE", "operationParameters": operationParams, "readVersion": float64(0), "isolationLevel": "WriteSerializable", "isBlindAppend": true, "operationMetrics": operationMetrics})
	expectedState.CommitInfos = append(expectedState.CommitInfos, CommitInfo{"timestamp": float64(1627668687609), "operation": "WRITE", "operationParameters": operationParams, "readVersion": float64(1), "isolationLevel": "WriteSerializable", "isBlindAppend": true, "operationMetrics": operationMetrics})
	expectedState.MinReaderVersion = 1
	expectedState.MinWriterVersion = 1
	add := new(Add)
	path := "date=2020-06-01/part-00000-b207ef5f-4458-4969-bd34-46439cdeb6a6.c000.snappy.parquet"
	add.Path = path
	partitionValues := make(map[string]string)
	partitionValues["date"] = "2020-06-01"
	add.PartitionValues = partitionValues
	size := int64(1502)
	add.Size = size
	modificationTime := int64(1627668686000)
	add.ModificationTime = modificationTime
	dataChange := true
	add.DataChange = dataChange
	stats := "{\"numRecords\":1,\"minValues\":{\"value\":\"x\",\"ts\":\"2021-07-30T18:11:24.594Z\"},\"maxValues\":{\"value\":\"x\",\"ts\":\"2021-07-30T18:11:24.594Z\"},\"nullCount\":{\"value\":0,\"ts\":0}}"
	add.Stats = stats
	expectedState.Files[add.Path] = *add
	add = new(Add)
	path2 := "date=2020-06-01/part-00000-762e2b03-6a04-4707-b676-5d38d1ef9fca.c000.snappy.parquet"
	add.Path = path2
	add.PartitionValues = partitionValues
	add.Size = size
	modificationTime2 := int64(1627668688000)
	add.ModificationTime = modificationTime2
	add.DataChange = dataChange
	stats2 := "{\"numRecords\":1,\"minValues\":{\"value\":\"x\",\"ts\":\"2021-07-30T18:11:27.001Z\"},\"maxValues\":{\"value\":\"x\",\"ts\":\"2021-07-30T18:11:27.001Z\"},\"nullCount\":{\"value\":0,\"ts\":0}}"
	add.Stats = stats2
	expectedState.Files[add.Path] = *add
	var schema SchemaTypeStruct = SchemaTypeStruct{
		Fields: []SchemaField{
			{Name: "value", Type: String, Nullable: true, Metadata: make(map[string]any)},
			{Name: "ts", Type: Timestamp, Nullable: true, Metadata: make(map[string]any)},
			{Name: "date", Type: String, Nullable: true, Metadata: make(map[string]any)},
		},
	}
	provider := "parquet"
	options := map[string]string{}
	expectedState.CurrentMetadata = NewDeltaTableMetaData("", "", Format{Provider: provider, Options: options}, schema, []string{"date"}, make(map[string]string))
	expectedState.CurrentMetadata.CreatedTime = time.Unix(1627668675, 432000000)
	expectedState.CurrentMetadata.Id = uuid.MustParse("853536c9-0abe-4e66-9732-1718e542e6aa")

	if !reflect.DeepEqual(*expectedState, table.State) {
		t.Errorf("table state expected %v, found %v", *expectedState, table.State)
	}

	// Invalid version should return error
	version = 20
	err = table.LoadVersion(&version)
	if !errors.Is(err, ErrInvalidVersion) {
		t.Error("loading an invalid version did not return correct error")
	}

	// Calling with no version should load latest version
	err = table.LoadVersion(nil)
	if err != nil {
		t.Error(err)
	}
	if table.State.Version != 12 {
		t.Errorf("expected version %d, found %d", 12, table.State.Version)
	}
}
