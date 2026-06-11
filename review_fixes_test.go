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

// Tests for the fixes from the 2026-06-11 review (see REVIEW_2026-06-11.md)

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/rivian/delta-go/internal/dynamodbutils"
	"github.com/rivian/delta-go/logstore"
	"github.com/rivian/delta-go/logstore/dynamodblogstore"
	"github.com/rivian/delta-go/storage"
)

// headErrorStore wraps an ObjectStore, failing Head calls with the given error after
// allowCount successful calls.
type headErrorStore struct {
	storage.ObjectStore
	headErr    error
	allowCount int
	headCalls  int
}

func (s *headErrorStore) Head(location storage.Path) (storage.ObjectMeta, error) {
	s.headCalls++
	if s.headErr != nil && s.headCalls > s.allowCount {
		return storage.ObjectMeta{}, s.headErr
	}
	return s.ObjectStore.Head(location)
}

// F5: LatestVersion must propagate transient (non-404) Head errors instead of looping
// forever or silently returning a version earlier than the table's true latest version.
func TestLatestVersionPropagatesHeadErrors(t *testing.T) {
	table, _, tmpDir := setupTest(t)

	fileName := fmt.Sprintf("part-%s.snappy.parquet", uuid.NewString())
	filePath := filepath.Join(tmpDir, fileName)
	if _, err := writeParquet(makeTestData(5), filePath); err != nil {
		t.Fatal(err)
	}
	add, _, err := NewAdd(table.Store, storage.NewPath(fileName), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Create(*NewTableMetaData("test", "", new(Format).Default(), SchemaTypeStruct{}, nil, make(map[string]string)), new(Protocol).Default(), make(map[string]any), []Add{*add}); err != nil {
		t.Fatal(err)
	}

	// Sanity check before injecting errors
	version, err := table.LatestVersion()
	if err != nil {
		t.Fatalf("LatestVersion failed: %v", err)
	}
	if version != 0 {
		t.Fatalf("expected version 0, got %d", version)
	}

	throttleErr := errors.New("simulated throttling error")
	table.Store = &headErrorStore{ObjectStore: table.Store, headErr: throttleErr}

	_, err = table.LatestVersion()
	if err == nil {
		t.Fatal("expected LatestVersion to fail when Head fails with a non-404 error")
	}
	if !errors.Is(err, throttleErr) {
		t.Fatalf("expected the underlying Head error to be propagated, got %v", err)
	}
}

// F2: a fixLog failure must be returned to the caller, not masked to nil by the
// unlock defer. The entry's temp file does not exist, so every fix attempt fails.
func TestFixLogFailureIsReturned(t *testing.T) {
	_, table, _, _, _ := setUpSingleClusterLogStoreTest(t)

	entry := logstore.New(
		table.Store.BaseURI(),
		storage.NewPath("00000000000000000099.json"),
		storage.NewPath(".tmp/00000000000000000099.json."+uuid.NewString()),
		false /* isComplete */, 0 /* expirationTime */)

	transaction := table.CreateTransaction(NewTransactionOptions())
	err := transaction.fixLog(entry)
	if err == nil {
		t.Fatal("expected fixLog to return an error when the temp file is missing")
	}
}

// F3: prepareCommit must propagate action serialization failures.
func TestPrepareCommitSerializationErrorIsReturned(t *testing.T) {
	table, _, _ := setupTest(t)

	transaction := table.CreateTransaction(NewTransactionOptions())
	// A channel cannot be marshalled to JSON
	transaction.AddAction(CommitInfo{"bad": make(chan int)})
	_, err := transaction.prepareCommit()
	if err == nil {
		t.Fatal("expected prepareCommit to return a serialization error")
	}
}

// F8: client version helpers
func TestIsVersionWrittenByDeltaGo(t *testing.T) {
	_, table, actions, operation, appMetaData := setUpSingleClusterLogStoreTest(t)

	transaction := table.CreateTransaction(NewTransactionOptions())
	transaction.AddActions(actions)
	transaction.SetOperation(operation)
	transaction.SetAppMetadata(appMetaData)
	version, err := transaction.CommitLogStore()
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	written, err := table.IsVersionWrittenByDeltaGo(version)
	if err != nil {
		t.Fatalf("IsVersionWrittenByDeltaGo failed: %v", err)
	}
	if !written {
		t.Error("expected version committed via CommitLogStore to be recognized as written by delta-go")
	}
}

func TestCommitInfoClientVersion(t *testing.T) {
	ci := CommitInfo{"clientVersion": "delta-go.alpha-0.0.0"}
	if got := ci.ClientVersion(); got != "delta-go.alpha-0.0.0" {
		t.Errorf("ClientVersion() = %q", got)
	}
	ci = CommitInfo{}
	if got := ci.ClientVersion(); got != "" {
		t.Errorf("ClientVersion() on empty commit info = %q, want empty", got)
	}
	ci = CommitInfo{"clientVersion": 42}
	if got := ci.ClientVersion(); got != "" {
		t.Errorf("ClientVersion() on non-string entry = %q, want empty", got)
	}
}

// F11: NewAdd must return ErrObjectDoesNotExist for a missing file (previously the
// reversed errors.Is converted this into ErrAddZeroSize), and must propagate transient
// Head errors instead of misreporting them as a zero-size add.
func TestNewAddMissingFile(t *testing.T) {
	table, _, _ := setupTest(t)

	_, _, err := NewAdd(table.Store, storage.NewPath("does-not-exist.parquet"), nil)
	if !errors.Is(err, storage.ErrObjectDoesNotExist) {
		t.Errorf("expected ErrObjectDoesNotExist, got %v", err)
	}
	if errors.Is(err, ErrAddZeroSize) {
		t.Error("a missing file must not be reported as a zero-size add")
	}
}

func TestNewAddTransientHeadError(t *testing.T) {
	table, _, _ := setupTest(t)

	throttleErr := errors.New("simulated throttling error")
	store := &headErrorStore{ObjectStore: table.Store, headErr: throttleErr}

	_, _, err := NewAdd(store, storage.NewPath("any.parquet"), nil)
	if !errors.Is(err, throttleErr) {
		t.Errorf("expected the Head error to be propagated, got %v", err)
	}
	if errors.Is(err, ErrAddZeroSize) {
		t.Error("a transient Head error must not be reported as a zero-size add")
	}
}

// F10: NewAdd should perform exactly one Head request per file; the stats generation
// must reuse the size already known to the Add action.
func TestNewAddPerformsSingleHead(t *testing.T) {
	table, _, tmpDir := setupTest(t)

	fileName := fmt.Sprintf("part-%s.snappy.parquet", uuid.NewString())
	filePath := filepath.Join(tmpDir, fileName)
	if _, err := writeParquet(makeTestData(5), filePath); err != nil {
		t.Fatal(err)
	}

	store := &headErrorStore{ObjectStore: table.Store}
	_, _, err := NewAdd(store, storage.NewPath(fileName), nil)
	if err != nil {
		t.Fatal(err)
	}
	if store.headCalls != 1 {
		t.Errorf("expected exactly 1 Head call in NewAdd, got %d", store.headCalls)
	}
}

// F22: a multi-part checkpoint boundary landing on the Txn rows must not skip a Txn.
func TestCheckpointRowsTxnPartBoundary(t *testing.T) {
	state := NewTableState(5)
	metadata := NewTableMetaData("test", "", new(Format).Default(), SchemaTypeStruct{}, nil, make(map[string]string))
	state.CurrentMetadata = metadata
	state.AppTransactionVersion["app1"] = 1
	state.AppTransactionVersion["app2"] = 2

	config := NewCheckpointConfiguration()

	// startOffset 2 == first row after protocol and metadata: must be txn app1
	rows, err := checkpointRows(state, 2, config)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 txn rows from offset 2, got %d", len(rows))
	}
	if rows[0].Txn == nil || rows[0].Txn.AppID != "app1" {
		t.Errorf("expected first row to be txn app1, got %+v", rows[0])
	}
	if rows[1].Txn == nil || rows[1].Txn.AppID != "app2" {
		t.Errorf("expected second row to be txn app2, got %+v", rows[1])
	}
}

// F21: if any unexpired tombstone lacks extended file metadata, all tombstones must have
// extended file metadata stripped during checkpoint preparation.
func TestPrepareStateForCheckpointStripsExtendedFileMetadata(t *testing.T) {
	state := NewTableState(5)
	metadata := NewTableMetaData("test", "", new(Format).Default(), SchemaTypeStruct{}, nil, make(map[string]string))
	state.CurrentMetadata = metadata
	state.Tombstones["with-metadata.parquet"] = Remove{Path: "with-metadata.parquet", ExtendedFileMetadata: true}
	state.Tombstones["without-metadata.parquet"] = Remove{Path: "without-metadata.parquet", ExtendedFileMetadata: false}

	if err := state.prepareStateForCheckpoint(nil); err != nil {
		t.Fatal(err)
	}
	for path, remove := range state.Tombstones {
		if remove.ExtendedFileMetadata {
			t.Errorf("expected extended file metadata to be stripped from %s", path)
		}
	}
}

// F13: SkipTableCreation must skip the DescribeTable/CreateTable check.
func TestLogStoreSkipTableCreation(t *testing.T) {
	client := dynamodbutils.NewMockClient()
	tableName := "skip_creation_log_store"

	if _, err := dynamodblogstore.New(dynamodblogstore.Options{Client: client, TableName: tableName, SkipTableCreation: true}); err != nil {
		t.Fatalf("expected New with SkipTableCreation to succeed, got %v", err)
	}
	if _, ok := client.TablesToItems().Get(tableName); ok {
		t.Error("expected no DynamoDB table to be created when SkipTableCreation is set")
	}

	// Without the option the table is created
	if _, err := dynamodblogstore.New(dynamodblogstore.Options{Client: client, TableName: tableName}); err != nil {
		t.Fatalf("expected New to succeed, got %v", err)
	}
	if _, ok := client.TablesToItems().Get(tableName); !ok {
		t.Error("expected the DynamoDB table to be created when SkipTableCreation is not set")
	}
}

// F6/F16: a missing log store entry must be reported as ErrEntryDoesNotExist.
func TestLogStoreGetMissingEntry(t *testing.T) {
	client := dynamodbutils.NewMockClient()
	ls, err := dynamodblogstore.New(dynamodblogstore.Options{Client: client, TableName: "missing_entry_log_store"})
	if err != nil {
		t.Fatal(err)
	}

	_, err = ls.Get(storage.NewPath("s3://bucket/table"), storage.NewPath("00000000000000000000.json"))
	if !errors.Is(err, logstore.ErrEntryDoesNotExist) {
		t.Errorf("expected ErrEntryDoesNotExist, got %v", err)
	}
}
