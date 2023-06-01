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
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/rivian/delta-go/lock"
	"github.com/rivian/delta-go/lock/filelock"
	"github.com/rivian/delta-go/state"
	"github.com/rivian/delta-go/state/filestate"
	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/filestore"
)

// / Helper function to set up test state
func setupCheckpointTest(t *testing.T, inputFolder string, overrideStore bool) (store storage.ObjectStore, state state.StateStore, lock lock.Locker, checkpointLock lock.Locker) {
	t.Helper()

	tmpDir := t.TempDir()
	tmpPath := storage.NewPath(tmpDir)
	if overrideStore {
		store = filestore.New(storage.NewPath("/tmp/parquet"))
	} else {
		store = filestore.New(tmpPath)
	}
	state = filestate.New(tmpPath, "_delta_log/_commit.state")
	lock = filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.LockOptions{})
	checkpointLock = filelock.New(tmpPath, "_delta_log/_checkpoint.lock", filelock.LockOptions{})

	if len(inputFolder) > 0 {
		// Copy input folder to temp folder
		err := copyFilesToTempDirRecursively(t, inputFolder, tmpDir)
		if err != nil {
			t.Fatal(err)
		}
	}
	return
}

func copyFilesToTempDirRecursively(t *testing.T, inputFolder string, outputFolder string) error {
	t.Helper()

	results, err := os.ReadDir(inputFolder)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, r := range results {
		outputPath := filepath.Join(outputFolder, r.Name())
		inputPath := filepath.Join(inputFolder, r.Name())
		if r.IsDir() {
			err = os.Mkdir(outputPath, 0755)
			if err != nil {
				return err
			}
			err = copyFilesToTempDirRecursively(t, inputPath, outputPath)
			if err != nil {
				return err
			}
		} else {
			out, err := os.Create(outputPath)
			if err != nil {
				return err
			}
			in, err := os.Open(inputPath)
			if err != nil {
				return err
			}
			_, err = io.Copy(out, in)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type SimpleCheckpointTestData struct {
	Value string    `json:"value" parquet:"value"`
	TS    time.Time `json:"ts" parquet:"ts,timestamp"`
	Date  string    `json:"date" parquet:"date"`
}

type SimpleCheckpointTestPartition struct {
	Date string `json:"date" parquet:"date"`
}

func TestSimpleCheckpoint(t *testing.T) {
	store, state, lock, checkpointLock := setupCheckpointTest(t, "testdata/checkpoints", false)
	checkpointConfiguration := NewCheckpointConfiguration()

	// Create a checkpoint at version 5
	_, err := CreateCheckpoint[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, checkpointLock, checkpointConfiguration, 5)
	if err != nil {
		t.Fatal(err)
	}

	// Does the checkpoint exist
	_, err = store.Head(storage.NewPath("_delta_log/00000000000000000005.checkpoint.parquet"))
	if err != nil {
		t.Fatal(err)
	}

	// Does _last_checkpoint point to the checkpoint file
	table := NewDeltaTable[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, lock, state)
	checkpoints, allReturned, err := table.findLatestCheckpointsForVersion(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) != 1 {
		t.Errorf("expected %d checkpoint, found %d", 1, len(checkpoints))
	}
	if allReturned {
		t.Errorf("allReturned is true but should be false since _last_checkpoint was used")
	}
	if len(checkpoints) > 0 {
		lastCheckpoint := checkpoints[len(checkpoints)-1]
		if lastCheckpoint.Version != 5 {
			t.Errorf("last checkpoint version is %d, should be 5", lastCheckpoint.Version)
		}
	}

	// Checkpoint at version 10
	_, err = CreateCheckpoint[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, checkpointLock, checkpointConfiguration, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Checkpoint file exists
	_, err = store.Head(storage.NewPath("_delta_log/00000000000000000010.checkpoint.parquet"))
	if err != nil {
		t.Fatal(err)
	}

	// Does _last_checkpoint point to the checkpoint file
	checkpoints, allReturned, err = table.findLatestCheckpointsForVersion(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) != 1 {
		t.Errorf("expected %d checkpoint, found %d", 1, len(checkpoints))
	}
	if allReturned {
		t.Errorf("allReturned is true but should be false since _last_checkpoint was used")
	}
	if len(checkpoints) > 0 {
		lastCheckpoint := checkpoints[len(checkpoints)-1]
		if lastCheckpoint.Version != 10 {
			t.Errorf("last checkpoint version is %d, should be 10", lastCheckpoint.Version)
		}
	}
	// Reload table
	table, err = OpenTable[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, lock, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(table.State.Files) != 12 {
		t.Errorf("Found %d files, expected 12", len(table.State.Files))
	}

	// Can't create a checkpoint if it already exists
	_, err = CreateCheckpoint[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, checkpointLock, checkpointConfiguration, 10)
	if !errors.Is(err, ErrorCheckpointAlreadyExists) {
		t.Error("creating a checkpoint when it already exists did not return correct error")
	}
}

type TombstonesTestData struct {
	Id int32 `parquet:"id" json:"id"`
}

func getTestAdd[RowType any, PartitionType any](offsetMillis int64) *Add[RowType, PartitionType] {
	add := new(Add[RowType, PartitionType])
	add.Path = uuid.NewString()
	add.Size = 100
	add.DataChange = true
	add.PartitionValues = make(map[string]string)
	add.ModificationTime = DeltaDataTypeTimestamp(time.Now().UnixMilli() - offsetMillis)
	return add
}

func getTestRemove(offsetMillis int64, path string) *Remove {
	remove := new(Remove)
	remove.Path = path
	remove.Size = 100
	remove.DataChange = true
	remove.PartitionValues = make(map[string]string)
	remove.DeletionTimestamp = DeltaDataTypeTimestamp(time.Now().UnixMilli() - offsetMillis)
	return remove
}

func testDoCommit[RowType any, PartitionType any](t *testing.T, table *DeltaTable[RowType, PartitionType], actions []Action) (state.DeltaDataTypeVersion, error) {
	t.Helper()
	tx := table.CreateTransaction(&DeltaTransactionOptions{})
	tx.AddActions(actions)
	return tx.Commit(nil, nil)
}

func TestTombstones(t *testing.T) {
	store, state, lock, checkpointLock := setupCheckpointTest(t, "", false)
	checkpointConfiguration := NewCheckpointConfiguration()

	table := NewDeltaTable[TombstonesTestData, SimpleCheckpointTestPartition](store, lock, state)

	// Set tombstone expiry time to 2 hours
	metadata := NewDeltaTableMetaData("", "", Format{}, GetSchema(new(TombstonesTestData)), make([]string, 0), map[string]string{string(DeletedFileRetentionDurationDeltaConfigKey): "interval 2 hours"})
	protocol := Protocol{MinReaderVersion: 1, MinWriterVersion: 2}

	table.Create(*metadata, protocol, CommitInfo{}, make([]Add[TombstonesTestData, SimpleCheckpointTestPartition], 0))
	add1 := getTestAdd[TombstonesTestData, SimpleCheckpointTestPartition](3 * 60 * 1000) // 3 mins ago
	add2 := getTestAdd[TombstonesTestData, SimpleCheckpointTestPartition](2 * 60 * 1000) // 2 mins ago
	v, err := testDoCommit(t, table, []Action{add1})
	if err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Errorf("Version is %d, expected 1", v)
	}
	v, err = testDoCommit(t, table, []Action{add2})
	if err != nil {
		t.Fatal(err)
	}
	if v != 2 {
		t.Errorf("Version is %d, expected 2", v)
	}

	// Create a checkpoint
	_, err = CreateCheckpoint[TombstonesTestData, SimpleCheckpointTestPartition](store, checkpointLock, checkpointConfiguration, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Load the checkpoint
	// Reload table
	table, err = OpenTable[TombstonesTestData, SimpleCheckpointTestPartition](store, lock, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(table.State.Files) != 2 {
		t.Errorf("State contains %d files, expected 2", len(table.State.Files))
	}
	_, ok := table.State.Files[add1.Path]
	if !ok {
		t.Errorf("Missing file %s", add1.Path)
	}
	_, ok = table.State.Files[add2.Path]
	if !ok {
		t.Errorf("Missing file %s", add2.Path)
	}

	// Simulate an optimize at 5 minutes ago: the tombstones should not be expired since that's set to 2 hours
	optimizeTime := int64(5) * 60 * 1000
	remove1 := getTestRemove(optimizeTime, add1.Path)
	remove2 := getTestRemove(optimizeTime, add2.Path)
	add3 := getTestAdd[TombstonesTestData, SimpleCheckpointTestPartition](optimizeTime)
	add4 := getTestAdd[TombstonesTestData, SimpleCheckpointTestPartition](optimizeTime)
	v, err = testDoCommit(t, table, []Action{remove1, remove2, add3, add4})
	if err != nil {
		t.Fatal(err)
	}
	if v != 3 {
		t.Errorf("Version is %d, expected 3", v)
	}

	// Create a checkpoint and load it
	_, err = CreateCheckpoint[TombstonesTestData, SimpleCheckpointTestPartition](store, checkpointLock, checkpointConfiguration, 3)
	if err != nil {
		t.Fatal(err)
	}
	table, err = OpenTable[TombstonesTestData, SimpleCheckpointTestPartition](store, lock, state)
	if err != nil {
		t.Fatal(err)
	}

	// Verify only the new adds are present
	if len(table.State.Files) != 2 {
		t.Errorf("State contains %d files, expected 2", len(table.State.Files))
	}
	_, ok = table.State.Files[add3.Path]
	if !ok {
		t.Errorf("Missing file %s", add3.Path)
	}
	_, ok = table.State.Files[add4.Path]
	if !ok {
		t.Errorf("Missing file %s", add4.Path)
	}
	// Verify tombstones are present
	if len(table.State.Tombstones) != 2 {
		t.Errorf("State contains %d tombstones, expected 2", len(table.State.Tombstones))
	}
}

func TestExpiredTombstones(t *testing.T) {
	store, state, lock, checkpointLock := setupCheckpointTest(t, "", false)
	checkpointConfiguration := NewCheckpointConfiguration()

	table := NewDeltaTable[TombstonesTestData, SimpleCheckpointTestPartition](store, lock, state)

	metadata := NewDeltaTableMetaData("", "", Format{}, GetSchema(new(TombstonesTestData)), make([]string, 0), map[string]string{string(DeletedFileRetentionDurationDeltaConfigKey): "interval 1 minute"})
	protocol := Protocol{MinReaderVersion: 1, MinWriterVersion: 2}
	table.Create(*metadata, protocol, CommitInfo{}, make([]Add[TombstonesTestData, SimpleCheckpointTestPartition], 0))
	add1 := getTestAdd[TombstonesTestData, SimpleCheckpointTestPartition](3 * 60 * 1000) // 3 mins ago
	add2 := getTestAdd[TombstonesTestData, SimpleCheckpointTestPartition](2 * 60 * 1000) // 2 mins ago
	v, err := testDoCommit(t, table, []Action{add1})
	if err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Errorf("Version is %d, expected 1", v)
	}
	v, err = testDoCommit(t, table, []Action{add2})
	if err != nil {
		t.Fatal(err)
	}
	if v != 2 {
		t.Errorf("Version is %d, expected 2", v)
	}

	// Create a checkpoint
	_, err = CreateCheckpoint[TombstonesTestData, SimpleCheckpointTestPartition](store, checkpointLock, checkpointConfiguration, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Load the checkpoint
	// Reload table
	table, err = OpenTable[TombstonesTestData, SimpleCheckpointTestPartition](store, lock, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(table.State.Files) != 2 {
		t.Errorf("State contains %d files, expected 2", len(table.State.Files))
	}
	_, ok := table.State.Files[add1.Path]
	if !ok {
		t.Errorf("Missing file %s", add1.Path)
	}
	_, ok = table.State.Files[add2.Path]
	if !ok {
		t.Errorf("Missing file %s", add2.Path)
	}

	// Simulate an optimize
	optimizeTime := int64(5) * 59 * 1000
	remove1 := getTestRemove(optimizeTime, add1.Path)
	remove2 := getTestRemove(optimizeTime, add2.Path)
	add3 := getTestAdd[TombstonesTestData, SimpleCheckpointTestPartition](optimizeTime)
	add4 := getTestAdd[TombstonesTestData, SimpleCheckpointTestPartition](optimizeTime)
	v, err = testDoCommit(t, table, []Action{remove1, remove2, add3, add4})
	if err != nil {
		t.Fatal(err)
	}
	if v != 3 {
		t.Errorf("Version is %d, expected 3", v)
	}

	// Create a checkpoint and load it
	_, err = CreateCheckpoint[TombstonesTestData, SimpleCheckpointTestPartition](store, checkpointLock, checkpointConfiguration, 3)
	if err != nil {
		t.Fatal(err)
	}
	table, err = OpenTable[TombstonesTestData, SimpleCheckpointTestPartition](store, lock, state)
	if err != nil {
		t.Fatal(err)
	}

	// Verify only the new adds are present
	if len(table.State.Files) != 2 {
		t.Errorf("State contains %d files, expected 2", len(table.State.Files))
	}
	_, ok = table.State.Files[add3.Path]
	if !ok {
		t.Errorf("Missing file %s", add3.Path)
	}
	_, ok = table.State.Files[add4.Path]
	if !ok {
		t.Errorf("Missing file %s", add4.Path)
	}
	// Verify stale tombstones were removed
	if len(table.State.Tombstones) != 0 {
		t.Errorf("State contains %d tombstones, expected 0", len(table.State.Tombstones))
	}
}

func TestMultiPartCheckpoint(t *testing.T) {
	store, stateStore, lock, checkpointLock := setupCheckpointTest(t, "", false)
	checkpointConfiguration := NewCheckpointConfiguration()
	checkpointConfiguration.MaxRowsPerPart = 5

	table := NewDeltaTable[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, lock, stateStore)

	metadata := NewDeltaTableMetaData("test-data", "For testing multi-part checkpoints", Format{Provider: "tester", Options: map[string]string{"hello": "world"}},
		GetSchema(new(SimpleCheckpointTestData)), make([]string, 0), map[string]string{"delta.isTest": "true"})
	protocol := Protocol{MinReaderVersion: 4, MinWriterVersion: 3}
	table.Create(*metadata, protocol, CommitInfo{}, make([]Add[SimpleCheckpointTestData, SimpleCheckpointTestPartition], 0))
	paths := make([]string, 0, 10)
	// Commit ten Add actions
	for i := 0; i < 10; i++ {
		add := getTestAdd[SimpleCheckpointTestData, SimpleCheckpointTestPartition](60 * 1000)
		paths = append(paths, add.Path)
		v, err := testDoCommit(t, table, []Action{add})
		if err != nil {
			t.Fatal(err)
		}
		if int(v) != i+1 {
			t.Errorf("Version is %d, expected %d", v, i+1)
		}
	}
	sort.Strings(paths)

	// Commit a delete
	remove := getTestRemove(0, paths[0])
	v, err := testDoCommit(t, table, []Action{remove})
	if err != nil {
		t.Fatal(err)
	}
	if int(v) != 11 {
		t.Errorf("Version is %d, expected %d", v, 11)
	}

	// And a txn
	txn := new(Txn)
	txn.AppId = "testApp"
	txn.LastUpdated = DeltaDataTypeTimestamp(time.Now().UnixMilli())
	txn.Version = DeltaDataTypeVersion(v)
	v, err = testDoCommit(t, table, []Action{txn})
	if err != nil {
		t.Fatal(err)
	}
	if int(v) != 12 {
		t.Errorf("Version is %d, expected %d", v, 12)
	}

	// Create a checkpoint.
	// There should be 14 rows: 1 protocol and 1 metadata, 10 adds, 1 remove and 1 txn.
	// With max 5 rows per checkpoint part, we should get 3 parquet files.
	_, err = CreateCheckpoint[TombstonesTestData, SimpleCheckpointTestPartition](store, checkpointLock, checkpointConfiguration, 12)
	if err != nil {
		t.Fatal(err)
	}

	// Do all three checkpoint files exist
	_, err = store.Head(storage.NewPath("_delta_log/00000000000000000012.checkpoint.0000000001.0000000003.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Head(storage.NewPath("_delta_log/00000000000000000012.checkpoint.0000000002.0000000003.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Head(storage.NewPath("_delta_log/00000000000000000012.checkpoint.0000000003.0000000003.parquet"))
	if err != nil {
		t.Fatal(err)
	}

	// Does _last_checkpoint point to the checkpoint file
	table = NewDeltaTable[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, lock, stateStore)
	checkpoints, allReturned, err := table.findLatestCheckpointsForVersion(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) != 1 {
		t.Errorf("expected %d checkpoint, found %d", 1, len(checkpoints))
	}
	if allReturned {
		t.Errorf("allReturned is true but should be false since _last_checkpoint was used")
	}
	if len(checkpoints) > 0 {
		lastCheckpoint := checkpoints[len(checkpoints)-1]
		if lastCheckpoint.Version != 12 {
			t.Errorf("last checkpoint version is %d, expected 12", lastCheckpoint.Version)
		}
		if lastCheckpoint.Parts != 3 {
			t.Errorf("last checkpoint parts count is %d, expected 3", lastCheckpoint.Parts)
		}
	}

	// Load the multipart checkpoint
	err = table.Load()
	if err != nil {
		t.Fatal(err)
	}

	// Check all the adds are correct; we removed the first add
	if len(table.State.Files) != 9 {
		t.Errorf("Found %d files, expected 9", len(table.State.Files))
	} else {
		keys := make([]string, 0, len(table.State.Files))
		for k := range table.State.Files {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i := 0; i < 9; i++ {
			if keys[i] != paths[i+1] {
				t.Errorf("Found path %s, expected %s", keys[i], paths[i])
			}
		}
	}

	// Check the metadata is correct
	if table.State.CurrentMetadata.Name != metadata.Name {
		t.Errorf("Found metadata name %s, expected %s", table.State.CurrentMetadata.Name, metadata.Name)
	}
	if table.State.CurrentMetadata.Description != metadata.Description {
		t.Errorf("Found metadata description %s, expected %s", table.State.CurrentMetadata.Description, metadata.Description)
	}
	if !reflect.DeepEqual(table.State.CurrentMetadata.Format, metadata.Format) {
		t.Errorf("Found metadata format %v, expected %v", table.State.CurrentMetadata.Format, metadata.Format)
	}
	if !reflect.DeepEqual(table.State.CurrentMetadata.Configuration, metadata.Configuration) {
		t.Errorf("Found metadata configuration %v, expected %v", table.State.CurrentMetadata.Configuration, metadata.Configuration)
	}

	// Check the tombstone is correct
	if len(table.State.Tombstones) != 1 {
		t.Errorf("Found %d tombstones, expected 1", len(table.State.Tombstones))
	} else {
		checkpointRemove, ok := table.State.Tombstones[paths[0]]
		if !ok {
			t.Errorf("Missing expected tombstone %s", paths[0])
		} else {
			if remove.Path != checkpointRemove.Path {
				t.Errorf("Found tombstone path %s, expected %s", remove.Path, checkpointRemove.Path)
			}
		}
	}

	// Check the txn is correct
	if len(table.State.AppTransactionVersion) != 1 {
		t.Errorf("Found %d app versions, expected 1", len(table.State.AppTransactionVersion))
	} else {
		version, ok := table.State.AppTransactionVersion[txn.AppId]
		if !ok {
			t.Error("Did not find expected app in app versions")
		} else {
			if version != state.DeltaDataTypeVersion(txn.Version) {
				t.Errorf("Found version %d in app versions, expected %d", version, txn.Version)
			}
		}
	}

	// Verify correct protocol
	if table.State.MinReaderVersion != protocol.MinReaderVersion {
		t.Errorf("State MinReaderVersion is %d, expected %d", table.State.MinReaderVersion, protocol.MinReaderVersion)
	}
	if table.State.MinWriterVersion != protocol.MinWriterVersion {
		t.Errorf("State MinWriterVersion is %d, expected %d", table.State.MinWriterVersion, protocol.MinWriterVersion)
	}

	// Remove _last_checkpoint
	err = store.Delete(storage.NewPath("_delta_log/_last_checkpoint"))
	if err != nil {
		t.Fatal(err)
	}

	// Re-load and check version
	err = table.Load()
	if err != nil {
		t.Fatal(err)
	}
	if table.State.Version != 12 {
		t.Errorf("Expected versino %d, found %d", 12, table.State.Version)
	}
}

func TestCheckpointInfoFromURI(t *testing.T) {
	type test struct {
		input          string
		wantCheckpoint *CheckPoint
		wantPart       DeltaDataTypeInt
	}

	tests := []test{
		{input: "_delta_log/00000000000000000000.json", wantCheckpoint: nil},
		{input: "_delta_log/01234567890123456789.json", wantCheckpoint: nil},
		{input: "_delta_log/_commit_aabbccdd-eeff-1122-3344-556677889900.json.tmp", wantCheckpoint: nil},
		{input: "_delta_log/00000000000000000001.checkpoint.parquet", wantCheckpoint: &CheckPoint{Version: 1, Size: 0, Parts: 0}, wantPart: 0},
		{input: "_delta_log/00000000000000123456.checkpoint.0000000002.0000000063.parquet", wantCheckpoint: &CheckPoint{Version: 123456, Size: 0, Parts: 63}, wantPart: 2},
	}

	for _, tc := range tests {
		gotCheckpoint, gotPart, err := checkpointInfoFromURI(storage.NewPath(tc.input))
		if err != nil {
			t.Error(err)
		}
		if gotCheckpoint == nil {
			if tc.wantCheckpoint != nil {
				t.Errorf("expected %v, got nil", tc.wantCheckpoint)
			}
			continue
		}
		if tc.wantCheckpoint == nil {
			t.Errorf("expected nil, got %v", gotCheckpoint)
			continue
		}

		if *gotCheckpoint != *tc.wantCheckpoint {
			t.Errorf("expected %v, got %v", *tc.wantCheckpoint, *gotCheckpoint)
		}
		if gotPart != tc.wantPart {
			t.Errorf("expected %d, got %d", tc.wantPart, gotPart)
		}
	}
}

func TestDoesCheckpointVersionExist(t *testing.T) {
	store, _, _, checkpointLock := setupCheckpointTest(t, "testdata/checkpoints", false)
	checkpointConfiguration := NewCheckpointConfiguration()
	checkpointConfiguration.MaxRowsPerPart = 8

	// There is no checkpoint at version 5 yet
	checkpointExists, err := doesCheckpointVersionExist(store, 5, false)
	if err != nil {
		t.Fatal(err)
	}
	if checkpointExists {
		t.Error("checkpoint should not exist")
	}

	// Create a checkpoint at version 5
	_, err = CreateCheckpoint[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, checkpointLock, checkpointConfiguration, 5)
	if err != nil {
		t.Fatal(err)
	}
	// Verify checkpoint exists
	checkpointExists, err = doesCheckpointVersionExist(store, 5, false)
	if err != nil {
		t.Error(err)
	}
	if !checkpointExists {
		t.Error("checkpoint should exist")
	}

	// Create a multi-part checkpoint at version 10
	_, err = CreateCheckpoint[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, checkpointLock, checkpointConfiguration, 10)
	if err != nil {
		t.Fatal(err)
	}
	// Verify checkpoint exists without multi-part validation
	checkpointExists, err = doesCheckpointVersionExist(store, 10, false)
	if err != nil {
		t.Error(err)
	}
	if !checkpointExists {
		t.Error("checkpoint should exist")
	}

	// Validate multi-part is all present
	checkpointExists, err = doesCheckpointVersionExist(store, 10, true)
	if err != nil {
		t.Error(err)
	}
	if !checkpointExists {
		t.Error("checkpoint should exist")
	}

	// Rename a piece of the multi-part
	err = store.Rename(storage.NewPath("_delta_log/00000000000000000010.checkpoint.0000000001.0000000002.parquet"), storage.NewPath("_delta_log/00000000000000000010.checkpoint.0000000001.0000000003.parquet"))
	if err != nil {
		t.Error(err)
	}

	// Validating the multi-part should return an error
	_, err = doesCheckpointVersionExist(store, 10, true)
	if !errors.Is(err, ErrorCheckpointInvalidFileName) {
		t.Error("doesCheckpointVersionExist on incomplete checkpoint did not return correct error")
	}

	// Delete one piece of the multi-part
	err = store.Delete(storage.NewPath("_delta_log/00000000000000000010.checkpoint.0000000001.0000000003.parquet"))
	if err != nil {
		t.Error(err)
	}

	// Validating the multi-part should return an error
	_, err = doesCheckpointVersionExist(store, 10, true)
	if !errors.Is(err, ErrorCheckpointIncomplete) {
		t.Error("doesCheckpointVersionExist on incomplete checkpoint did not return correct error")
	}
}

func TestInvalidCheckpointFallback(t *testing.T) {
	store, state, lock, checkpointLock := setupCheckpointTest(t, "testdata/checkpoints", false)
	checkpointConfiguration := NewCheckpointConfiguration()

	// Create a checkpoint at version 5
	_, err := CreateCheckpoint[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, checkpointLock, checkpointConfiguration, 5)
	if err != nil {
		t.Fatal(err)
	}

	// Create a checkpoint at version 10
	_, err = CreateCheckpoint[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, checkpointLock, checkpointConfiguration, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Replace the version 10 checkpoint with an invalid file
	err = store.Put(storage.NewPath("_delta_log/00000000000000000010.checkpoint.parquet"), []byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	// Open table; _last_checkpoint is pointing to an invalid checkpoint now
	table, err := OpenTable[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, lock, state)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure we still loaded the last version
	if table.State.Version != 12 {
		t.Errorf("expected version %d, found %d", 12, table.State.Version)
	}

	// Modify _last_checkpoint to also be invalid
	err = store.Put(storage.NewPath("_delta_log/_last_checkpoint"), []byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	// Open table
	table, err = OpenTable[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, lock, state)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure we still loaded the last version
	if table.State.Version != 12 {
		t.Errorf("expected version %d, found %d", 12, table.State.Version)
	}

	// Delete checkpoint 5
	err = store.Delete(storage.NewPath("_delta_log/00000000000000000005.checkpoint.parquet"))
	if err != nil {
		t.Fatal(err)
	}

	// Open table
	table, err = OpenTable[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, lock, state)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure we still loaded the last version
	if table.State.Version != 12 {
		t.Errorf("expected version %d, found %d", 12, table.State.Version)
	}
}