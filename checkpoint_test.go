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
	"strconv"
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
func setupCheckpointTest(t *testing.T, inputFolder string, overrideStore bool) (store *filestore.FileObjectStore, state state.StateStore, lock lock.Locker, checkpointLock lock.Locker) {
	t.Helper()

	tmpDir := t.TempDir()
	tmpPath := storage.NewPath(tmpDir)
	if overrideStore {
		store = filestore.New(storage.NewPath("/tmp/parquet"))
	} else {
		store = filestore.New(tmpPath)
	}

	if len(inputFolder) > 0 {
		// Copy input folder to temp folder
		err := copyFilesToTempDirRecursively(t, inputFolder, tmpDir)
		if err != nil {
			t.Fatal(err)
		}
	}

	os.MkdirAll(filepath.Join(tmpDir, "_delta_log"), 0777)
	state = filestate.New(tmpPath, "_delta_log/_commit.state")
	lock = filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{})
	checkpointLock = filelock.New(tmpPath, "_delta_log/_checkpoint.lock", filelock.Options{})
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

func TestSimpleCheckpoint(t *testing.T) {
	store, state, lock, checkpointLock := setupCheckpointTest(t, "testdata/checkpoints", false)
	checkpointConfiguration := NewCheckpointConfiguration()

	// Create a checkpoint at version 5
	_, err := CreateCheckpoint(store, checkpointLock, checkpointConfiguration, 5)
	if err != nil {
		t.Fatal(err)
	}

	// Does the checkpoint exist
	_, err = store.Head(storage.NewPath("_delta_log/00000000000000000005.checkpoint.parquet"))
	if err != nil {
		t.Fatal(err)
	}

	// Does _last_checkpoint point to the checkpoint file
	table := NewDeltaTable(store, lock, state)
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

	// Remove the previous log to make sure we use the checkpoint when loading
	err = store.Delete(CommitUriFromVersion(4))
	if err != nil {
		t.Error(err)
	}

	// Checkpoint at version 10
	_, err = CreateCheckpoint(store, checkpointLock, checkpointConfiguration, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Checkpoint file exists
	checkpointMeta, err := store.Head(storage.NewPath("_delta_log/00000000000000000010.checkpoint.parquet"))
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
		if lastCheckpoint.NumOfAddFiles != 10 {
			t.Errorf("last checkpoint number of add files is %d, should be 10", lastCheckpoint.NumOfAddFiles)
		}
		if lastCheckpoint.Size != 12 {
			t.Errorf("last checkpoint number of actions is %d, should be 12", lastCheckpoint.Size)
		}

		if lastCheckpoint.SizeInBytes != checkpointMeta.Size {
			t.Errorf("last checkpoint size in bytes is %d, should be %d", lastCheckpoint.SizeInBytes, checkpointMeta.Size)
		}
	}
	// Remove the previous log to make sure we use the checkpoint when loading
	err = store.Delete(CommitUriFromVersion(9))
	if err != nil {
		t.Error(err)
	}

	// Reload table
	table, err = OpenTable(store, lock, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(table.State.Files) != 12 {
		t.Errorf("Found %d files, expected 12", len(table.State.Files))
	}

	// Spot check contents. This add is from version 4, whose JSON file has been removed, so
	// it is certain to have been loaded from a checkpoint parquet file.
	checkPath := "date=2020-06-01/part-00000-ee6161de-c5be-4117-9ffe-e09b7475dbc7.c000.snappy.parquet"
	checkAdd, present := table.State.Files[checkPath]
	if !present {
		t.Errorf("Expected file at %s but not found", checkPath)
	} else {
		expectedStats := "{\"numRecords\":3,\"minValues\":{\"value\":\"c\",\"ts\":\"2021-07-29T18:11:33.223Z\"},\"maxValues\":{\"value\":\"y\",\"ts\":\"2021-08-31T13:01:45.876Z\"},\"nullCount\":{\"value\":1,\"ts\":0}}"
		expectedAdd := Add{
			Path:             checkPath,
			PartitionValues:  map[string]string{"date": "2020-06-01"},
			Size:             4567,
			ModificationTime: 1627668694000,
			DataChange:       false,
			Stats:            expectedStats,
		}
		if !reflect.DeepEqual(expectedAdd, checkAdd) {
			t.Errorf("Add does not match: expected %v found %v", expectedAdd, checkAdd)
		}
	}

	// Can't create a checkpoint if it already exists
	_, err = CreateCheckpoint(store, checkpointLock, checkpointConfiguration, 10)
	if !errors.Is(err, ErrorCheckpointAlreadyExists) {
		t.Errorf("creating a checkpoint when it already exists did not return correct error, %v", err)
	}
}

type tombstonesTestData struct {
	Id int32 `parquet:"name=id, type=INT32" json:"id"`
}

func getTestAdd(offsetMillis int64) *Add {
	add := new(Add)
	path := uuid.NewString()
	add.Path = path
	add.Size = 100
	dataChange := true
	add.DataChange = dataChange
	partitionValues := make(map[string]string)
	add.PartitionValues = partitionValues
	add.ModificationTime = time.Now().UnixMilli() - offsetMillis
	return add
}

func getTestRemove(offsetMillis int64, path string) *Remove {
	remove := new(Remove)
	remove.Path = path
	size := int64(100)
	remove.Size = &size
	remove.DataChange = true
	partitionValues := make(map[string]string)
	remove.PartitionValues = &partitionValues
	deletionTimestamp := time.Now().UnixMilli() - offsetMillis
	remove.DeletionTimestamp = &deletionTimestamp
	return remove
}

func testDoCommit(t *testing.T, table *DeltaTable, actions []Action) (int64, error) {
	t.Helper()
	tx := table.CreateTransaction(&DeltaTransactionOptions{})
	tx.AddActions(actions)
	return tx.Commit(nil, nil)
}

func TestTombstones(t *testing.T) {
	store, state, lock, checkpointLock := setupCheckpointTest(t, "", false)
	checkpointConfiguration := NewCheckpointConfiguration()

	table := NewDeltaTable(store, lock, state)

	// Set tombstone expiry time to 2 hours
	metadata := NewDeltaTableMetaData("", "", Format{}, GetSchema(new(tombstonesTestData)), make([]string, 0), map[string]string{string(DeletedFileRetentionDurationDeltaConfigKey): "interval 2 hours"})
	protocol := new(Protocol).Default()
	table.Create(*metadata, protocol, CommitInfo{}, make([]Add, 0))
	add1 := getTestAdd(3 * 60 * 1000) // 3 mins ago
	add2 := getTestAdd(2 * 60 * 1000) // 2 mins ago
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
	_, err = CreateCheckpoint(store, checkpointLock, checkpointConfiguration, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Load the checkpoint
	// Remove the previous log to make sure we use the checkpoint when loading
	err = store.Delete(CommitUriFromVersion(1))
	if err != nil {
		t.Error(err)
	}
	// Reload table
	table, err = OpenTable(store, lock, state)
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
	add3 := getTestAdd(optimizeTime)
	add4 := getTestAdd(optimizeTime)
	v, err = testDoCommit(t, table, []Action{remove1, remove2, add3, add4})
	if err != nil {
		t.Fatal(err)
	}
	if v != 3 {
		t.Errorf("Version is %d, expected 3", v)
	}

	// Create a checkpoint and load it
	_, err = CreateCheckpoint(store, checkpointLock, checkpointConfiguration, 3)
	if err != nil {
		t.Fatal(err)
	}
	table, err = OpenTable(store, lock, state)
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

	table := NewDeltaTable(store, lock, state)

	metadata := NewDeltaTableMetaData("", "", Format{}, GetSchema(new(tombstonesTestData)), make([]string, 0), map[string]string{string(DeletedFileRetentionDurationDeltaConfigKey): "interval 1 minute"})
	protocol := new(Protocol).Default()
	table.Create(*metadata, protocol, CommitInfo{}, make([]Add, 0))
	add1 := getTestAdd(3 * 60 * 1000) // 3 mins ago
	add2 := getTestAdd(2 * 60 * 1000) // 2 mins ago
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
	_, err = CreateCheckpoint(store, checkpointLock, checkpointConfiguration, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Load the checkpoint
	// Reload table
	table, err = OpenTable(store, lock, state)
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
	add3 := getTestAdd(optimizeTime)
	add4 := getTestAdd(optimizeTime)
	v, err = testDoCommit(t, table, []Action{remove1, remove2, add3, add4})
	if err != nil {
		t.Fatal(err)
	}
	if v != 3 {
		t.Errorf("Version is %d, expected 3", v)
	}

	// Create a checkpoint and load it
	_, err = CreateCheckpoint(store, checkpointLock, checkpointConfiguration, 3)
	if err != nil {
		t.Fatal(err)
	}
	table, err = OpenTable(store, lock, state)
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

func TestCheckpointNoPartition(t *testing.T) {
	store, stateStore, lock, checkpointLock := setupCheckpointTest(t, "", false)
	checkpointConfiguration := NewCheckpointConfiguration()

	table := NewDeltaTable(store, lock, stateStore)

	metadata := NewDeltaTableMetaData("", "", Format{}, GetSchema(new(tombstonesTestData)), make([]string, 0), map[string]string{string(DeletedFileRetentionDurationDeltaConfigKey): "interval 1 minute"})
	protocol := new(Protocol).Default()
	table.Create(*metadata, protocol, CommitInfo{}, make([]Add, 0))
	add1 := getTestAdd(3 * 60 * 1000) // 3 mins ago
	add2 := getTestAdd(2 * 60 * 1000) // 2 mins ago
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
	_, err = CreateCheckpoint(store, checkpointLock, checkpointConfiguration, 2)
	if err != nil {
		t.Fatal(err)
	}

	// Load the checkpoint - don't use OpenTable since it will fall back to incremental if checkpoint read fails
	var version int64 = 2
	checkpoints, _, err := table.findLatestCheckpointsForVersion(&version)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) == 0 {
		t.Fatal("did not find checkpoint")
	}

	err = table.restoreCheckpoint(&checkpoints[len(checkpoints)-1])
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

	add1.DataChange = false
	if !reflect.DeepEqual(table.State.Files[add1.Path], *add1) {
		t.Errorf("Expected %v found %v", add1, table.State.Files[add1.Path])
	}
}

func TestMultiPartCheckpoint(t *testing.T) {
	store, stateStore, lock, checkpointLock := setupCheckpointTest(t, "", false)
	checkpointConfiguration := NewCheckpointConfiguration()
	checkpointConfiguration.MaxRowsPerPart = 5

	table := NewDeltaTable(store, lock, stateStore)

	provider := "tester"
	options := map[string]string{"hello": "world"}
	metadata := NewDeltaTableMetaData("test-data", "For testing multi-part checkpoints", Format{Provider: provider, Options: options},
		SchemaTypeStruct{}, make([]string, 0), map[string]string{"delta.isTest": "true"})
	protocol := new(Protocol).Default()
	table.Create(*metadata, protocol, CommitInfo{}, make([]Add, 0))
	paths := make([]string, 0, 10)
	// Commit ten Add actions
	for i := 0; i < 10; i++ {
		add := getTestAdd(60 * 1000)
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
	appId := "testApp"
	txn.AppId = appId
	lastUpdated := int64(time.Now().UnixMilli())
	txn.LastUpdated = &lastUpdated
	txnVersion := v
	txn.Version = txnVersion
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
	_, err = CreateCheckpoint(store, checkpointLock, checkpointConfiguration, 12)
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
	table = NewDeltaTable(store, lock, stateStore)
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
		if lastCheckpoint.Parts == nil {
			t.Error("last checkpoint parts count is nil, expected 3")
		} else if *lastCheckpoint.Parts != 3 {
			t.Errorf("last checkpoint parts count is %d, expected 3", *lastCheckpoint.Parts)
		}
	}

	// Remove the previous commit to make sure we load the checkpoint files
	err = store.Delete(CommitUriFromVersion(11))
	if err != nil {
		t.Error(err)
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
			if version != txn.Version {
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
		t.Errorf("Expected version %d, found %d", 12, table.State.Version)
	}
}

func TestCheckpointInfoFromURI(t *testing.T) {
	type test struct {
		input          string
		wantCheckpoint *CheckPoint
		wantPart       int32
	}

	part63 := int32(63)

	tests := []test{
		{input: "_delta_log/00000000000000000000.json", wantCheckpoint: nil},
		{input: "_delta_log/01234567890123456789.json", wantCheckpoint: nil},
		{input: "_delta_log/_commit_aabbccdd-eeff-1122-3344-556677889900.json.tmp", wantCheckpoint: nil},
		{input: "_delta_log/00000000000000000001.checkpoint.parquet", wantCheckpoint: &CheckPoint{Version: 1, Size: 0, Parts: nil}, wantPart: 0},
		{input: "_delta_log/00000000000000123456.checkpoint.0000000002.0000000063.parquet", wantCheckpoint: &CheckPoint{Version: 123456, Size: 0, Parts: &part63}, wantPart: 2},
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

		if !reflect.DeepEqual(*gotCheckpoint, *tc.wantCheckpoint) {
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
	_, err = CreateCheckpoint(store, checkpointLock, checkpointConfiguration, 5)
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
	_, err = CreateCheckpoint(store, checkpointLock, checkpointConfiguration, 10)
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
	_, err := CreateCheckpoint(store, checkpointLock, checkpointConfiguration, 5)
	if err != nil {
		t.Fatal(err)
	}

	// Create a checkpoint at version 10
	_, err = CreateCheckpoint(store, checkpointLock, checkpointConfiguration, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Replace the version 10 checkpoint with an invalid file
	err = store.Put(storage.NewPath("_delta_log/00000000000000000010.checkpoint.parquet"), []byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	// Open table; _last_checkpoint is pointing to an invalid checkpoint now
	table, err := OpenTable(store, lock, state)
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
	table, err = OpenTable(store, lock, state)
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
	table, err = OpenTable(store, lock, state)
	if err != nil {
		t.Fatal(err)
	}
	// Make sure we still loaded the last version
	if table.State.Version != 12 {
		t.Errorf("expected version %d, found %d", 12, table.State.Version)
	}
}

// / Check cleanup removes logs if enabled and doesn't if disabled
func TestCheckpointCleanupExpiredLogs(t *testing.T) {
	tests := []bool{
		true,
		false,
	}

	for _, enableCleanupInTableConfig := range tests {
		for _, disableCleanupInCheckpointConfig := range tests {
			store, stateStore, lock, checkpointLock := setupCheckpointTest(t, "", false)

			table := NewDeltaTable(store, lock, stateStore)
			// Use log expiration of 10 minutes
			table.Create(DeltaTableMetaData{Configuration: map[string]string{string(LogRetentionDurationDeltaConfigKey): "interval 10 minutes", string(EnableExpiredLogCleanupDeltaConfigKey): strconv.FormatBool(enableCleanupInTableConfig)}}, new(Protocol).Default(), CommitInfo{}, []Add{})

			add1 := getTestAdd(3 * 60 * 1000) // 3 mins ago
			add2 := getTestAdd(2 * 60 * 1000) // 2 mins ago
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
			now := time.Now()
			// With cleanup enabled, 25 and 15 minutes ago should be deleted, 5 should not
			err = os.Chtimes(filepath.Join(store.BaseURI.Raw, CommitUriFromVersion(0).Raw), now.Add(-25*time.Minute), now.Add(-25*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			err = os.Chtimes(filepath.Join(store.BaseURI.Raw, CommitUriFromVersion(1).Raw), now.Add(-15*time.Minute), now.Add(-15*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			err = os.Chtimes(filepath.Join(store.BaseURI.Raw, CommitUriFromVersion(2).Raw), now.Add(-5*time.Minute), now.Add(-5*time.Minute))
			if err != nil {
				t.Fatal(err)
			}

			_, err = OpenTableWithVersion(store, lock, stateStore, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, err = OpenTableWithVersion(store, lock, stateStore, 1)
			if err != nil {
				t.Fatal(err)
			}
			_, err = OpenTableWithVersion(store, lock, stateStore, 2)
			if err != nil {
				t.Fatal(err)
			}

			checkpointConfiguration := NewCheckpointConfiguration()
			checkpointConfiguration.DisableCleanup = disableCleanupInCheckpointConfig

			ok, err := table.CreateCheckpoint(checkpointLock, checkpointConfiguration, 2)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				t.Fatal("unable to create checkpoint")
			}

			shouldCleanup := enableCleanupInTableConfig && !disableCleanupInCheckpointConfig

			// Check cleanup results
			var version int64 = 0
			err = table.LoadVersion(&version)
			if shouldCleanup {
				if !errors.Is(err, ErrorInvalidVersion) {
					t.Fatal("did not remove version 0")
				}
			} else {
				if errors.Is(err, ErrorInvalidVersion) {
					t.Fatal("should not remove version 0")
				}
				if err != nil {
					t.Errorf("unexpected error %v", err)
				}
			}
			version = 1
			err = table.LoadVersion(&version)
			if shouldCleanup {
				if !errors.Is(err, ErrorInvalidVersion) {
					t.Fatal("did not remove version 1")
				}
			} else {
				if errors.Is(err, ErrorInvalidVersion) {
					t.Fatal("should not remove version 1")
				}
				if err != nil {
					t.Errorf("unexpected error %v", err)
				}
			}
			version = 2
			err = table.LoadVersion(&version)
			if errors.Is(err, ErrorInvalidVersion) {
				t.Fatal("unable to load version 2")
			}
			if err != nil {
				t.Errorf("unexpected error %v", err)
			}
		}
	}
}

// / Test with times requiring adjustment
// / Based on the scenario described in the comments for BufferingLogDeletionIterator at
// / https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/DeltaHistoryManager.scala
func TestCheckpointCleanupTimeAdjustment(t *testing.T) {
	store, stateStore, lock, checkpointLock := setupCheckpointTest(t, "", false)

	table := NewDeltaTable(store, lock, stateStore)
	// Use log expiration of 12 minutes
	table.Create(DeltaTableMetaData{Configuration: map[string]string{string(LogRetentionDurationDeltaConfigKey): "interval 11 minutes", string(EnableExpiredLogCleanupDeltaConfigKey): "true"}}, Protocol{}, CommitInfo{}, []Add{})

	add1 := getTestAdd(20 * 60 * 1000) // 20 mins ago
	add2 := getTestAdd(19 * 60 * 1000) // 19 mins ago
	add3 := getTestAdd(18 * 60 * 1000) // 18 mins ago
	add4 := getTestAdd(17 * 60 * 1000) // 17 mins ago
	add5 := getTestAdd(16 * 60 * 1000) // 16 mins ago
	_, err := testDoCommit(t, table, []Action{add1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = testDoCommit(t, table, []Action{add2})
	if err != nil {
		t.Fatal(err)
	}
	_, err = testDoCommit(t, table, []Action{add3})
	if err != nil {
		t.Fatal(err)
	}
	_, err = testDoCommit(t, table, []Action{add4})
	if err != nil {
		t.Fatal(err)
	}
	v, err := testDoCommit(t, table, []Action{add5})
	if err != nil {
		t.Fatal(err)
	}
	if v != 5 {
		t.Fatalf("expected version %d found %d", 5, v)
	}

	now := time.Now()
	// Set last updated times for each version to:
	// 0: 20 min ago
	// 1: 15 min ago
	// 2: 10 min ago
	// 3: 13 min ago
	// 4: 12 min ago
	// 5: 6 min ago
	err = os.Chtimes(filepath.Join(store.BaseURI.Raw, CommitUriFromVersion(0).Raw), now.Add(-20*time.Minute), now.Add(-20*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	err = os.Chtimes(filepath.Join(store.BaseURI.Raw, CommitUriFromVersion(1).Raw), now.Add(-15*time.Minute), now.Add(-15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	err = os.Chtimes(filepath.Join(store.BaseURI.Raw, CommitUriFromVersion(2).Raw), now.Add(-10*time.Minute), now.Add(-10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	err = os.Chtimes(filepath.Join(store.BaseURI.Raw, CommitUriFromVersion(3).Raw), now.Add(-13*time.Minute), now.Add(-13*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	err = os.Chtimes(filepath.Join(store.BaseURI.Raw, CommitUriFromVersion(3).Raw), now.Add(-12*time.Minute), now.Add(-12*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	err = os.Chtimes(filepath.Join(store.BaseURI.Raw, CommitUriFromVersion(3).Raw), now.Add(-6*time.Minute), now.Add(-6*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	ok, err := table.CreateCheckpoint(checkpointLock, NewCheckpointConfiguration(), 5)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("unable to create checkpoint")
	}

	// Even though we checkpointed at version 5, and expiry is set to 11 minutes (covering versions 0-4),
	// because of the time adjustment we should only have removed versions 0 and 1
	var version int64 = 0
	err = table.LoadVersion(&version)
	if !errors.Is(err, ErrorInvalidVersion) {
		t.Fatal("did not remove version 0")
	}
	version = 1
	err = table.LoadVersion(&version)
	if !errors.Is(err, ErrorInvalidVersion) {
		t.Fatal("did not remove version 1")
	}
	// We can't load versions 2 and 3 but the logs should persist
	_, err = store.Head(CommitUriFromVersion(2))
	if errors.Is(err, storage.ErrorObjectDoesNotExist) {
		t.Fatal("should not remove version 2")
	}
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Head(CommitUriFromVersion(3))
	if errors.Is(err, storage.ErrorObjectDoesNotExist) {
		t.Fatal("should not remove version 3")
	}
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Head(CommitUriFromVersion(4))
	if errors.Is(err, storage.ErrorObjectDoesNotExist) {
		t.Fatal("should not remove version 4")
	}
	if err != nil {
		t.Fatal(err)
	}

	version = 5
	err = table.LoadVersion(&version)
	if errors.Is(err, ErrorInvalidVersion) {
		t.Fatal("should not remove version 5")
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointLocked(t *testing.T) {
	store, _, _, checkpointLock := setupCheckpointTest(t, "testdata/checkpoints", false)

	locked, err := checkpointLock.TryLock()
	if err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("unable to obtain lock")
	}

	localLock := filelock.New(store.BaseURI, "_delta_log/_checkpoint.lock", filelock.Options{})

	checkpointed, err := CreateCheckpoint(store, localLock, NewCheckpointConfiguration(), 5)
	if !errors.Is(err, lock.ErrorLockNotObtained) {
		t.Fatalf("expected ErrorLockNotObtained when calling checkpoint with lock already in use, got %v", err)
	}
	if checkpointed {
		t.Fatal("should not create checkpoint with lock in use")
	}

	err = checkpointLock.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	checkpointed, err = CreateCheckpoint(store, localLock, NewCheckpointConfiguration(), 5)
	if err != nil {
		t.Fatalf("unexpected error creating checkpoint %v", err)
	}
	if !checkpointed {
		t.Fatal("did not create checkpoint")
	}
}

func TestCheckpointUnlockFailure(t *testing.T) {
	store, _, _, _ := setupCheckpointTest(t, "testdata/checkpoints", false)
	brokenLock := testBrokenUnlockLocker{*filelock.New(store.BaseURI, "_delta_log/_commit.lock", filelock.Options{TTL: 60 * time.Second})}

	checkpointed, err := CreateCheckpoint(store, &brokenLock, NewCheckpointConfiguration(), 5)
	if !errors.Is(err, lock.ErrorUnableToUnlock) {
		t.Fatalf("expected ErrorUnableToUnlock when calling checkpoint with broken test lock, got %v", err)
	}
	if !checkpointed {
		t.Fatal("did not create checkpoint")
	}
}

func TestCheckpointInvalidVersion(t *testing.T) {
	store, stateStore, lock, checkpointLock := setupCheckpointTest(t, "", false)

	table := NewDeltaTable(store, lock, stateStore)

	metadata := NewDeltaTableMetaData("", "", Format{}, GetSchema(new(tombstonesTestData)), make([]string, 0), map[string]string{string(DeletedFileRetentionDurationDeltaConfigKey): "interval 1 minute"})
	protocol := new(Protocol).Default()
	protocol.MinReaderVersion = 2
	protocol.MinWriterVersion = 2
	err := table.Create(*metadata, protocol, CommitInfo{}, make([]Add, 0))
	if !errors.Is(err, ErrorUnsupportedReaderVersion) || !errors.Is(err, ErrorUnsupportedWriterVersion) {
		t.Error("should return unsupported reader/writer version errors")
		if err != nil {
			t.Error(err)
		}
	}
	add1 := getTestAdd(3 * 60 * 1000) // 3 mins ago
	add2 := getTestAdd(2 * 60 * 1000) // 2 mins ago
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

	// Create a checkpoint with default configuration - should fail
	configuration := NewCheckpointConfiguration()
	checkpointed, err := CreateCheckpoint(store, checkpointLock, configuration, 2)
	if !errors.Is(err, ErrorUnsupportedReaderVersion) || !errors.Is(err, ErrorUnsupportedWriterVersion) {
		t.Error("should return unsupported reader/writer version errors")
	}
	if checkpointed {
		t.Error("should not create checkpoint with default configuration")
	}

	// Create a checkpoint with unsafe ignore option set in configuration - should succeed
	configuration.UnsafeIgnoreUnsupportedReaderWriterVersionErrors = true
	checkpointed, err = CreateCheckpoint(store, checkpointLock, configuration, 2)
	if err != nil {
		t.Error("should not return an error")
	}
	if !checkpointed {
		t.Error("should create checkpoint with modified configuration")
	}
}
