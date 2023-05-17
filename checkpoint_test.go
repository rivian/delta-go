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
	"io"
	"os"
	"path/filepath"
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
func setupCheckpointTest(t *testing.T, inputFolder string) (store storage.ObjectStore, state state.StateStore, lock lock.Locker, checkpointLock lock.Locker) {
	t.Helper()

	tmpDir := t.TempDir()
	tmpPath := storage.NewPath(tmpDir)
	store = filestore.New(tmpPath)
	state = filestate.New(storage.NewPath(tmpDir), "_delta_log/_commit.state")
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
	store, state, lock, checkpointLock := setupCheckpointTest(t, "testdata/checkpoints")

	// Create a checkpoint at version 5
	_, err := CreateCheckpoint[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, checkpointLock, 5)
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
	lastCheckpoint, err := table.GetLastCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if lastCheckpoint.Version != 5 {
		t.Errorf("last checkpoint version is %d, should be 5", lastCheckpoint.Version)
	}

	// Checkpoint at version 10
	_, err = CreateCheckpoint[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, checkpointLock, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Checkpoint file exists
	_, err = store.Head(storage.NewPath("_delta_log/00000000000000000010.checkpoint.parquet"))
	if err != nil {
		t.Fatal(err)
	}

	// Does _last_checkpoint point to the checkpoint file
	lastCheckpoint, err = table.GetLastCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if lastCheckpoint.Version != 10 {
		t.Errorf("last checkpoint version is %d, should be 10", lastCheckpoint.Version)
	}

	// Reload table
	table, err = OpenTable[SimpleCheckpointTestData, SimpleCheckpointTestPartition](store, lock, state)
	if err != nil {
		t.Fatal(err)
	}
	if len(table.State.Files) != 12 {
		t.Errorf("Found %d files, expected 12", len(table.State.Files))
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
	store, state, lock, checkpointLock := setupCheckpointTest(t, "")

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
	CreateCheckpoint[TombstonesTestData, SimpleCheckpointTestPartition](store, checkpointLock, 2)
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
	CreateCheckpoint[TombstonesTestData, SimpleCheckpointTestPartition](store, checkpointLock, 3)
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
	store, state, lock, checkpointLock := setupCheckpointTest(t, "")

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
	CreateCheckpoint[TombstonesTestData, SimpleCheckpointTestPartition](store, checkpointLock, 2)
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
	CreateCheckpoint[TombstonesTestData, SimpleCheckpointTestPartition](store, checkpointLock, 2)
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
