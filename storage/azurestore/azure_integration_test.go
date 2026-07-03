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

//go:build azure_integration

// This file holds manual integration tests that write to a real ADLS Gen2 account.
// They never run as part of the normal test suite. To run them:
//
//	export AZURE_STORAGE_ACCOUNT=<your-account>
//	export AZURE_CONTAINER=<your-filesystem>
//	export AZURE_PATH=delta-go-itest          # optional, defaults to delta-go-itest
//	go test -tags azure_integration ./storage/azurestore/ -run Integration -v
//
// Authentication uses azidentity.DefaultAzureCredential (env vars, managed identity,
// or `az login`). The target account must have hierarchical namespace (ADLS Gen2) enabled.
package azurestore

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/chelseajonesr/rfarrow"
	"github.com/google/uuid"
	delta "github.com/rivian/delta-go"
	"github.com/rivian/delta-go/lock/nillock"
	"github.com/rivian/delta-go/state/localstate"
	"github.com/rivian/delta-go/storage"
)

type itestRow struct {
	Id    int64   `parquet:"name=id"`
	Label string  `parquet:"name=label, converted=UTF8"`
	Value float64 `parquet:"name=value"`
}

func itestSchema() delta.SchemaTypeStruct {
	return delta.SchemaTypeStruct{
		Fields: []delta.SchemaField{
			{Name: "id", Type: delta.Long, Nullable: false, Metadata: make(map[string]any)},
			{Name: "label", Type: delta.String, Nullable: false, Metadata: make(map[string]any)},
			{Name: "value", Type: delta.Double, Nullable: false, Metadata: make(map[string]any)},
		},
	}
}

func makeParquetData(n int) ([]byte, error) {
	rows := make([]itestRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, itestRow{Id: int64(i), Label: uuid.NewString(), Value: rand.Float64()})
	}
	buf := new(bytes.Buffer)
	props := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	if err := rfarrow.WriteGoStructsToParquet(rows, buf, props); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func makeParquet(t *testing.T, n int) []byte {
	t.Helper()
	data, err := makeParquetData(n)
	if err != nil {
		t.Fatalf("failed to write parquet: %v", err)
	}
	return data
}

// newIntegrationStore builds an AzureObjectStore from environment configuration,
// skipping the test if the required variables are not present.
func newIntegrationStore(t *testing.T) (*AzureObjectStore, storage.Path) {
	t.Helper()
	account := os.Getenv("AZURE_STORAGE_ACCOUNT")
	container := os.Getenv("AZURE_CONTAINER")
	if account == "" || container == "" {
		t.Skip("AZURE_STORAGE_ACCOUNT and AZURE_CONTAINER must be set for the Azure integration test")
	}
	path := os.Getenv("AZURE_PATH")
	if path == "" {
		path = "delta-go-itest"
	}
	// Use a unique subfolder per run so repeated runs do not collide.
	path = fmt.Sprintf("%s/%d", path, time.Now().UnixNano())

	baseURI := storage.NewPath(fmt.Sprintf("az://%s/%s", container, path))

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		t.Fatalf("failed to create DefaultAzureCredential: %v", err)
	}
	store, err := NewWithCredential(baseURI, cred, nil)
	if err != nil {
		t.Fatalf("failed to create AzureObjectStore: %v", err)
	}
	return store, baseURI
}

// TestIntegrationObjectStore exercises the raw object-store operations against a
// real ADLS Gen2 location: Put, Get, Head, List, Rename, and atomic RenameIfNotExists.
func TestIntegrationObjectStore(t *testing.T) {
	store, baseURI := newIntegrationStore(t)
	t.Logf("writing to %s", baseURI.Raw)

	src := storage.NewPath("smoke/source.txt")
	data := []byte("hello adls gen2")
	if err := store.Put(src, data); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	got, err := store.Get(src)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get returned %q, want %q", got, data)
	}

	meta, err := store.Head(src)
	if err != nil {
		t.Fatalf("Head failed: %v", err)
	}
	if meta.Size != int64(len(data)) {
		t.Fatalf("Head size = %d, want %d", meta.Size, len(data))
	}

	result, err := store.ListAll(storage.NewPath("smoke/"))
	if err != nil {
		t.Fatalf("ListAll failed: %v", err)
	}
	if len(result.Objects) == 0 {
		t.Fatalf("ListAll returned no objects")
	}

	// Atomic rename into a new location.
	dst := storage.NewPath("smoke/dest.txt")
	if err := store.RenameIfNotExists(src, dst); err != nil {
		t.Fatalf("RenameIfNotExists failed: %v", err)
	}

	// A second rename into the same destination must fail atomically.
	other := storage.NewPath("smoke/other.txt")
	if err := store.Put(other, []byte("other")); err != nil {
		t.Fatalf("Put other failed: %v", err)
	}
	if err := store.RenameIfNotExists(other, dst); err == nil {
		t.Fatalf("expected RenameIfNotExists to fail for an existing destination")
	}

	if err := store.DeleteFolder(storage.NewPath("smoke/")); err != nil {
		t.Fatalf("DeleteFolder failed: %v", err)
	}
}

// TestIntegrationDeltaTableWrite creates a Delta table at a real ADLS Gen2 location,
// writes a parquet data file, and commits a transaction adding it to the table.
func TestIntegrationDeltaTableWrite(t *testing.T) {
	store, baseURI := newIntegrationStore(t)
	t.Logf("creating delta table at %s", baseURI.Raw)

	table := delta.NewTable(store, nillock.New(), localstate.New(-1))
	metadata := delta.NewTableMetaData("delta-go azure itest", "integration test table",
		new(delta.Format).Default(), itestSchema(), []string{}, make(map[string]string))
	if err := table.Create(*metadata, new(delta.Protocol).Default(), delta.CommitInfo{}, []delta.Add{}); err != nil {
		t.Fatalf("table.Create failed: %v", err)
	}

	// Write a parquet data file into the table directory.
	fileName := fmt.Sprintf("part-%s.snappy.parquet", uuid.NewString())
	if err := store.Put(storage.NewPath(fileName), makeParquet(t, 5)); err != nil {
		t.Fatalf("writing parquet failed: %v", err)
	}

	add, _, err := delta.NewAdd(store, storage.NewPath(fileName), make(map[string]string))
	if err != nil {
		t.Fatalf("delta.NewAdd failed: %v", err)
	}

	transaction := table.CreateTransaction(delta.NewTransactionOptions())
	transaction.AddAction(add)
	transaction.SetOperation(delta.Write{Mode: delta.Append})
	version, err := transaction.Commit()
	if err != nil {
		t.Fatalf("transaction.Commit failed: %v", err)
	}
	t.Logf("committed version %d", version)

	// Verify the commit log entry exists.
	logEntry := storage.NewPath(fmt.Sprintf("_delta_log/%020d.json", version))
	if _, err := store.Head(logEntry); err != nil {
		t.Fatalf("expected commit log %s to exist: %v", logEntry.Raw, err)
	}
}

// TestIntegrationConcurrentWrites hammers a single Delta table with many writers
// committing in parallel. Each writer is an independent table instance using
// nillock, so the ONLY thing serializing the commits is the storage layer's atomic
// RenameIfNotExists. It asserts that every writer won a distinct, contiguous version
// (no lost updates, no duplicate versions, no gaps) — proving ADLS Gen2 atomic rename
// correctly arbitrates concurrent commits.
//
// Tune the writer count with AZURE_ITEST_WRITERS (default 50). Set AZURE_ITEST_KEEP=1
// to leave the table on the account for inspection.
func TestIntegrationConcurrentWrites(t *testing.T) {
	store, baseURI := newIntegrationStore(t)

	numWriters := 50
	if v := os.Getenv("AZURE_ITEST_WRITERS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			numWriters = parsed
		}
	}
	t.Logf("hammering %s with %d concurrent writers", baseURI.Raw, numWriters)

	if os.Getenv("AZURE_ITEST_KEEP") == "" {
		t.Cleanup(func() {
			if err := store.DeleteFolder(storage.NewPath("")); err != nil {
				t.Logf("cleanup: failed to delete table folder: %v", err)
			}
		})
	}

	// Create the table (version 0).
	table := delta.NewTable(store, nillock.New(), localstate.New(-1))
	metadata := delta.NewTableMetaData("delta-go azure concurrency itest", "parallel write stress test",
		new(delta.Format).Default(), itestSchema(), []string{}, make(map[string]string))
	if err := table.Create(*metadata, new(delta.Protocol).Default(), delta.CommitInfo{}, []delta.Add{}); err != nil {
		t.Fatalf("table.Create failed: %v", err)
	}

	type result struct {
		version int64
		err     error
	}
	results := make([]result, numWriters)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // release all writers at once to maximize contention

			// Each writer is fully independent: its own table, lock, and state store.
			writerTable := delta.NewTable(store, nillock.New(), localstate.New(-1))
			opts := delta.NewTransactionOptions()
			opts.RetryWaitDuration = 25 * time.Millisecond
			txn := writerTable.CreateTransaction(opts)

			data, err := makeParquetData(1 + rand.Intn(5))
			if err != nil {
				results[i] = result{err: fmt.Errorf("parquet: %w", err)}
				return
			}
			fileName := fmt.Sprintf("part-%s.snappy.parquet", uuid.NewString())
			if err := store.Put(storage.NewPath(fileName), data); err != nil {
				results[i] = result{err: fmt.Errorf("put: %w", err)}
				return
			}
			add, _, err := delta.NewAdd(store, storage.NewPath(fileName), make(map[string]string))
			if err != nil {
				results[i] = result{err: fmt.Errorf("newAdd: %w", err)}
				return
			}
			txn.AddAction(add)
			txn.SetOperation(delta.Write{Mode: delta.Append})
			v, err := txn.Commit()
			results[i] = result{version: v, err: err}
		}(i)
	}

	commitStart := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(commitStart)

	// Every writer must succeed and win a distinct version.
	seen := make(map[int64]int)
	var failures int
	for i, r := range results {
		if r.err != nil {
			failures++
			t.Errorf("writer %d failed: %v", i, r.err)
			continue
		}
		if prev, dup := seen[r.version]; dup {
			t.Errorf("DUPLICATE version %d won by writers %d and %d (atomicity violated)", r.version, prev, i)
		}
		seen[r.version] = i
	}
	if failures > 0 {
		t.Fatalf("%d/%d writers failed", failures, numWriters)
	}

	// The won versions must be exactly the contiguous range [1, numWriters].
	wonVersions := make([]int64, 0, len(seen))
	for v := range seen {
		wonVersions = append(wonVersions, v)
	}
	sort.Slice(wonVersions, func(a, b int) bool { return wonVersions[a] < wonVersions[b] })
	for idx, v := range wonVersions {
		if v != int64(idx+1) {
			t.Fatalf("non-contiguous committed versions: got %v, expected 1..%d", wonVersions, numWriters)
		}
	}
	t.Logf("all %d writers committed distinct versions 1..%d in %s", numWriters, numWriters, elapsed)

	// Cross-check against what actually landed in _delta_log on the account.
	listResult, err := store.ListAll(storage.NewPath("_delta_log/"))
	if err != nil {
		t.Fatalf("ListAll(_delta_log) failed: %v", err)
	}
	commitVersions := make([]int64, 0, numWriters+1)
	for _, o := range listResult.Objects {
		name := path.Base(o.Location.Raw)
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		raw := strings.TrimSuffix(name, ".json")
		if parsed, perr := strconv.ParseInt(raw, 10, 64); perr == nil {
			commitVersions = append(commitVersions, parsed)
		}
	}
	sort.Slice(commitVersions, func(a, b int) bool { return commitVersions[a] < commitVersions[b] })
	if len(commitVersions) != numWriters+1 {
		t.Fatalf("_delta_log has %d commit files, expected %d (versions: %v)", len(commitVersions), numWriters+1, commitVersions)
	}
	for idx, v := range commitVersions {
		if v != int64(idx) {
			t.Fatalf("_delta_log commit versions not contiguous from 0: got %v", commitVersions)
		}
	}
	t.Logf("_delta_log contains contiguous commits 0..%d (%d files) — atomic rename held under load",
		numWriters, len(commitVersions))
}

// TestIntegrationCheckpointAndPrefixList exercises the on-disk (streaming)
// checkpoint path and the S3-style prefix semantics of List against a real ADLS
// Gen2 (HNS) account. It is a regression test for the HNS ListPaths behavior:
// the dfs List API lists the contents of an existing directory and 404s
// otherwise, but delta-go calls List with file-name prefixes (the checkpoint
// working folder, and "_delta_log/<version>" in DoesCheckpointVersionExist).
// Before the fix those prefix lists 404'd and aborted every checkpoint.
func TestIntegrationCheckpointAndPrefixList(t *testing.T) {
	store, baseURI := newIntegrationStore(t)
	t.Logf("checkpoint itest table at %s", baseURI.Raw)

	if os.Getenv("AZURE_ITEST_KEEP") == "" {
		t.Cleanup(func() {
			if err := store.DeleteFolder(storage.NewPath("")); err != nil {
				t.Logf("cleanup: failed to delete table folder: %v", err)
			}
		})
	}

	// Create the table (version 0) and commit a few data files so there is
	// non-trivial state to checkpoint.
	table := delta.NewTable(store, nillock.New(), localstate.New(-1))
	metadata := delta.NewTableMetaData("delta-go azure checkpoint itest", "checkpoint + prefix list regression",
		new(delta.Format).Default(), itestSchema(), []string{}, make(map[string]string))
	if err := table.Create(*metadata, new(delta.Protocol).Default(), delta.CommitInfo{}, []delta.Add{}); err != nil {
		t.Fatalf("table.Create failed: %v", err)
	}

	const commits = 3
	for i := 0; i < commits; i++ {
		fileName := fmt.Sprintf("part-%s.snappy.parquet", uuid.NewString())
		if err := store.Put(storage.NewPath(fileName), makeParquet(t, 5)); err != nil {
			t.Fatalf("writing parquet failed: %v", err)
		}
		add, _, err := delta.NewAdd(store, storage.NewPath(fileName), make(map[string]string))
		if err != nil {
			t.Fatalf("delta.NewAdd failed: %v", err)
		}
		txn := table.CreateTransaction(delta.NewTransactionOptions())
		txn.AddAction(add)
		txn.SetOperation(delta.Write{Mode: delta.Append})
		if _, err := txn.Commit(); err != nil {
			t.Fatalf("commit %d failed: %v", i, err)
		}
	}

	version := table.State.Version
	t.Logf("table at version %d", version)

	// Sanity: a file-name prefix that is NOT a directory must return the
	// matching files (S3 prefix semantics), not a 404. Before the fix this
	// returned an error / empty on HNS.
	verPrefix := storage.NewPath(fmt.Sprintf("_delta_log/%020d", version))
	preList, err := store.ListAll(verPrefix)
	if err != nil {
		t.Fatalf("ListAll(%s) failed: %v", verPrefix.Raw, err)
	}
	var sawCommitJSON bool
	for _, o := range preList.Objects {
		if strings.HasSuffix(o.Location.Raw, fmt.Sprintf("%020d.json", version)) {
			sawCommitJSON = true
		}
	}
	if !sawCommitJSON {
		t.Fatalf("prefix List(%s) did not return the commit json; got %d objects: %v",
			verPrefix.Raw, len(preList.Objects), objectLocations(preList))
	}

	// A prefix under a non-existent directory must be an empty listing, not an
	// error.
	missing, err := store.ListAll(storage.NewPath("_delta_log/.tmp/nope-does-not-exist"))
	if err != nil {
		t.Fatalf("ListAll(missing) should be empty, got error: %v", err)
	}
	if len(missing.Objects) != 0 {
		t.Fatalf("ListAll(missing) returned %d objects, want 0", len(missing.Objects))
	}

	// Streaming (on-disk optimize) checkpoint — the path that failed on HNS
	// because it lists a freshly-generated, not-yet-existing working folder.
	optConfig, err := delta.NewOptimizeCheckpointConfiguration(store, version)
	if err != nil {
		t.Fatalf("NewOptimizeCheckpointConfiguration failed: %v", err)
	}
	cpConfig := delta.NewCheckpointConfiguration()
	cpConfig.ReadWriteConfiguration = *optConfig
	created, err := table.CreateCheckpoint(nillock.New(), cpConfig, version)
	if err != nil {
		t.Fatalf("CreateCheckpoint failed: %v", err)
	}
	if !created {
		t.Fatalf("CreateCheckpoint returned created=false")
	}

	// The checkpoint parquet and the _last_checkpoint pointer must now exist.
	checkpointFile := storage.NewPath(fmt.Sprintf("_delta_log/%020d.checkpoint.parquet", version))
	if _, err := store.Head(checkpointFile); err != nil {
		t.Fatalf("expected checkpoint %s to exist: %v", checkpointFile.Raw, err)
	}
	if _, err := store.Head(storage.NewPath("_delta_log/_last_checkpoint")); err != nil {
		t.Fatalf("expected _last_checkpoint to exist: %v", err)
	}

	// With correct prefix List, DoesCheckpointVersionExist must now find it.
	exists, err := delta.DoesCheckpointVersionExist(store, version, false)
	if err != nil {
		t.Fatalf("DoesCheckpointVersionExist failed: %v", err)
	}
	if !exists {
		t.Fatalf("DoesCheckpointVersionExist(%d) = false, want true", version)
	}

	// The working folder under _delta_log/.tmp/ must have been cleaned up.
	tmp, err := store.ListAll(storage.NewPath("_delta_log/.tmp"))
	if err != nil {
		t.Fatalf("ListAll(_delta_log/.tmp) failed: %v", err)
	}
	if len(tmp.Objects) != 0 {
		t.Fatalf("checkpoint working folder not cleaned up; _delta_log/.tmp has %d objects: %v",
			len(tmp.Objects), objectLocations(tmp))
	}

	t.Logf("checkpoint at version %d written and prefix List verified", version)
}

func objectLocations(res storage.ListResult) []string {
	out := make([]string, 0, len(res.Objects))
	for _, o := range res.Objects {
		out = append(out, o.Location.Raw)
	}
	return out
}

