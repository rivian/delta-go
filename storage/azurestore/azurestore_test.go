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
package azurestore

import (
	"bytes"
	"errors"
	"io"
	"sort"
	"testing"

	"github.com/rivian/delta-go/internal/azureutils"
	"github.com/rivian/delta-go/storage"
)

// setupTest creates a mock ADLS Gen2 client and an AzureObjectStore.
func setupTest(t *testing.T) (baseURI storage.Path, mockClient *azureutils.MockClient, store *AzureObjectStore) {
	t.Helper()
	baseURI = storage.NewPath("az://test-container/test-delta-table")
	mockClient, err := azureutils.NewMockClient(t, baseURI)
	if err != nil {
		t.Fatalf("Error occurred setting up for tests %e.", err)
	}
	store, err = New(mockClient, baseURI)
	if err != nil {
		t.Fatalf("Error occurred setting up for tests %e.", err)
	}
	return
}

func verifyFileContents(t *testing.T, path storage.Path, mockClient *azureutils.MockClient, data []byte, msg string) {
	t.Helper()
	results, err := mockClient.GetFile(path)
	if err != nil {
		t.Errorf("Error occurred verifying file contents: %e (checking: %s)", err, msg)
	}
	if !bytes.Equal(results, data) {
		t.Errorf("Checking: %s. Results did not match expected. Results: %s, Expected: %s", msg, results, data)
	}
}

func TestParseAzureURI(t *testing.T) {
	tests := []struct {
		name          string
		uri           string
		wantAccount   string
		wantContainer string
		wantPath      string
		wantScheme    string
		wantErr       bool
	}{
		{name: "az scheme", uri: "az://my-container/tables/t1", wantContainer: "my-container", wantPath: "tables/t1", wantScheme: "az"},
		{name: "az scheme root", uri: "az://my-container", wantContainer: "my-container", wantPath: "", wantScheme: "az"},
		{name: "abfss scheme", uri: "abfss://fs@acct.dfs.core.windows.net/tables/t1", wantAccount: "acct", wantContainer: "fs", wantPath: "tables/t1", wantScheme: "abfss"},
		{name: "abfss missing container", uri: "abfss://acct.dfs.core.windows.net/tables/t1", wantErr: true},
		{name: "unsupported scheme", uri: "s3://bucket/path", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account, container, path, scheme, err := parseAzureURI(storage.NewPath(tt.uri))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %e", err)
			}
			if account != tt.wantAccount || container != tt.wantContainer || path != tt.wantPath || scheme != tt.wantScheme {
				t.Errorf("got (%s,%s,%s,%s), want (%s,%s,%s,%s)", account, container, path, scheme, tt.wantAccount, tt.wantContainer, tt.wantPath, tt.wantScheme)
			}
		})
	}
}

func TestPut(t *testing.T) {
	_, mockClient, store := setupTest(t)

	path := storage.NewPath("test.txt")
	data := []byte("data1")
	if err := store.Put(path, data); err != nil {
		t.Errorf("Error occurred calling Put: %e", err)
	}
	verifyFileContents(t, path, mockClient, data, "Put")

	data2 := []byte("data2")
	if err := store.Put(path, data2); err != nil {
		t.Errorf("Error occurred calling Put: %e", err)
	}
	verifyFileContents(t, path, mockClient, data2, "Put overwrite")
}

func TestPutErrorHandling(t *testing.T) {
	_, mockClient, store := setupTest(t)

	mockClient.MockError = errors.New("something went wrong")
	err := store.Put(storage.NewPath("test.txt"), []byte("data1"))
	if !errors.Is(err, storage.ErrPutObject) {
		t.Errorf("Expected ErrPutObject, got %v", err)
	}
}

func TestGet(t *testing.T) {
	_, mockClient, store := setupTest(t)

	path := storage.NewPath("test.txt")
	data := []byte("some data")
	if err := mockClient.PutFile(path, data); err != nil {
		t.Errorf("Error occurred setting up TestGet: %e", err)
	}
	results, err := store.Get(path)
	if err != nil {
		t.Errorf("Error occurred calling Get: %e", err)
	}
	if !bytes.Equal(results, data) {
		t.Errorf("Results did not match. Results: %s, Expected: %s", results, data)
	}
}

func TestGetDoesNotExist(t *testing.T) {
	_, _, store := setupTest(t)

	_, err := store.Get(storage.NewPath("missing.txt"))
	if !errors.Is(err, storage.ErrObjectDoesNotExist) {
		t.Errorf("Expected ErrObjectDoesNotExist, got %v", err)
	}
}

func TestHead(t *testing.T) {
	_, mockClient, store := setupTest(t)

	path := storage.NewPath("test.txt")
	data := []byte("some data")
	if err := mockClient.PutFile(path, data); err != nil {
		t.Errorf("Error occurred setting up TestHead: %e", err)
	}
	meta, err := store.Head(path)
	if err != nil {
		t.Errorf("Error occurred calling Head: %e", err)
	}
	if meta.Size != int64(len(data)) {
		t.Errorf("Expected size %d, got %d", len(data), meta.Size)
	}
	if meta.Location.Raw != path.Raw {
		t.Errorf("Expected location %s, got %s", path.Raw, meta.Location.Raw)
	}
}

func TestHeadDoesNotExist(t *testing.T) {
	_, _, store := setupTest(t)

	_, err := store.Head(storage.NewPath("missing.txt"))
	if !errors.Is(err, storage.ErrObjectDoesNotExist) {
		t.Errorf("Expected ErrObjectDoesNotExist, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	_, mockClient, store := setupTest(t)

	path := storage.NewPath("test.txt")
	if err := mockClient.PutFile(path, []byte("data")); err != nil {
		t.Errorf("Error occurred setting up TestDelete: %e", err)
	}
	if err := store.Delete(path); err != nil {
		t.Errorf("Error occurred calling Delete: %e", err)
	}
	if mockClient.FileExists(path) {
		t.Error("Expected file to be deleted")
	}
}

func TestRename(t *testing.T) {
	_, mockClient, store := setupTest(t)

	from := storage.NewPath("from.txt")
	to := storage.NewPath("to.txt")
	data := []byte("data")
	if err := mockClient.PutFile(from, data); err != nil {
		t.Errorf("Error occurred setting up TestRename: %e", err)
	}
	if err := store.Rename(from, to); err != nil {
		t.Errorf("Error occurred calling Rename: %e", err)
	}
	verifyFileContents(t, to, mockClient, data, "Rename destination")
	if mockClient.FileExists(from) {
		t.Error("Expected source to be removed after Rename")
	}
}

func TestRenameIfNotExists(t *testing.T) {
	_, mockClient, store := setupTest(t)

	from := storage.NewPath("from.txt")
	to := storage.NewPath("to.txt")
	if err := mockClient.PutFile(from, []byte("data")); err != nil {
		t.Errorf("Error occurred setting up TestRenameIfNotExists: %e", err)
	}
	if err := store.RenameIfNotExists(from, to); err != nil {
		t.Errorf("Error occurred calling RenameIfNotExists: %e", err)
	}

	// Now the destination exists; a second rename of a new source must fail.
	from2 := storage.NewPath("from2.txt")
	if err := mockClient.PutFile(from2, []byte("data2")); err != nil {
		t.Errorf("Error occurred setting up second source: %e", err)
	}
	err := store.RenameIfNotExists(from2, to)
	if !errors.Is(err, storage.ErrObjectAlreadyExists) {
		t.Errorf("Expected ErrObjectAlreadyExists, got %v", err)
	}
}

func TestListAll(t *testing.T) {
	_, mockClient, store := setupTest(t)

	paths := []string{"_delta_log/00000000000000000000.json", "_delta_log/00000000000000000001.json", "part-0.parquet"}
	for _, p := range paths {
		if err := mockClient.PutFile(storage.NewPath(p), []byte("x")); err != nil {
			t.Errorf("Error occurred setting up TestListAll: %e", err)
		}
	}

	result, err := store.ListAll(storage.NewPath(""))
	if err != nil {
		t.Errorf("Error occurred calling ListAll: %e", err)
	}

	got := make([]string, 0, len(result.Objects))
	for _, o := range result.Objects {
		got = append(got, o.Location.Raw)
	}
	sort.Strings(got)
	sort.Strings(paths)
	if len(got) != len(paths) {
		t.Fatalf("Expected %d objects, got %d (%v)", len(paths), len(got), got)
	}
	for i := range paths {
		if got[i] != paths[i] {
			t.Errorf("Expected %s, got %s", paths[i], got[i])
		}
	}
}

func TestListWithPrefix(t *testing.T) {
	_, mockClient, store := setupTest(t)

	for _, p := range []string{"_delta_log/00000000000000000000.json", "part-0.parquet"} {
		if err := mockClient.PutFile(storage.NewPath(p), []byte("x")); err != nil {
			t.Errorf("Error occurred setting up TestListWithPrefix: %e", err)
		}
	}

	result, err := store.ListAll(storage.NewPath("_delta_log/"))
	if err != nil {
		t.Errorf("Error occurred calling ListAll: %e", err)
	}
	for _, o := range result.Objects {
		if o.Location.Raw == "part-0.parquet" {
			t.Errorf("Prefix list should not include %s", o.Location.Raw)
		}
	}
}

func TestListPaginated(t *testing.T) {
	_, mockClient, store := setupTest(t)
	mockClient.PaginateListResults = true

	paths := []string{"a.txt", "b.txt", "c.txt"}
	for _, p := range paths {
		if err := mockClient.PutFile(storage.NewPath(p), []byte("x")); err != nil {
			t.Errorf("Error occurred setting up TestListPaginated: %e", err)
		}
	}

	result, err := store.ListAll(storage.NewPath(""))
	if err != nil {
		t.Errorf("Error occurred calling ListAll: %e", err)
	}
	if len(result.Objects) != len(paths) {
		t.Errorf("Expected %d objects across pages, got %d", len(paths), len(result.Objects))
	}
}

func TestReadAt(t *testing.T) {
	_, mockClient, store := setupTest(t)

	path := storage.NewPath("test.txt")
	data := []byte("hello world")
	if err := mockClient.PutFile(path, data); err != nil {
		t.Errorf("Error occurred setting up TestReadAt: %e", err)
	}

	// Read exactly len(p) bytes; ReadAt returns a nil error in this case.
	p := make([]byte, 5)
	n, err := store.ReadAt(path, p, 6, int64(len(data)))
	if err != nil {
		t.Errorf("Error occurred calling ReadAt: %e", err)
	}
	if n != 5 {
		t.Errorf("Expected 5 bytes, got %d", n)
	}
	if !bytes.Equal(p, []byte("world")) {
		t.Errorf("Expected 'world', got %s", p)
	}

	// Reading more bytes than remain returns io.EOF with a short count.
	q := make([]byte, 10)
	n, err = store.ReadAt(path, q, 6, int64(len(data)))
	if !errors.Is(err, io.EOF) {
		t.Errorf("Expected io.EOF for short read, got %v", err)
	}
	if n != 5 {
		t.Errorf("Expected 5 bytes for short read, got %d", n)
	}
}

func TestSupportsWriter(t *testing.T) {
	_, _, store := setupTest(t)
	if store.SupportsWriter() {
		t.Error("Expected SupportsWriter to be false")
	}
	_, _, err := store.Writer(storage.NewPath("x"), 0)
	if !errors.Is(err, storage.ErrOperationNotSupported) {
		t.Errorf("Expected ErrOperationNotSupported, got %v", err)
	}
}

func TestBaseURI(t *testing.T) {
	baseURI, _, store := setupTest(t)
	if store.BaseURI().Raw != baseURI.Raw {
		t.Errorf("Expected base URI %s, got %s", baseURI.Raw, store.BaseURI().Raw)
	}
}
