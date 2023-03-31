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
package filestore

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/rivian/delta-go/storage"
)

func TestPut(t *testing.T) {

	tmpDir := t.TempDir()
	// fileLockKey := filepath.Join(tmpDir, "_delta_log/_commit.lock")

	tmpPath := storage.NewPath(tmpDir)
	store := FileObjectStore{BaseURI: tmpPath}
	// lock := filelock.FileLock{Key: fileLockKey}

	putPath := storage.NewPath("test_file.json")
	err := store.Put(putPath, []byte("some data"))
	if err != nil {
		t.Errorf("err = %e;", err)
	}

	if !fileExists(filepath.Join(tmpDir, putPath.Raw)) {
		t.Errorf("File does not exist")
	}

	data, err := os.ReadFile(filepath.Join(tmpDir, putPath.Raw))
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if string(data) != "some data" {
		t.Errorf("file has: %s, want 'some data'", string(data))
	}
}

func TestHead(t *testing.T) {

	tmpDir := t.TempDir()
	// fileLockKey := filepath.Join(tmpDir, "_delta_log/_commit.lock")

	tmpPath := storage.NewPath(tmpDir)
	store := FileObjectStore{BaseURI: tmpPath}
	// lock := filelock.FileLock{Key: fileLockKey}

	putPath := storage.NewPath("test_file.json")

	//check before writing the file
	meta, err := store.Head(putPath)
	//err object should not exist
	if !errors.Is(err, storage.ErrorObjectDoesNotExist) {
		t.Errorf("err = %e;", err)
	}

	//size should be 0
	if meta.Size != 0 {
		t.Errorf("file size: %d, want size=0", meta.Size)
	}

	err = store.Put(putPath, []byte("some data"))
	if err != nil {
		t.Errorf("err = %e;", err)
	}

	meta, err = store.Head(putPath)
	if err != nil {
		t.Errorf("err = %e;", err)
	}

	if meta.Size != 9 {
		t.Errorf("file size: %d, want size=9", meta.Size)
	}

}

func TestRenameIfNotExists(t *testing.T) {

	tmpDir := t.TempDir()

	tmpPath := storage.NewPath(tmpDir)
	store := FileObjectStore{BaseURI: tmpPath}

	fromPath := storage.NewPath("data.json.tmp")
	toPath := storage.NewPath("data.json")

	// Write data to be renamed
	err := store.Put(fromPath, []byte("some data"))
	if err != nil {
		t.Errorf("err = %e;", err)
	}

	err = store.RenameIfNotExists(fromPath, toPath)
	if err != nil {
		t.Errorf("err = %e;", err)
	}

	//File already exists from previous rename, so err should be storage.ErrorVersionAlreadyExists
	err = store.RenameIfNotExists(fromPath, toPath)
	if !errors.Is(err, storage.ErrorVersionAlreadyExists) {
		t.Errorf("err = %e;", err)
	}

	data, err := os.ReadFile(filepath.Join(tmpDir, toPath.Raw))
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if string(data) != "some data" {
		t.Errorf("file has: %s, want 'some data'", string(data))
	}
}

func TestDelete(t *testing.T) {

	tmpDir := t.TempDir()

	tmpPath := storage.NewPath(tmpDir)
	store := FileObjectStore{BaseURI: tmpPath}

	filePath := storage.NewPath("data.json")

	// Write data to be deleted
	err := store.Put(filePath, []byte("some data"))
	if err != nil {
		t.Errorf("Error setting up TestDelete: %e", err)
	}

	// Verify file exists before test
	if !fileExists(filepath.Join(tmpDir, filePath.Raw)) {
		t.Errorf("File does not exist")
	}

	// Delete and verify results
	err = store.Delete(filePath)
	if err != nil {
		t.Errorf("Unexpected error calling Delete: %e", err)
	}
	if fileExists(filepath.Join(tmpDir, filePath.Raw)) {
		t.Errorf("File exists after Delete")
	}

	// Verify that deleting a non-existent file returns an error
	err = store.Delete(filePath)
	if err == nil {
		t.Errorf("Expected an error calling Delete on a non-existent file")
	}
	if !errors.Is(err, storage.ErrorDeleteObject) && !errors.Is(err, storage.ErrorObjectDoesNotExist) {
		t.Errorf("Invalid error from Delete: %e", err)
	}
}

func compareExpectedPaths(t *testing.T, expected []string, results []storage.ObjectMeta) {
	t.Helper()

	if len(expected) != len(results) {
		t.Errorf("Length of results does not match expected length: Expected %d, results %d", len(expected), len(results))
		return
	}

	actual := make([]string, 0, len(results))
	for _, r := range results {
		actual = append(actual, r.Location.Raw)
	}
	sort.Strings(expected)
	sort.Strings(actual)
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v, results %v", expected, results)
	}
}

func TestList(t *testing.T) {
	tmpDir := t.TempDir()

	tmpPath := storage.NewPath(tmpDir)
	store := FileObjectStore{BaseURI: tmpPath}

	// Create some files and directories
	filePaths := []string{"data.json", "data2.json", "d3.json", "data/more.json", "data/more2.json", "data3/hello.json"}
	data := []byte("some data")
	for _, filePath := range filePaths {
		err := store.Put(storage.NewPath(filePath), data)
		if err != nil {
			t.Fatalf("Error setting up TestList: %e", err)
		}
	}

	type args struct {
		prefix *storage.Path
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{name: "Everything", args: args{prefix: storage.NewPath("d")}, want: append(filePaths, "data/", "data3/"), wantErr: false},
		{name: "Empty prefix", args: args{prefix: storage.NewPath("")}, want: append(filePaths, "data/", "data3/"), wantErr: false},
		// Test a prefix with no trailing separator that doesn't match everything but does match both subfolders and files
		{name: "Files and folders", args: args{prefix: storage.NewPath("data")}, want: []string{"data.json", "data2.json", "data/more.json", "data/more2.json", "data3/hello.json", "data/", "data3/"}, wantErr: false},
		// Test a trailing separator that matches a folder with contents
		{name: "Folder", args: args{prefix: storage.NewPath("data/")}, want: []string{"data/more.json", "data/more2.json", "data/"}, wantErr: false},
		// Test no trailing separator, no match
		{name: "No match", args: args{prefix: storage.NewPath("data4")}, want: []string{}, wantErr: false},
		// Test a trailing separator, no match
		{name: "No match folder", args: args{prefix: storage.NewPath("data4/")}, want: []string{}, wantErr: false},
		// Test a prefix that includes a subfolder that has a match
		{name: "Subfolder and additional prefix", args: args{prefix: storage.NewPath("data/more.")}, want: []string{"data/more.json"}, wantErr: false},
		// Test a prefix that includes a subfolder with no match
		{name: "Subfolder and additional prefix no match", args: args{prefix: storage.NewPath("data/moredata.")}, want: []string{}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.List(tt.args.prefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileObjectStore.List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			compareExpectedPaths(t, tt.want, got)
		})
	}
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
