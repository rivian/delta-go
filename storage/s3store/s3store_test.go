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
package s3store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"testing"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/rivian/delta-go/s3utils"
	"github.com/rivian/delta-go/storage"
)

// Test helper: setupTest does common setup for our tests, creating a mock S3 client and an S3ObjectStore
func setupTest(t *testing.T) (baseURI storage.Path, mockClient *s3utils.MockClient, s3Store *S3ObjectStore) {
	t.Helper()
	baseURI = storage.NewPath("s3://test-bucket/test-delta-table")
	mockClient, err := s3utils.NewMockClient(t, baseURI)
	if err != nil {
		t.Fatalf("Error occurred setting up for tests %e.", err)
	}
	s3Store, err = New(mockClient, baseURI)
	if err != nil {
		t.Fatalf("Error occurred setting up for tests %e.", err)
	}
	return
}

// Test helper: verify the file exists and has the expected contents
func verifyFileContents(t *testing.T, baseURI storage.Path, path storage.Path, mockClient *s3utils.MockClient, data []byte, errorMessage string) {
	t.Helper()
	results, err := mockClient.GetFile(baseURI, path)
	if err != nil {
		t.Errorf("Error occurred verifying file contents: %e (checking: %s)", err, errorMessage)
	}
	if !bytes.Equal(results, data) {
		t.Errorf("Checking: %s. Results did not match expected. Results: %s, Expected: %s", errorMessage, results, data)
	}
}

// Test helper: verify the file does not exist
func verifyFileDoesNotExist(t *testing.T, baseURI storage.Path, path storage.Path, mockClient *s3utils.MockClient, errorMessage string) {
	t.Helper()
	fileExists, err := mockClient.FileExists(baseURI, path)
	if fileExists {
		t.Error(errorMessage)
	}
	if err != nil {
		t.Errorf("Error occurred checking file exists: %e (checking for: %s)", err, errorMessage)
	}
}

func TestPut(t *testing.T) {
	baseURI, mockClient, s3Store := setupTest(t)

	// Test storing a simple file
	path := storage.NewPath("test.txt")
	data := []byte("data1")
	err := s3Store.Put(path, data)
	if err != nil {
		t.Errorf("Error occurred calling Put: %e", err)
	}
	// Verify the file exists and contents match
	verifyFileContents(t, baseURI, path, mockClient, data, "Put")

	// Overwrite the existing file
	data2 := []byte("data2")
	err = s3Store.Put(path, data2)
	if err != nil {
		t.Errorf("Error occurred calling Put: %e", err)
	}
	// Verify the overwritten contents
	verifyFileContents(t, baseURI, path, mockClient, data2, "Put overwrite")
}

func TestPutErrorHandling(t *testing.T) {
	_, mockClient, s3Store := setupTest(t)

	// Test error handling
	path := storage.NewPath("test.txt")
	data := []byte("data1")
	mockClient.MockError = errors.New("Something went wrong")
	err := s3Store.Put(path, data)
	if !errors.Is(err, storage.ErrPutObject) {
		t.Errorf("Expected error calling Put")
	}
}

func TestGet(t *testing.T) {
	baseURI, mockClient, s3Store := setupTest(t)

	// Set up a test file
	path := storage.NewPath("test.txt")
	data := []byte("some data")
	err := mockClient.PutFile(baseURI, path, data)
	if err != nil {
		t.Errorf("Error occurred setting up TestGet: %e", err)
	}
	// Verify that Get retrieves the file
	results, err := s3Store.Get(path)
	if err != nil {
		t.Errorf("Error occurred calling Get: %e", err)
	}
	if !bytes.Equal(results, data) {
		t.Errorf("Results did not match expected. Results: %s, Expected: %s", results, data)
	}
}

func TestGetErrorHandling(t *testing.T) {
	baseURI, mockClient, s3Store := setupTest(t)

	// Test calling Get on a nonexistent file returns an error
	path := storage.NewPath("test.txt")
	_, err := s3Store.Get(path)
	if !errors.Is(err, storage.ErrGetObject) && !errors.Is(err, storage.ErrObjectDoesNotExist) {
		t.Errorf("Calling Get on a nonexistent file did not return an appropriate error")
	}

	// Test client call returning an error
	// Set up the test file
	data := []byte("some data")
	err = mockClient.PutFile(baseURI, path, data)
	if err != nil {
		t.Errorf("Error occurred setting up TestGetErrorHandling: %e", err)
	}
	mockClient.MockError = errors.New("Something went wrong")
	_, err = s3Store.Get(path)
	if !errors.Is(err, storage.ErrGetObject) {
		t.Errorf("Calling Get did not return an appropriate error")
	}

	// Test client returning an AWS 404 error should cause an ErrObjectDoesNotExist
	response := new(http.Response)
	response.StatusCode = http.StatusNotFound
	smithyResponse := new(smithyhttp.Response)
	smithyResponse.Response = response
	smithyResponseError := new(smithyhttp.ResponseError)
	smithyResponseError.Response = smithyResponse
	responseError := new(awshttp.ResponseError)
	responseError.ResponseError = smithyResponseError
	mockClient.MockError = responseError
	_, err = s3Store.Get(path)
	if !errors.Is(err, storage.ErrObjectDoesNotExist) {
		t.Errorf("Head did not return an appropriate error")
	}
}

func TestRename(t *testing.T) {
	baseURI, mockClient, s3Store := setupTest(t)

	// Set up test file
	path := storage.NewPath("first_copy.txt")
	data := []byte("some data")
	err := mockClient.PutFile(baseURI, path, data)
	if err != nil {
		t.Errorf("Error occurred setting up TestRename: %e", err)
	}

	// Test Rename
	newPath := storage.NewPath("second_copy.txt")
	err = s3Store.Rename(path, newPath)
	if err != nil {
		t.Errorf("Error occurred calling Rename: %e", err)
	}

	// Verify file is not at old path
	verifyFileDoesNotExist(t, baseURI, path, mockClient, "Original file still exists after Rename")

	// Verify file is at new path
	verifyFileContents(t, baseURI, newPath, mockClient, data, "Rename")

	// Set up another test file
	overwritePath := storage.NewPath("third_copy.txt")
	overwriteData := []byte("more data")
	err = mockClient.PutFile(baseURI, overwritePath, overwriteData)
	if err != nil {
		t.Errorf("Error occurred setting up TestRename: %e", err)
	}

	// Overwrite the new file
	err = s3Store.Rename(newPath, overwritePath)
	if err != nil {
		t.Errorf("Error occurred calling Rename: %e", err)
	}
	// Verify file is not at old path
	verifyFileDoesNotExist(t, baseURI, newPath, mockClient, "Original file still exists after Rename")

	// Verify file is at new path
	verifyFileContents(t, baseURI, overwritePath, mockClient, data, "Rename overwrite")
}

func TestRenameErrorHandling(t *testing.T) {
	baseURI, mockClient, s3Store := setupTest(t)

	path := storage.NewPath("first_copy.txt")
	// Test rename where source does not exist
	newPath := storage.NewPath("second_copy.txt")
	err := s3Store.Rename(path, newPath)
	if !errors.Is(err, storage.ErrCopyObject) && !errors.Is(err, storage.ErrObjectDoesNotExist) {
		t.Errorf("Rename did not return an appropriate error")
	}

	// Set up test file
	data := []byte("some data")
	err = mockClient.PutFile(baseURI, path, data)
	if err != nil {
		t.Errorf("Error occurred setting up TestRename: %e", err)
	}

	// Test rename where client returns an error
	mockClient.MockError = errors.New("Something went wrong")
	err = s3Store.Rename(path, newPath)
	if !errors.Is(err, storage.ErrCopyObject) && !errors.Is(err, storage.ErrDeleteObject) {
		t.Errorf("Rename did not return an appropriate error")
	}
}

func TestRenameIfNotExists(t *testing.T) {
	baseURI, mockClient, s3Store := setupTest(t)

	// Set up test file
	path := storage.NewPath("first_copy.txt")
	data := []byte("some data")
	err := mockClient.PutFile(baseURI, path, data)
	if err != nil {
		t.Errorf("Error occurred setting up TestRenameIfNotExists: %e", err)
	}

	// Test Rename
	newPath := storage.NewPath("second_copy.txt")
	err = s3Store.RenameIfNotExists(path, newPath)
	if err != nil {
		t.Errorf("Error occurred calling RenameIfNotExists: %e", err)
	}

	// Verify file is not at old path
	verifyFileDoesNotExist(t, baseURI, path, mockClient, "Original file still exists after RenameIfNotExists")

	// Verify file is at new path
	verifyFileContents(t, baseURI, newPath, mockClient, data, "RenameIfNotExists")
}

func TestRenameIfNotExistsErrorHandling(t *testing.T) {
	baseURI, mockClient, s3Store := setupTest(t)

	// Set up test file
	path := storage.NewPath("first_copy.txt")
	data := []byte("some data")
	err := mockClient.PutFile(baseURI, path, data)
	if err != nil {
		t.Errorf("Error occurred setting up TestRenameIfNotExists: %e", err)
	}

	// Set up another test file
	overwritePath := storage.NewPath("second_copy.txt")
	overwriteData := []byte("more data")
	err = mockClient.PutFile(baseURI, overwritePath, overwriteData)
	if err != nil {
		t.Errorf("Error occurred setting up TestRenameIfNotExists: %e", err)
	}

	// Renaming and overwriting should return an error
	err = s3Store.RenameIfNotExists(path, overwritePath)
	if !errors.Is(err, storage.ErrObjectAlreadyExists) {
		t.Errorf("RenameIfNotExists did not return expected error when overwriting")
	}

	// Test client returning an error
	mockClient.MockError = errors.New("Something went wrong")
	newPath := storage.NewPath("third_copy.txt")
	err = s3Store.Rename(path, newPath)
	if !errors.Is(err, storage.ErrCopyObject) && !errors.Is(err, storage.ErrDeleteObject) {
		t.Errorf("Rename did not return an error")
	}
}

func TestHead(t *testing.T) {
	baseURI, mockClient, s3Store := setupTest(t)

	// Set up test file
	path := storage.NewPath("first_copy.txt")
	data := []byte("some data")
	time0 := time.Now()

	// Add time buffer to alleviate problems that arise from timestamp proximity
	time.Sleep(5 * time.Second)

	err := mockClient.PutFile(baseURI, path, data)
	if err != nil {
		t.Errorf("Error occurred setting up TestHead: %e", err)
	}
	time1 := time.Now()

	// Verify the metadata returned from Head()
	objMeta, err := s3Store.Head(path)
	if err != nil {
		t.Errorf("Failed to retrieve object's metadata: %v", err)
	}
	if objMeta.Size != int64(len(data)) {
		t.Errorf("Size did not match expected. Returned size: %d, Expected size: %d", objMeta.Size, len(data))
	}
	if objMeta.LastModified.Before(time0) || objMeta.LastModified.After(time1) {
		t.Errorf("LastModified did not match expected. Returned LastModified: %s, Expected within range %s - %s", objMeta.LastModified, time0, time1)
	}
}

func TestHeadErrorHandling(t *testing.T) {
	baseURI, mockClient, s3Store := setupTest(t)

	path := storage.NewPath("first_copy.txt")

	// Test calling Head() on a nonexistent file
	_, err := s3Store.Head(path)
	if !errors.Is(err, storage.ErrHeadObject) && !errors.Is(err, storage.ErrObjectDoesNotExist) {
		t.Errorf("Head did not return an expected error for nonexistent file")
	}

	// Test client returning an error
	data := []byte("some data")
	err = mockClient.PutFile(baseURI, path, data)
	if err != nil {
		t.Errorf("Error occurred setting up TestHeadErrorHandling: %e", err)
	}
	mockClient.MockError = errors.New("Something went wrong")
	_, err = s3Store.Head(path)
	if !errors.Is(err, storage.ErrHeadObject) {
		t.Errorf("Head did not return an appropriate error")
	}

	// Test client returning an AWS 404 error should cause an ErrObjectDoesNotExist
	response := new(http.Response)
	response.StatusCode = http.StatusNotFound
	smithyResponse := new(smithyhttp.Response)
	smithyResponse.Response = response
	smithyResponseError := new(smithyhttp.ResponseError)
	smithyResponseError.Response = smithyResponse
	responseError := new(awshttp.ResponseError)
	responseError.ResponseError = smithyResponseError
	mockClient.MockError = responseError
	_, err = s3Store.Head(path)
	if !errors.Is(err, storage.ErrObjectDoesNotExist) {
		t.Errorf("Head did not return an appropriate error")
	}
}

func TestDelete(t *testing.T) {
	baseURI, mockClient, s3Store := setupTest(t)

	// Set up test file
	path := storage.NewPath("first_copy.txt")
	data := []byte("some data")
	err := mockClient.PutFile(baseURI, path, data)
	if err != nil {
		t.Errorf("Error occurred setting up TestDelete: %e", err)
	}
	fileExists, err := mockClient.FileExists(baseURI, path)
	if err != nil {
		t.Errorf("Error occurred setting up TestDelete: %e", err)
	}
	if !fileExists {
		t.Errorf("Error occurred setting up TestDelete: file does not exist")
	}

	// Verify delete removes the file
	err = s3Store.Delete(path)
	if err != nil {
		t.Errorf("Unexpected error calling Delete: %e", err)
	}
	verifyFileDoesNotExist(t, baseURI, path, mockClient, "File still exists after Delete")
}

func TestDeleteErrorHandling(t *testing.T) {
	baseURI, mockClient, s3Store := setupTest(t)

	// Test deleting a nonexistent file
	path := storage.NewPath("first_copy.txt")
	err := s3Store.Delete(path)
	if !errors.Is(err, storage.ErrDeleteObject) && !errors.Is(err, storage.ErrObjectDoesNotExist) {
		t.Errorf("Delete did not return an expected error for nonexistent file")
	}

	// Test client returning an error
	data := []byte("some data")
	err = mockClient.PutFile(baseURI, path, data)
	if err != nil {
		t.Errorf("Error occurred setting up TestDeleteErrorHandling: %e", err)
	}
	mockClient.MockError = errors.New("Something went wrong")
	err = s3Store.Delete(path)
	if !errors.Is(err, storage.ErrDeleteObject) {
		t.Errorf("Delete did not return an expected error")
	}
}

func TestDeleteFolder(t *testing.T) {
	baseURI, mockClient, s3Store := setupTest(t)

	// Set up test folder
	data := []byte("some data")
	for i := 0; i < 100; i++ {
		path := storage.NewPath(fmt.Sprintf("test/%d.txt", i))
		err := mockClient.PutFile(baseURI, path, data)
		if err != nil {
			t.Errorf("Error occurred setting up TestDeleteFolder: %v", err)
		}
		fileExists, err := mockClient.FileExists(baseURI, path)
		if err != nil {
			t.Errorf("Error occurred setting up TestDeleteFolder: %v", err)
		}
		if !fileExists {
			t.Errorf("Error occurred setting up TestDeleteFolder: file does not exist")
		}
	}

	err := s3Store.DeleteFolder(storage.NewPath("test"))
	if err != nil {
		t.Errorf("Unexpected error calling DeleteFolder: %v", err)
	}

	for i := 0; i < 100; i++ {
		path := storage.NewPath(fmt.Sprintf("test/%d.txt", i))
		verifyFileDoesNotExist(t, baseURI, path, mockClient, "File still exists after DeleteFolder")
	}
}

func TestDeleteFolderErrorHandling(t *testing.T) {
	baseURI, mockClient, s3Store := setupTest(t)

	// Test deleting a nonexistent folder
	path := storage.NewPath("test")
	err := s3Store.DeleteFolder(path)
	if !errors.Is(err, storage.ErrDeleteObject) && !errors.Is(err, storage.ErrObjectDoesNotExist) {
		t.Errorf("DeleteFolder did not return an expected error for nonexistent file")
	}

	// Test client returning an error
	// Set up test folder
	data := []byte("some data")
	for i := 0; i < 100; i++ {
		path := storage.NewPath(fmt.Sprintf("test/%d.txt", i))
		err := mockClient.PutFile(baseURI, path, data)
		if err != nil {
			t.Errorf("Error occurred setting up TestDeleteFolder: %e", err)
		}
		fileExists, err := mockClient.FileExists(baseURI, path)
		if err != nil {
			t.Errorf("Error occurred setting up TestDeleteFolder: %e", err)
		}
		if !fileExists {
			t.Errorf("Error occurred setting up TestDeleteFolder: file does not exist")
		}
	}

	mockClient.MockError = errors.New("Something went wrong")
	err = s3Store.DeleteFolder(path)
	if !errors.Is(err, storage.ErrDeleteObject) {
		t.Errorf("DeleteFolder did not return an expected error")
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

	for i := range expected {
		if actual[i] != expected[i] {
			t.Errorf("Expected path %s but result is %s", expected[i], actual[i])
		}
	}
}

func TestList(t *testing.T) {
	baseURI, mockClient, store := setupTest(t)

	// Create some files and directories
	filePaths := []string{"data.json", "data2.json", "d3.json", "data/more.json", "data/more2.json", "data3/hello.json"}
	data := []byte("some data")
	for _, filePath := range filePaths {
		err := mockClient.PutFile(baseURI, storage.NewPath(filePath), data)
		if err != nil {
			t.Errorf("Error setting up TestList: %e", err)
		}
	}

	type args struct {
		prefix storage.Path
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
			got, err := store.List(tt.args.prefix, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileObjectStore.List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			compareExpectedPaths(t, tt.want, got.Objects)
		})
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.ListAll(tt.args.prefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileObjectStore.ListAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			compareExpectedPaths(t, tt.want, got.Objects)
		})
	}
}

// This tests the mock client pagination
func TestListPagination(t *testing.T) {
	baseURI, mockClient, store := setupTest(t)

	// Create many files
	var files []string
	for i := 0; i < 2100; i++ {
		filePath := fmt.Sprintf("temp_%d", i)
		files = append(files, filePath)
		err := mockClient.PutFile(baseURI, storage.NewPath(filePath), []byte{})
		if err != nil {
			t.Errorf("Error setting up TestList: %e", err)
		}
	}
	sort.Strings(files)

	mockClient.PaginateListResults = true

	got, err := store.List(storage.NewPath("temp"), nil)
	if err != nil {
		t.Error(err)
	}
	if got.NextToken != "1000" {
		t.Errorf("Expected next token 1000, got %s", got.NextToken)
	}
	if len(got.Objects) != 1000 {
		t.Errorf("Expected 1000 objects, got %d", len(got.Objects))
	}
	for i := 0; i < 1000; i++ {
		if got.Objects[i].Location.Raw != files[i] {
			t.Errorf("Expected %s got %s", files[i], got.Objects[i].Location.Raw)
		}
	}

	got2, err := store.List(storage.NewPath("temp"), &got)
	if err != nil {
		t.Error(err)
	}
	if got2.NextToken != "2000" {
		t.Errorf("Expected next token 2000, got %s", got2.NextToken)
	}
	if len(got2.Objects) != 1000 {
		t.Errorf("Expected 1000 objects, got %d", len(got2.Objects))
	}
	for i := 1000; i < 2000; i++ {
		if got2.Objects[i-1000].Location.Raw != files[i] {
			t.Errorf("Expected %s got %s", files[i], got2.Objects[i-1000].Location.Raw)
		}
	}

	got3, err := store.List(storage.NewPath("temp"), &got2)
	if err != nil {
		t.Error(err)
	}
	if got3.NextToken != "" {
		t.Errorf("Expected next token empty, got %s", got3.NextToken)
	}
	if len(got3.Objects) != 100 {
		t.Errorf("Expected 100 objects, got %d", len(got3.Objects))
	}
	for i := 2000; i < 2100; i++ {
		if got3.Objects[i-2000].Location.Raw != files[i] {
			t.Errorf("Expected %s got %s", files[i], got3.Objects[i-2000].Location.Raw)
		}
	}
}

func TestListErrorHandling(t *testing.T) {
	_, mockClient, store := setupTest(t)

	// Test client returning an error
	mockClient.MockError = errors.New("Something went wrong")
	path := storage.NewPath("data.txt")
	_, err := store.List(path, nil)
	if !errors.Is(err, storage.ErrListObjects) {
		t.Errorf("Delete did not return an expected error")
	}
	_, err = store.ListAll(path)
	if !errors.Is(err, storage.ErrListObjects) {
		t.Errorf("Delete did not return an expected error")
	}
}

func TestReadAt(t *testing.T) {
	baseURI, mockClient, store := setupTest(t)

	putPath := storage.NewPath("test_file.json")
	err := mockClient.PutFile(baseURI, putPath, []byte("01234567890123456789"))
	if err != nil {
		t.Error(err)
	}

	type args struct {
		location storage.Path
		offset   int64
		max      int64
	}
	tests := []struct {
		name        string
		args        args
		want        string
		wantN       int
		wantErr     error
		optionalErr error
	}{
		{name: "Everything", args: args{location: putPath, offset: 0, max: 19}, want: "01234567890123456789", wantN: 20, wantErr: nil, optionalErr: io.EOF},
		{name: "Start", args: args{location: putPath, offset: 0, max: 9}, want: "0123456789", wantN: 10, wantErr: nil, optionalErr: io.EOF},
		{name: "End", args: args{location: putPath, offset: 17, max: 19}, want: "789", wantN: 3, wantErr: nil, optionalErr: io.EOF},
		{name: "Middle", args: args{location: putPath, offset: 3, max: 5}, want: "345", wantN: 3, wantErr: nil, optionalErr: io.EOF},
		{name: "Negative", args: args{location: putPath, offset: -3, max: 5}, want: "\000\000\000\000\000\000\000\000\000", wantN: 0, wantErr: storage.ErrReadAt},
		{name: "Off the End", args: args{location: putPath, offset: 15, max: 24}, want: "\000\000\000\000\000\000\000\000\000\000", wantN: 0, wantErr: io.EOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := make([]byte, tt.args.max-tt.args.offset+1)
			n, err := store.ReadAt(tt.args.location, p, tt.args.offset, tt.args.max)
			if tt.wantErr != nil && err == nil {
				t.Errorf("FileObjectStore.ReadAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Errorf("FileObjectStore.ReadAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr == nil && err != nil && !errors.Is(err, tt.optionalErr) {
				t.Errorf("FileObjectStore.ReadAt() error = %v, optionalErr %v", err, tt.optionalErr)
				return
			}
			if n != tt.wantN {
				t.Errorf("FileObjectStore.ReadAt() n = %d, want %d", n, tt.wantN)
				return
			}
			if !bytes.Equal(p, []byte(tt.want)) {
				t.Errorf("FileObjectStore.ReadAt() result = %v, want %v", p, []byte(tt.want))
				return
			}
		})
	}
}
