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

package azureutils

import (
	"context"
	"errors"
	"io"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/filestore"
)

// MockClient is a Client backed by a local file store in a temporary directory.
// It is used to exercise the Azure object store without any network or credentials.
type MockClient struct {
	// fileStore mocks ADLS Gen2 storage on the local file system
	fileStore *filestore.FileObjectStore
	// storePath is the path within the container that the object store writes under
	storePath string
	// For testing: if MockError is set, any Client method called will return that error
	MockError error
	// For testing: enable pagination during List
	PaginateListResults bool
}

// Compile time check that MockClient implements Client.
var _ Client = (*MockClient)(nil)

// NewMockClient creates a mock Client that uses a filestore in a temporary directory
// to store, retrieve, and manipulate paths. The baseURI's path component is used as
// the store path so that list results match a real ADLS Gen2 filesystem.
func NewMockClient(t *testing.T, baseURI storage.Path) (*MockClient, error) {
	tmpDir := t.TempDir()
	var fileStore filestore.FileObjectStore
	fileStore.SetBaseURI(storage.NewPath(tmpDir))

	baseURL, err := baseURI.ParseURL()
	if err != nil {
		return nil, err
	}

	client := new(MockClient)
	client.fileStore = &fileStore
	client.storePath = strings.Trim(baseURL.Path, "/")
	return client, nil
}

// FileStore returns the underlying file store.
func (m *MockClient) FileStore() *filestore.FileObjectStore {
	return m.fileStore
}

// Key returns the container-relative key for a location, matching what the object
// store passes to the Client.
func (m *MockClient) Key(location storage.Path) string {
	return path.Join(m.storePath, location.Raw)
}

// GetFile returns the bytes stored at the given location, for use in unit tests.
func (m *MockClient) GetFile(location storage.Path) ([]byte, error) {
	return m.fileStore.Get(storage.NewPath(m.Key(location)))
}

// PutFile writes bytes directly to the underlying file store, for use in unit tests.
func (m *MockClient) PutFile(location storage.Path, data []byte) error {
	return m.fileStore.Put(storage.NewPath(m.Key(location)), data)
}

// FileExists reports whether an object exists at the given location, for use in unit tests.
func (m *MockClient) FileExists(location storage.Path) bool {
	_, err := m.fileStore.Head(storage.NewPath(m.Key(location)))
	return err == nil
}

// Upload writes data to the path at key.
func (m *MockClient) Upload(_ context.Context, key string, data []byte) error {
	if m.MockError != nil {
		return m.MockError
	}
	return m.fileStore.Put(storage.NewPath(key), data)
}

// Download returns the bytes stored at key.
func (m *MockClient) Download(_ context.Context, key string) ([]byte, error) {
	if m.MockError != nil {
		return nil, m.MockError
	}
	data, err := m.fileStore.Get(storage.NewPath(key))
	if errors.Is(err, storage.ErrObjectDoesNotExist) {
		return nil, errors.Join(ErrNotFound, err)
	}
	return data, err
}

// DownloadRange returns count bytes stored at key starting at offset.
func (m *MockClient) DownloadRange(_ context.Context, key string, offset int64, count int64) ([]byte, error) {
	if m.MockError != nil {
		return nil, m.MockError
	}
	data, err := m.fileStore.Get(storage.NewPath(key))
	if errors.Is(err, storage.ErrObjectDoesNotExist) {
		return nil, errors.Join(ErrNotFound, err)
	}
	if err != nil {
		return nil, err
	}
	if offset < 0 || offset > int64(len(data)) {
		return nil, errors.Join(io.EOF, errors.New("invalid offset"))
	}
	end := int64(len(data))
	if count > 0 && offset+count < end {
		end = offset + count
	}
	return data[offset:end], nil
}

// GetProperties returns the metadata of the path at key.
func (m *MockClient) GetProperties(_ context.Context, key string) (ObjectProperties, error) {
	if m.MockError != nil {
		return ObjectProperties{}, m.MockError
	}
	meta, err := m.fileStore.Head(storage.NewPath(key))
	if errors.Is(err, storage.ErrObjectDoesNotExist) {
		return ObjectProperties{}, errors.Join(ErrNotFound, err)
	}
	if err != nil {
		return ObjectProperties{}, err
	}
	return ObjectProperties{ContentLength: meta.Size, LastModified: meta.LastModified}, nil
}

// Delete removes the file at key.
func (m *MockClient) Delete(_ context.Context, key string) error {
	if m.MockError != nil {
		return m.MockError
	}
	err := m.fileStore.Delete(storage.NewPath(key))
	if errors.Is(err, storage.ErrObjectDoesNotExist) {
		return errors.Join(ErrNotFound, err)
	}
	return err
}

// DeleteDirectory recursively removes the directory at key.
func (m *MockClient) DeleteDirectory(_ context.Context, key string) error {
	if m.MockError != nil {
		return m.MockError
	}
	result, err := m.fileStore.ListAll(storage.NewPath(key))
	if err != nil {
		return err
	}
	if len(result.Objects) == 0 {
		return ErrNotFound
	}
	return m.fileStore.DeleteFolder(storage.NewPath(key))
}

// List returns a single page of paths under prefix, resuming from marker if set.
func (m *MockClient) List(_ context.Context, prefix string, marker string) (ListResult, error) {
	if m.MockError != nil {
		return ListResult{}, m.MockError
	}
	result, err := m.fileStore.ListAll(storage.NewPath(prefix))
	if err != nil {
		return ListResult{}, err
	}

	paths := make([]PathItem, 0, len(result.Objects))
	for _, o := range result.Objects {
		paths = append(paths, PathItem{
			Name:         o.Location.Raw,
			ContentLength: o.Size,
			LastModified:  o.LastModified,
			IsDirectory:   strings.HasSuffix(o.Location.Raw, "/"),
		})
	}

	if !m.PaginateListResults {
		return ListResult{Paths: paths}, nil
	}

	// Emulate single-item paging using the marker as an integer offset.
	offset := 0
	if marker != "" {
		if parsed, perr := strconv.Atoi(marker); perr == nil {
			offset = parsed
		}
	}
	if offset >= len(paths) {
		return ListResult{}, nil
	}
	out := ListResult{Paths: paths[offset : offset+1]}
	if offset+1 < len(paths) {
		out.NextMarker = strconv.Itoa(offset + 1)
	}
	return out, nil
}

// Rename moves the path at source to destination.
func (m *MockClient) Rename(_ context.Context, source string, destination string, overwrite bool) error {
	if m.MockError != nil {
		return m.MockError
	}
	if !overwrite {
		err := m.fileStore.RenameIfNotExists(storage.NewPath(source), storage.NewPath(destination))
		if errors.Is(err, storage.ErrObjectAlreadyExists) {
			return errors.Join(ErrAlreadyExists, err)
		}
		if errors.Is(err, storage.ErrObjectDoesNotExist) {
			return errors.Join(ErrNotFound, err)
		}
		return err
	}
	err := m.fileStore.Rename(storage.NewPath(source), storage.NewPath(destination))
	if errors.Is(err, storage.ErrObjectDoesNotExist) {
		return errors.Join(ErrNotFound, err)
	}
	return err
}
