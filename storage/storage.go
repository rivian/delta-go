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
package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"time"
)

var (
	ErrorObjectAlreadyExists error = errors.New("the object already exists")
	ErrorObjectDoesNotExist  error = errors.New("the object does not exist")
	ErrorObjectIsDir         error = errors.New("the object is a directory")
	ErrorCopyObject          error = errors.New("error while copying the object")
	ErrorPutObject           error = errors.New("error while putting the object")
	ErrorGetObject           error = errors.New("error while getting the object")
	ErrorHeadObject          error = errors.New("error while getting the object head")
	ErrorDeleteObject        error = errors.New("error while deleting the object")
	ErrorURLJoinPath         error = errors.New("error during url.JoinPath")
	ErrorListObjects         error = errors.New("error while listing objects")
)

type DeltaStorageResult struct {
}

// TODO Implement methods for path
type Path struct {
	Raw string
}

func NewPath(raw string) *Path {
	p := new(Path)
	p.Raw = raw
	return p
}

func (p *Path) CommitPathForVersion(version int64) string {
	s := fmt.Sprintf("%020d.json", version)
	return filepath.Join(p.Raw, s)
}

// Calls url.Parse on Path.Raw
func (p *Path) ParseURL() (*url.URL, error) {
	return url.Parse(p.Raw)
}

func (p *Path) Base() string {
	return filepath.Base(p.Raw)
}

func (p *Path) Ext() string {
	return filepath.Ext(p.Raw)
}

func PathFromIter(elem []string) Path {
	s := filepath.Join(elem...)
	return Path{Raw: s}
}

func (p *Path) Join(path *Path) *Path {
	return &Path{Raw: filepath.Join(p.Raw, path.Raw)}
}

type URL struct {
	/// A URL that identifies a file or directory to list files from
	URL  url.URL
	Path Path
	/// The path prefix
	Prefix Path
}

type DeltaStorageConfig struct {
	URL URL
}

type Storage interface {
	RenameIfNotExists(from Path, to Path) error
}

// Id type for multi-part uploads.
type MultipartId string
type Range struct {
	Start int64
	End   int64
}

// / The metadata that describes an object.
type ObjectMeta struct {
	/// The full path to the object
	Location Path
	/// The last modified time
	LastModified time.Time
	/// The size in bytes of the object
	Size int64
}

// / Result of a list call that includes objects, prefixes (directories) and a
// / token for the next set of results. Individual result sets may be limited to
// / 1,000 objects based on the underlying object storage's limitations.
type ListResult struct {
	/// Prefixes that are common (like directories)
	// CommonPrefixes []Path
	/// Object metadata for the listing
	Objects   []ObjectMeta
	NextToken string
}

// / Lock data which stores an attempt to rename `source` into `destination`
type LockData struct {
	// Source object key
	Source string `json:"source"`
	// Destination object key
	Destination string `json:"destination"`
	Version     int64  `json:"version"`
}

func (ld *LockData) Json() []byte {
	data, _ := json.Marshal(ld)
	return data
}

// ObjectStore Universal API to multiple object store services.
type ObjectStore interface {
	/// Save the provided bytes to the specified location.
	Put(location *Path, bytes []byte) error

	// 	/// Get a multi-part upload that allows writing data in chunks
	// 	///
	// 	/// Most cloud-based uploads will buffer and upload parts in parallel.
	// 	///
	// 	/// To complete the upload, [AsyncWrite::poll_shutdown] must be called
	// 	/// to completion.
	// 	///
	// 	/// For some object stores (S3, GCS, and local in particular), if the
	// 	/// writer fails or panics, you must call [ObjectStore::abort_multipart]
	// 	/// to clean up partially written data.
	// 	PutMultipart(location *Path) error

	// 	/// Cleanup an aborted upload.
	// 	///
	// 	/// See documentation for individual stores for exact behavior, as capabilities
	// 	/// vary by object store.
	// 	AbortMultipart(location *Path, multipart_id *MultipartId) error

	/// Return the bytes that are stored at the specified location.
	Get(location *Path) ([]byte, error)

	// 	/// Return the bytes that are stored at the specified location
	// 	/// in the given byte range
	// 	GetRange(location *Path, r Range) error

	// 	/// Return the bytes that are stored at the specified location
	// 	/// in the given byte ranges
	// 	GetRanges(location *Path, ranges []Range) ([]byte, error)
	/// Return the metadata for the specified location
	Head(location *Path) (ObjectMeta, error)

	// 	/// Delete the object at the specified location.
	Delete(location *Path) error

	/// List the objects with the given prefix.  This may be limited to a certain number of objects (e.g. 1000)
	/// based on the underlying object storage's limitations.
	/// If a previousResult is provided and the store supports paging, the next page of results will be returned.
	///
	/// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
	/// `foo/bar_baz/x`.
	List(prefix *Path, previousResult *ListResult) (ListResult, error)

	/// List all objects with the given prefix. If the underlying object storage returns a limited number of objects,
	/// this will perform paging as required to return all results
	///
	/// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
	/// `foo/bar_baz/x`.
	ListAll(prefix *Path) (ListResult, error)

	/// Returns true if this store returns list results sorted
	IsListOrdered() bool

	// 	/// List all the objects with the given prefix.
	// 	///
	// 	/// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
	// 	/// `foo/bar_baz/x`.
	// 	List(prefix *Path) (bufio.Scanner, ObjectMeta)

	// 	/// List objects with the given prefix and an implementation specific
	// 	/// delimiter. Returns common prefixes (directories) in addition to object
	// 	/// metadata.
	// 	///
	// 	/// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
	// 	/// `foo/bar_baz/x`.
	// 	ListWithDelimiter(prefix *Path) ListResult

	// /// Copy an object from one path to another in the same object store.
	// ///
	// /// If there exists an object at the destination, it will be overwritten.
	// Copy(from *Path, to *Path) error

	// 	/// Move an object from one path to another in the same object store.
	// 	///
	// 	/// By default, this is implemented as a copy and then delete source. It may not
	// 	/// check when deleting source that it was the same object that was originally copied.
	// 	///
	/// If there exists an object at the destination, it will be overwritten.
	Rename(from *Path, to *Path) error

	// 	/// Copy an object from one path to another, only if destination is empty.
	// 	///
	// 	/// Will return an error if the destination already has an object.
	// 	///
	// 	/// Performs an atomic operation if the underlying object storage supports it.
	// 	/// If atomic operations are not supported by the underlying object storage (like S3)
	// 	/// it will return an error.
	// 	CopyIfNotExists(from *Path, to *Path) error

	// Move an object from one path to another in the same object store.

	// Will return an error if the destination already has an object.
	RenameIfNotExists(from *Path, to *Path) error
}

// / Wrapper around List that will perform paging if required
type ListIterator struct {
	store      ObjectStore
	prefix     *Path
	listResult *ListResult
	nextIndex  int
}

func NewListIterator(prefix *Path, store ObjectStore) *ListIterator {
	iterator := new(ListIterator)
	iterator.listResult = nil
	iterator.prefix = prefix
	iterator.store = store
	return iterator
}

// / Return the next object in the list
// / When there are no more objects, return nil and the error ErrorObjectDoesNotExist
func (listIterator *ListIterator) Next() (*ObjectMeta, error) {
	// Fetch the first page, or the next page, if necessary
	if listIterator.listResult == nil || (listIterator.nextIndex >= len(listIterator.listResult.Objects) && listIterator.listResult.NextToken != "") {
		nextListResult, err := listIterator.store.List(listIterator.prefix, listIterator.listResult)
		if err != nil {
			return nil, err
		}
		listIterator.listResult = &nextListResult
		listIterator.nextIndex = 0
	}

	if listIterator.nextIndex >= len(listIterator.listResult.Objects) {
		return nil, ErrorObjectDoesNotExist
	}

	result := listIterator.listResult.Objects[listIterator.nextIndex]
	listIterator.nextIndex++
	return &result, nil
}
