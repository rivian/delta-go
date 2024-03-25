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

// Package storage contains the resources required to interact with an object store.
package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"time"
)

var (
	// ErrObjectAlreadyExists is returned when an object already exists.
	ErrObjectAlreadyExists error = errors.New("the object already exists")
	// ErrObjectDoesNotExist is returned when an object does not exist.
	ErrObjectDoesNotExist error = errors.New("the object does not exist")
	// ErrObjectIsDir is returned when an object is a directory.
	ErrObjectIsDir error = errors.New("the object is a directory")
	// ErrCopyObject is returned when an object cannot be copied.
	ErrCopyObject error = errors.New("error while copying the object")
	// ErrPutObject is returned when an object cannot be created.
	ErrPutObject error = errors.New("error while putting the object")
	// ErrGetObject is returned when an object cannot be retrieved.
	ErrGetObject error = errors.New("error while getting the object")
	// ErrHeadObject is returned when an object's metadata cannot be retrieved.
	ErrHeadObject error = errors.New("error while getting the object head")
	// ErrDeleteObject is returned when an object cannot be deleted.
	ErrDeleteObject error = errors.New("error while deleting the object")
	// ErrURLJoinPath is returned when paths cannot be joined.
	ErrURLJoinPath error = errors.New("error during url.JoinPath")
	// ErrListObjects is returned when objects cannot be listed.
	ErrListObjects error = errors.New("error while listing objects")
	// ErrOperationNotSupported is returned when an operation is not supported.
	ErrOperationNotSupported error = errors.New("the object store does not support this operation")
	// ErrWriter is returned when a writer cannot be retrieved.
	ErrWriter error = errors.New("error while getting writer")
	// ErrSeekOffset is returned when a seek offset is invalid
	ErrSeekOffset error = errors.New("invalid seek offset")
	// ErrSeekWhence is returned when a seek whence is invalid.
	ErrSeekWhence error = errors.New("invalid seek whence")
	// ErrReadAt is returned when an object cannot be read.
	ErrReadAt error = errors.New("error while reading the object")
)

// Path stores the location of an object.
// TODO Implement methods for path
type Path struct {
	Raw string
}

// NewPath creates a new Path instance.
func NewPath(raw string) Path {
	p := new(Path)
	p.Raw = raw
	return *p
}

// CommitPathForVersion retrieves the commit URI for a given version.
func (p Path) CommitPathForVersion(version int64) string {
	s := fmt.Sprintf("%020d.json", version)
	return filepath.Join(p.Raw, s)
}

// ParseURL parses a raw URL into a URL structure.
func (p Path) ParseURL() (*url.URL, error) {
	return url.Parse(p.Raw)
}

// Base returns the base of a path.
func (p Path) Base() string {
	return filepath.Base(p.Raw)
}

// Ext returns the extension of a path.
func (p Path) Ext() string {
	return filepath.Ext(p.Raw)
}

// PathFromIter joins a list of strings to create a path.
func PathFromIter(elem []string) Path {
	s := filepath.Join(elem...)
	return Path{Raw: s}
}

// Join joins two paths.
func (p Path) Join(path Path) Path {
	return Path{Raw: filepath.Join(p.Raw, path.Raw)}
}

// MultipartID is a type for multi-part uploads.
type MultipartID string

// Range represents a start and an end.
type Range struct {
	Start int64
	End   int64
}

// ObjectMeta is the metadata that describes an object.
type ObjectMeta struct {
	/// The full path to the object
	Location Path
	/// The last modified time
	LastModified time.Time
	/// The size in bytes of the object
	Size int64
}

// ListResult is the result of a list call that includes objects, prefixes (directories) and a
// token for the next set of results. Individual result sets may be limited to 1,000 objects
// based on the underlying object storage's limitations.
type ListResult struct {
	/// Prefixes that are common (like directories)
	// CommonPrefixes []Path
	/// Object metadata for the listing
	Objects   []ObjectMeta
	NextToken string
}

// LockData stores an attempt to rename `source` into `destination`
type LockData struct {
	// Source object key
	Source string `json:"source"`
	// Destination object key
	Destination string `json:"destination"`
	Version     int64  `json:"version"`
}

// JSON marshalls lock data into a JSON object.
func (ld *LockData) JSON() []byte {
	data, _ := json.Marshal(ld)
	return data
}

// ObjectStore Universal API to multiple object store services.
type ObjectStore interface {
	/// Save the provided bytes to the specified location.
	Put(location Path, bytes []byte) error

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
	// 	PutMultipart(location Path) error

	// 	/// Cleanup an aborted upload.
	// 	///
	// 	/// See documentation for individual stores for exact behavior, as capabilities
	// 	/// vary by object store.
	// 	AbortMultipart(location Path, multipart_id *MultipartId) error

	/// Return the bytes that are stored at the specified location.
	Get(location Path) ([]byte, error)

	// 	/// Return the bytes that are stored at the specified location
	// 	/// in the given byte range
	// 	GetRange(location Path, r Range) error

	// 	/// Return the bytes that are stored at the specified location
	// 	/// in the given byte ranges
	// 	GetRanges(location Path, ranges []Range) ([]byte, error)
	/// Return the metadata for the specified location
	Head(location Path) (ObjectMeta, error)

	// 	/// Delete the object at the specified location.
	Delete(location Path) error

	/// Delete the folder at the specified location.
	DeleteFolder(location Path) error

	/// List the objects with the given prefix.  This may be limited to a certain number of objects (e.g. 1000)
	/// based on the underlying object storage's limitations.
	/// If a previousResult is provided and the store supports paging, the next page of results will be returned.
	///
	/// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
	/// `foo/bar_baz/x`.
	List(prefix Path, previousResult *ListResult) (ListResult, error)

	/// List all objects with the given prefix. If the underlying object storage returns a limited number of objects,
	/// this will perform paging as required to return all results
	///
	/// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
	/// `foo/bar_baz/x`.
	ListAll(prefix Path) (ListResult, error)

	/// Returns true if this store returns list results sorted
	IsListOrdered() bool

	// 	/// List all the objects with the given prefix.
	// 	///
	// 	/// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
	// 	/// `foo/bar_baz/x`.
	// 	List(prefix Path) (bufio.Scanner, ObjectMeta)

	// 	/// List objects with the given prefix and an implementation specific
	// 	/// delimiter. Returns common prefixes (directories) in addition to object
	// 	/// metadata.
	// 	///
	// 	/// Prefixes are evaluated on a path segment basis, i.e. `foo/bar/` is a prefix of `foo/bar/x` but not of
	// 	/// `foo/bar_baz/x`.
	// 	ListWithDelimiter(prefix Path) ListResult

	//// Copy an object from one path to another in the same object store.
	////
	//// If there exists an object at the destination, it will be overwritten.
	// Copy(from Path, to Path) error

	// 	/// Move an object from one path to another in the same object store.
	// 	///
	// 	/// By default, this is implemented as a copy and then delete source. It may not
	// 	/// check when deleting source that it was the same object that was originally copied.
	// 	///
	/// If there exists an object at the destination, it will be overwritten.
	Rename(from Path, to Path) error

	// 	/// Copy an object from one path to another, only if destination is empty.
	// 	///
	// 	/// Will return an error if the destination already has an object.
	// 	///
	// 	/// Performs an atomic operation if the underlying object storage supports it.
	// 	/// If atomic operations are not supported by the underlying object storage (like S3)
	// 	/// it will return an error.
	// 	CopyIfNotExists(from Path, to Path) error

	// Move an object from one path to another in the same object store.

	// Will return an error if the destination already has an object.
	RenameIfNotExists(from Path, to Path) error

	/// Allow ObjectReaderAtSeeker to support the ReaderAt io interface
	/// Excerpt from the ReaderAt comments:
	//
	// ReadAt reads len(p) bytes into p starting at offset off in the
	// underlying input source. It returns the number of bytes
	// read (0 <= n <= len(p)) and any error encountered.
	// ...
	// If ReadAt is reading from an input source with a seek offset,
	// ReadAt should not affect nor be affected by the underlying
	// seek offset.
	ReadAt(location Path, p []byte, off int64, max int64) (n int, err error)

	// Whether or not this store can be used as an io.Writer
	SupportsWriter() bool

	// Allow use of an ObjectStore as an io.Writer
	// If error is nil, then the returned function should be called with a defer to close resources
	// Writer may not be supported for all store types
	Writer(to Path, flag int) (io.Writer, func() error, error)

	// BaseURI gets a store's base URI.
	BaseURI() Path
}

// ListIterator is a wrapper around List that performs paging if required.
type ListIterator struct {
	store      ObjectStore
	prefix     Path
	listResult *ListResult
	nextIndex  int
}

// NewListIterator creates a new ListIterator instance.
func NewListIterator(prefix Path, store ObjectStore) *ListIterator {
	iterator := new(ListIterator)
	iterator.listResult = nil
	iterator.prefix = prefix
	iterator.store = store
	return iterator
}

// Next returns the next object in a list.
// When there are no more objects, return nil and the error ErrObjectDoesNotExist
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
		return nil, ErrObjectDoesNotExist
	}

	result := listIterator.listResult.Objects[listIterator.nextIndex]
	listIterator.nextIndex++
	return &result, nil
}

// Compile time check that ObjectReaderAtSeeker implements io.ReaderAt and io.Seeker
var _ io.ReaderAt = (*ObjectReaderAtSeeker)(nil)
var _ io.Seeker = (*ObjectReaderAtSeeker)(nil)

// ObjectReaderAtSeeker supports the IO interfaces Seeker, Reader, and ReaderAt.
type ObjectReaderAtSeeker struct {
	store    ObjectStore
	location Path
	offset   int64
	size     int64
}

// NewObjectReaderAtSeeker creates a new ObjectReaderAtSeeker instance.
func NewObjectReaderAtSeeker(location Path, store ObjectStore) (*ObjectReaderAtSeeker, error) {
	reader := new(ObjectReaderAtSeeker)
	reader.store = store
	reader.location = location
	meta, err := store.Head(location)
	if err != nil {
		return nil, err
	}
	reader.size = meta.Size
	return reader, nil
}

// ReadAt counts the number of bytes read from an object.
func (reader *ObjectReaderAtSeeker) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || off >= reader.size {
		return 0, io.EOF
	}

	max := off + int64(len(p))
	if max > reader.size {
		max = reader.size
	}
	return reader.store.ReadAt(reader.location, p, off, max)
}

func (reader *ObjectReaderAtSeeker) Read(p []byte) (n int, err error) {
	max := reader.size - reader.offset
	if max > int64(len(p)) {
		max = int64(len(p))
	}
	return reader.store.ReadAt(reader.location, p, reader.offset, max)
}

// Seek sets the offset for the next read or write to offset.
func (reader *ObjectReaderAtSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += reader.offset
	case io.SeekEnd:
		offset += reader.size
	default:
		return 0, ErrSeekWhence
	}
	if offset < 0 {
		return 0, ErrSeekOffset
	}
	reader.offset = offset
	return offset, nil
}
