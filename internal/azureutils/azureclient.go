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

// Package azureutils implements utilities used to interact with Azure Data Lake
// Storage Gen2 (ADLS Gen2 / hierarchical namespace).
package azureutils

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/file"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
)

var (
	// ErrNotFound is returned when a path does not exist.
	ErrNotFound error = errors.New("the path does not exist")
	// ErrAlreadyExists is returned when a path already exists.
	ErrAlreadyExists error = errors.New("the path already exists")
)

// ObjectProperties describes the metadata of a single path.
type ObjectProperties struct {
	// ContentLength is the size in bytes of the path.
	ContentLength int64
	// LastModified is the last modified time of the path.
	LastModified time.Time
}

// PathItem describes a single entry returned by a list operation.
type PathItem struct {
	// Name is the path relative to the filesystem (container) root.
	Name string
	// ContentLength is the size in bytes of the path.
	ContentLength int64
	// LastModified is the last modified time of the path.
	LastModified time.Time
	// IsDirectory reports whether the path is a directory.
	IsDirectory bool
}

// ListResult is the result of a list operation.
type ListResult struct {
	// Paths are the entries returned by the list operation.
	Paths []PathItem
	// NextMarker is the continuation token for the next page, if any.
	NextMarker string
}

// Client defines the subset of ADLS Gen2 operations used by the Azure object store.
// It is satisfied by RealClient and by the in-memory MockClient used in tests.
type Client interface {
	Upload(ctx context.Context, key string, data []byte) error
	Download(ctx context.Context, key string) ([]byte, error)
	DownloadRange(ctx context.Context, key string, offset int64, count int64) ([]byte, error)
	GetProperties(ctx context.Context, key string) (ObjectProperties, error)
	Delete(ctx context.Context, key string) error
	DeleteDirectory(ctx context.Context, key string) error
	List(ctx context.Context, prefix string, marker string) (ListResult, error)
	Rename(ctx context.Context, source string, destination string, overwrite bool) error
}

// RealClient is a Client backed by a real ADLS Gen2 filesystem (container).
//
// Mutating and point operations use the dfs endpoint (fileSystem), which
// provides atomic rename. Listing uses the blob endpoint (container), whose
// List Blobs operation supports true server-side prefix filtering — the dfs
// ListPaths operation only lists the direct contents of an existing directory,
// which cannot satisfy delta-go's S3-style prefix List contract on ADLS Gen2
// (HNS) and would force an O(directory) enumerate-and-filter.
type RealClient struct {
	fileSystem *filesystem.Client
	container  *container.Client
}

// Compile time check that RealClient implements Client.
var _ Client = (*RealClient)(nil)

// NewClient creates a RealClient for the given storage account and filesystem
// (container) using the provided token credential.
func NewClient(account string, fileSystemName string, cred azcore.TokenCredential, opts *filesystem.ClientOptions) (*RealClient, error) {
	dfsURL := fmt.Sprintf("https://%s.dfs.core.windows.net/%s", account, fileSystemName)
	fsClient, err := filesystem.NewClient(dfsURL, cred, opts)
	if err != nil {
		return nil, err
	}
	blobURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s", account, fileSystemName)
	containerClient, err := container.NewClient(blobURL, cred, nil)
	if err != nil {
		return nil, err
	}
	return &RealClient{fileSystem: fsClient, container: containerClient}, nil
}

// mapError translates ADLS Gen2 service errors into this package's sentinel errors.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	if datalakeerror.HasCode(err, datalakeerror.PathNotFound, datalakeerror.SourcePathNotFound) {
		return errors.Join(ErrNotFound, err)
	}
	if datalakeerror.HasCode(err, datalakeerror.PathAlreadyExists) {
		return errors.Join(ErrAlreadyExists, err)
	}
	// Some operations (GetProperties, DownloadStream) are served by the blob
	// endpoint, which reports a missing path as HTTP 404 BlobNotFound rather than
	// the dfs PathNotFound code that HasCode matches. Fall back to the HTTP
	// status so callers relying on ErrNotFound (e.g. Head-before-write guards)
	// classify these correctly.
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
		return errors.Join(ErrNotFound, err)
	}
	return err
}

// Upload writes data to the path at key, creating or overwriting it.
func (c *RealClient) Upload(ctx context.Context, key string, data []byte) error {
	fileClient := c.fileSystem.NewFileClient(key)
	// UploadBuffer only appends and flushes; the file (and any missing parent
	// directories) must be created first on ADLS Gen2.
	if _, err := fileClient.Create(ctx, nil); err != nil {
		return mapError(err)
	}
	if len(data) == 0 {
		return nil
	}
	return mapError(fileClient.UploadBuffer(ctx, data, nil))
}

// Download returns the bytes stored at key.
func (c *RealClient) Download(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.fileSystem.NewFileClient(key).DownloadStream(ctx, nil)
	if err != nil {
		return nil, mapError(err)
	}
	defer resp.Body.Close() //nolint:errcheck
	return io.ReadAll(resp.Body)
}

// DownloadRange returns count bytes stored at key starting at offset.
func (c *RealClient) DownloadRange(ctx context.Context, key string, offset int64, count int64) ([]byte, error) {
	resp, err := c.fileSystem.NewFileClient(key).DownloadStream(ctx, &file.DownloadStreamOptions{
		Range: &file.HTTPRange{Offset: offset, Count: count},
	})
	if err != nil {
		return nil, mapError(err)
	}
	defer resp.Body.Close() //nolint:errcheck
	return io.ReadAll(resp.Body)
}

// GetProperties returns the metadata of the path at key.
func (c *RealClient) GetProperties(ctx context.Context, key string) (ObjectProperties, error) {
	resp, err := c.fileSystem.NewFileClient(key).GetProperties(ctx, nil)
	if err != nil {
		return ObjectProperties{}, mapError(err)
	}
	var props ObjectProperties
	if resp.ContentLength != nil {
		props.ContentLength = *resp.ContentLength
	}
	if resp.LastModified != nil {
		props.LastModified = *resp.LastModified
	}
	return props, nil
}

// Delete removes the file at key.
func (c *RealClient) Delete(ctx context.Context, key string) error {
	_, err := c.fileSystem.NewFileClient(key).Delete(ctx, nil)
	return mapError(err)
}

// DeleteDirectory recursively removes the directory at key.
func (c *RealClient) DeleteDirectory(ctx context.Context, key string) error {
	// The DFS API rejects directory paths with a trailing slash (InvalidUri).
	key = strings.TrimSuffix(key, "/")
	_, err := c.fileSystem.NewDirectoryClient(key).Delete(ctx, nil)
	return mapError(err)
}

// List returns a single page of paths whose name begins with prefix, resuming
// from marker if set.
//
// It uses the blob endpoint's List Blobs (flat) operation, which performs true
// server-side prefix filtering — the final path segment may be a partial file
// name (e.g. "_delta_log/00000000000000000066" matches
// "…000066.checkpoint.parquet"). The dfs ListPaths operation cannot do this:
// its Prefix maps to the REST "directory" parameter and only lists the direct
// contents of an existing directory (404ing on a non-directory prefix), which
// would force an O(directory) enumerate-and-filter and breaks paging when the
// prefix is a file-name prefix rather than a real directory.
func (c *RealClient) List(ctx context.Context, prefix string, marker string) (ListResult, error) {
	opts := &container.ListBlobsFlatOptions{
		Include: container.ListBlobsInclude{Metadata: true},
	}
	if prefix != "" {
		opts.Prefix = &prefix
	}
	if marker != "" {
		opts.Marker = &marker
	}
	pager := c.container.NewListBlobsFlatPager(opts)

	var result ListResult
	if !pager.More() {
		return result, nil
	}
	page, err := pager.NextPage(ctx)
	if err != nil {
		return ListResult{}, mapError(err)
	}
	if page.Segment != nil {
		for _, b := range page.Segment.BlobItems {
			if b == nil || b.Name == nil {
				continue
			}
			item := PathItem{Name: *b.Name}
			// HNS directory placeholders surface as zero-length blobs tagged
			// with metadata hdi_isfolder=true; report them as directories so
			// callers can skip them, matching the dfs ListPaths behavior.
			for k, v := range b.Metadata {
				if strings.EqualFold(k, "hdi_isfolder") && v != nil && strings.EqualFold(*v, "true") {
					item.IsDirectory = true
					break
				}
			}
			if b.Properties != nil {
				if b.Properties.ContentLength != nil {
					item.ContentLength = *b.Properties.ContentLength
				}
				if b.Properties.LastModified != nil {
					item.LastModified = *b.Properties.LastModified
				}
			}
			result.Paths = append(result.Paths, item)
		}
	}
	if page.NextMarker != nil {
		result.NextMarker = *page.NextMarker
	}
	return result, nil
}

// Rename moves the path at source to destination. When overwrite is false, the
// operation fails with ErrAlreadyExists if destination already exists, using an
// atomic If-None-Match precondition.
func (c *RealClient) Rename(ctx context.Context, source string, destination string, overwrite bool) error {
	var opts *file.RenameOptions
	if !overwrite {
		etag := azcore.ETagAny
		opts = &file.RenameOptions{
			AccessConditions: &file.AccessConditions{
				ModifiedAccessConditions: &file.ModifiedAccessConditions{
					IfNoneMatch: &etag,
				},
			},
		}
	}
	_, err := c.fileSystem.NewFileClient(source).Rename(ctx, destination, opts)
	return mapError(err)
}
