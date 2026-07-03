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

// Package azurestore contains the resources required to interact with an Azure
// Data Lake Storage Gen2 (ADLS Gen2 / hierarchical namespace) store.
package azurestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	"github.com/rivian/delta-go/internal/azureutils"
	"github.com/rivian/delta-go/storage"
)

// ErrUnsupportedScheme is returned when a base URI uses an unsupported scheme.
var ErrUnsupportedScheme error = errors.New("unsupported URI scheme, expected az:// or abfss://")

// ErrMissingAccount is returned when the storage account cannot be determined.
var ErrMissingAccount error = errors.New("storage account could not be determined from the URI or AZURE_STORAGE_ACCOUNT")

// AzureObjectStore allows interaction with an ADLS Gen2 object store.
type AzureObjectStore struct {
	// Client communicates with ADLS Gen2.
	Client  azureutils.Client
	baseURI storage.Path
	account string
	// container is the ADLS Gen2 filesystem name.
	container string
	// path is the location within the container that the store reads and writes under.
	path string
	// scheme is the URI scheme (az, abfss, etc.).
	scheme string
}

// Compile time check that AzureObjectStore implements storage.ObjectStore.
var _ storage.ObjectStore = (*AzureObjectStore)(nil)

// New creates a new AzureObjectStore from an injected client and base URI.
func New(client azureutils.Client, baseURI storage.Path) (*AzureObjectStore, error) {
	account, container, path, scheme, err := parseAzureURI(baseURI)
	if err != nil {
		return nil, err
	}
	store := &AzureObjectStore{
		Client:    client,
		baseURI:   baseURI,
		account:   account,
		container: container,
		path:      path,
		scheme:    scheme,
	}
	return store, nil
}

// NewWithCredential creates a new AzureObjectStore that talks to ADLS Gen2 using
// the provided token credential (e.g. azidentity.DefaultAzureCredential). When the
// base URI does not carry the storage account (az:// scheme), the account is read
// from the AZURE_STORAGE_ACCOUNT environment variable.
func NewWithCredential(baseURI storage.Path, cred azcore.TokenCredential, opts *filesystem.ClientOptions) (*AzureObjectStore, error) {
	account, container, _, _, err := parseAzureURI(baseURI)
	if err != nil {
		return nil, err
	}
	if account == "" {
		account = os.Getenv("AZURE_STORAGE_ACCOUNT")
	}
	if account == "" {
		return nil, ErrMissingAccount
	}
	client, err := azureutils.NewClient(account, container, cred, opts)
	if err != nil {
		return nil, err
	}
	return New(client, baseURI)
}

// parseAzureURI extracts the account, container, store path, and scheme from a base URI.
// Supported forms:
//
//	az://container/path
//	abfss://container@account.dfs.core.windows.net/path
func parseAzureURI(baseURI storage.Path) (account string, container string, path string, scheme string, err error) {
	u, err := baseURI.ParseURL()
	if err != nil {
		return "", "", "", "", err
	}
	scheme = u.Scheme
	switch scheme {
	case "az":
		container = u.Host
		if u.User != nil {
			container = u.User.Username()
			account = hostAccount(u.Host)
		}
	case "abfs", "abfss", "wasb", "wasbs":
		if u.User == nil || u.User.Username() == "" {
			return "", "", "", "", fmt.Errorf("%w: %s is missing the container@account form", ErrUnsupportedScheme, baseURI.Raw)
		}
		container = u.User.Username()
		account = hostAccount(u.Host)
	default:
		return "", "", "", "", fmt.Errorf("%w: %s", ErrUnsupportedScheme, baseURI.Raw)
	}
	path = strings.Trim(u.Path, "/")
	return account, container, path, scheme, nil
}

// hostAccount returns the storage account name from a host like account.dfs.core.windows.net.
func hostAccount(host string) string {
	return strings.Split(host, ".")[0]
}

// key returns the container-relative key for a location.
func (s *AzureObjectStore) key(location storage.Path) string {
	loc := strings.TrimPrefix(location.Raw, "/")
	if s.path == "" {
		return loc
	}
	if loc == "" {
		return s.path
	}
	return s.path + "/" + loc
}

// listInputs returns the full list prefix and the prefix to trim from results.
func (s *AzureObjectStore) listInputs(prefix storage.Path) (fullPrefix string, trimPrefix string) {
	pathWithSeparator := s.path
	if pathWithSeparator != "" && !strings.HasSuffix(pathWithSeparator, "/") {
		pathWithSeparator += "/"
	}
	trimPrefix = pathWithSeparator
	if prefix.Raw == "" {
		fullPrefix = pathWithSeparator
	} else {
		fullPrefix = s.key(prefix)
	}
	return fullPrefix, trimPrefix
}

// Put adds an object to the store.
func (s *AzureObjectStore) Put(location storage.Path, data []byte) error {
	if err := s.Client.Upload(context.Background(), s.key(location), data); err != nil {
		return errors.Join(storage.ErrPutObject, err)
	}
	return nil
}

// Get retrieves an object.
func (s *AzureObjectStore) Get(location storage.Path) ([]byte, error) {
	data, err := s.Client.Download(context.Background(), s.key(location))
	if errors.Is(err, azureutils.ErrNotFound) {
		return nil, errors.Join(storage.ErrObjectDoesNotExist, err)
	}
	if err != nil {
		return nil, errors.Join(storage.ErrGetObject, err)
	}
	return data, nil
}

// Head retrieves metadata from an object without returning the object itself.
func (s *AzureObjectStore) Head(location storage.Path) (storage.ObjectMeta, error) {
	var m storage.ObjectMeta
	props, err := s.Client.GetProperties(context.Background(), s.key(location))
	if errors.Is(err, azureutils.ErrNotFound) {
		return m, errors.Join(storage.ErrObjectDoesNotExist, err)
	}
	if err != nil {
		return m, errors.Join(storage.ErrHeadObject, err)
	}
	m.Location = location
	m.LastModified = props.LastModified
	m.Size = props.ContentLength
	return m, nil
}

// Delete removes an object from the store.
func (s *AzureObjectStore) Delete(location storage.Path) error {
	err := s.Client.Delete(context.Background(), s.key(location))
	if errors.Is(err, azureutils.ErrNotFound) {
		return errors.Join(storage.ErrObjectDoesNotExist, err)
	}
	if err != nil {
		return errors.Join(storage.ErrDeleteObject, err)
	}
	return nil
}

// DeleteFolder recursively removes objects under a prefix.
func (s *AzureObjectStore) DeleteFolder(location storage.Path) error {
	err := s.Client.DeleteDirectory(context.Background(), s.key(location))
	if errors.Is(err, azureutils.ErrNotFound) {
		return errors.Join(storage.ErrObjectDoesNotExist, err)
	}
	if err != nil {
		return errors.Join(storage.ErrDeleteObject, err)
	}
	return nil
}

// List returns the complete set of objects whose key begins with prefix in a
// single call (NextToken in the returned result is always empty).
//
// NOTE: the previousResult argument is NOT respected. Unlike a paging store,
// this implementation returns every match in one call and always sets
// NextToken to "", so there is never a "next page" to resume. It exists only to
// satisfy the storage.ObjectStore interface. Because the whole result set is
// returned up front (and both in-tree consumers, ListAll and ListIterator, only
// resume while NextToken != ""), a non-nil previousResult means "you already
// have everything" and yields an empty result — it never drops rows. Do not
// build a caller that pages this store by feeding previousResult back in while
// ignoring NextToken; use ListAll or check NextToken.
//
// The underlying client lists via the blob endpoint, which performs true
// server-side prefix filtering, so no directory/parent handling is required and
// the cost is O(#matching) rather than O(#entries in the directory). Pages are
// drained internally with the same stable prefix so the continuation token
// always matches the request it was minted for.
func (s *AzureObjectStore) List(prefix storage.Path, previousResult *storage.ListResult) (storage.ListResult, error) {
	// previousResult is ignored (see the doc comment): everything is returned by
	// the first call, so a caller that supplies one has nothing more to fetch.
	if previousResult != nil {
		return storage.ListResult{}, nil
	}
	fullPrefix, trimPrefix := s.listInputs(prefix)

	var objects []storage.ObjectMeta
	marker := ""
	for {
		res, err := s.Client.List(context.Background(), fullPrefix, marker)
		if err != nil {
			return storage.ListResult{}, errors.Join(storage.ErrListObjects, err)
		}
		for _, p := range res.Paths {
			if p.IsDirectory {
				continue
			}
			objects = append(objects, storage.ObjectMeta{
				Location:     storage.NewPath(strings.TrimPrefix(p.Name, trimPrefix)),
				LastModified: p.LastModified,
				Size:         p.ContentLength,
			})
		}
		if res.NextMarker == "" {
			break
		}
		marker = res.NextMarker
	}
	return storage.ListResult{Objects: objects}, nil
}

// ListAll lists all objects under a prefix, paging as required.
func (s *AzureObjectStore) ListAll(prefix storage.Path) (storage.ListResult, error) {
	var listResult storage.ListResult
	var previousResult *storage.ListResult
	for {
		result, err := s.List(prefix, previousResult)
		if err != nil {
			return storage.ListResult{}, err
		}
		listResult.Objects = append(listResult.Objects, result.Objects...)
		if result.NextToken == "" {
			break
		}
		previousResult = &result
	}
	listResult.NextToken = ""
	return listResult, nil
}

// IsListOrdered returns true; ADLS Gen2 returns paths in lexicographic order.
func (s *AzureObjectStore) IsListOrdered() bool {
	return true
}

// Rename renames an object, overwriting the destination if it exists.
func (s *AzureObjectStore) Rename(from storage.Path, to storage.Path) error {
	err := s.Client.Rename(context.Background(), s.key(from), s.key(to), true)
	if errors.Is(err, azureutils.ErrNotFound) {
		return errors.Join(storage.ErrObjectDoesNotExist, err)
	}
	if err != nil {
		return errors.Join(storage.ErrCopyObject, err)
	}
	return nil
}

// RenameIfNotExists renames an object only if the destination does not exist.
// ADLS Gen2 performs this atomically using an If-None-Match precondition.
func (s *AzureObjectStore) RenameIfNotExists(from storage.Path, to storage.Path) error {
	err := s.Client.Rename(context.Background(), s.key(from), s.key(to), false)
	if errors.Is(err, azureutils.ErrAlreadyExists) {
		return errors.Join(storage.ErrObjectAlreadyExists, fmt.Errorf("object at location %s already exists", to.Raw))
	}
	if errors.Is(err, azureutils.ErrNotFound) {
		return errors.Join(storage.ErrObjectDoesNotExist, err)
	}
	if err != nil {
		return errors.Join(storage.ErrCopyObject, err)
	}
	return nil
}

// ReadAt reads up to len(p) bytes from an object starting at offset off.
func (s *AzureObjectStore) ReadAt(location storage.Path, p []byte, off int64, max int64) (n int, err error) {
	if off < 0 {
		return 0, errors.Join(storage.ErrReadAt, errors.New("offset should not be negative"))
	}
	data, err := s.Client.DownloadRange(context.Background(), s.key(location), off, int64(len(p)))
	if errors.Is(err, azureutils.ErrNotFound) {
		return 0, errors.Join(storage.ErrObjectDoesNotExist, err)
	}
	if err != nil {
		return 0, errors.Join(storage.ErrReadAt, err)
	}
	n = copy(p, data)
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

// SupportsWriter returns false; the Azure store does not support streaming writes.
func (s *AzureObjectStore) SupportsWriter() bool {
	return false
}

// Writer returns an operation not supported error.
func (s *AzureObjectStore) Writer(to storage.Path, flag int) (io.Writer, func() error, error) {
	return nil, nil, storage.ErrOperationNotSupported
}

// BaseURI gets the store's base URI.
func (s *AzureObjectStore) BaseURI() storage.Path {
	return s.baseURI
}
