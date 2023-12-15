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
package s3utils

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/filestore"
)

type MockClient struct {
	// Use a FileObjectStore to mock S3 storage
	fileStore *filestore.FileObjectStore
	// The S3 store path
	s3StorePath string
	// For testing: if MockError is set, any S3ClientAPI function called will return that error
	MockError error
	// For testing: enable pagination
	PaginateListResults bool
	// For testing: disable object copying
	DisableObjectCopying bool
	// For testing: disable object deleting
	DisableObjectDeleting bool
}

// Compile time check that MockS3Client implements S3Client
var _ Client = (*MockClient)(nil)

// NewMockClient creates a mock S3 client that uses a filestore in a temporary directory to
// store, retrieve, and manipulate files
func NewMockClient(t *testing.T, baseURI storage.Path) (*MockClient, error) {
	tmpDir := t.TempDir()
	tmpPath := storage.NewPath(tmpDir)
	var fileStore filestore.FileObjectStore
	fileStore.SetBaseURI(tmpPath)
	client := new(MockClient)
	client.fileStore = &fileStore
	// The mock client needs information about the S3 store's path to avoid edge cases during List
	baseURL, err := baseURI.ParseURL()
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(baseURL.Path, "/") {
		client.s3StorePath = baseURL.Path
	} else {
		client.s3StorePath = baseURL.Path + "/"
	}
	return client, nil
}

// FileStore gets the file store.
func (m *MockClient) FileStore() *filestore.FileObjectStore {
	return m.fileStore
}

// S3StorePath gets the S3 store path.
func (m *MockClient) S3StorePath() string {
	return m.s3StorePath
}

// SetFileStore sets the file store.
func (m *MockClient) SetFileStore(store *filestore.FileObjectStore) {
	m.fileStore = store
}

// SetS3StorePath sets the S3 store path.
func (m *MockClient) SetS3StorePath(path string) {
	m.s3StorePath = path
}

// getFilePathFromS3Input generates the local file path from the S3 bucket and key
func getFilePathFromS3Input(bucket string, key string) (storage.Path, error) {
	filePath, err := url.JoinPath(bucket, key)
	if err != nil {
		return storage.NewPath(""), err
	}
	return storage.NewPath(filePath), nil
}

func (m *MockClient) HeadObject(ctx context.Context, input *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if m.MockError != nil {
		return nil, m.MockError
	}

	filePath, err := getFilePathFromS3Input(*input.Bucket, *input.Key)
	if err != nil {
		return nil, err
	}
	meta, err := m.fileStore.Head(filePath)
	if err != nil {
		return nil, err
	}

	headObjectOutput := new(s3.HeadObjectOutput)
	headObjectOutput.LastModified = &meta.LastModified
	headObjectOutput.ContentLength = aws.Int64(meta.Size)
	return headObjectOutput, nil
}

func (m *MockClient) PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.MockError != nil {
		return nil, m.MockError
	}

	filePath, err := getFilePathFromS3Input(*input.Bucket, *input.Key)
	if err != nil {
		return nil, err
	}
	buffer := new(bytes.Buffer)
	buffer.ReadFrom(input.Body)
	err = m.fileStore.Put(filePath, buffer.Bytes())

	putObjectOutput := new(s3.PutObjectOutput)
	return putObjectOutput, err
}

func (m *MockClient) GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.MockError != nil {
		return nil, m.MockError
	}

	filePath, err := getFilePathFromS3Input(*input.Bucket, *input.Key)
	if err != nil {
		return nil, err
	}
	data, err := m.fileStore.Get(filePath)
	if err != nil {
		return nil, err
	}

	if input.Range != nil && len(*input.Range) > 0 {
		rangeRegexp := regexp.MustCompile(`^bytes=(\d+)-(\d+)$`)
		groups := rangeRegexp.FindStringSubmatch(*input.Range)
		if groups == nil {
			return nil, errors.New("invalid range")
		}
		if groups != nil {
			off, err := strconv.ParseInt(groups[1], 10, 64)
			if err != nil {
				return nil, err
			}
			max, err := strconv.ParseInt(groups[2], 10, 64)
			if err != nil {
				return nil, err
			}
			if max > int64(len(data)) {
				return nil, errors.Join(io.EOF, errors.New("invalid max"))
			}
			data = data[off : max+1]
		}
	}

	getObjectOutput := new(s3.GetObjectOutput)
	getObjectOutput.Body = io.NopCloser(bytes.NewReader(data))
	getObjectOutput.ContentLength = aws.Int64(int64(len(data)))
	return getObjectOutput, nil
}

func (m *MockClient) CopyObject(ctx context.Context, input *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	if m.DisableObjectCopying {
		return nil, storage.ErrCopyObject
	}

	if m.MockError != nil {
		return nil, m.MockError
	}

	// The CopySource includes the bucket
	srcPath := storage.NewPath(*input.CopySource)

	destPath, err := getFilePathFromS3Input(*input.Bucket, *input.Key)
	if err != nil {
		return nil, err
	}
	data, err := m.fileStore.Get(srcPath)
	if err != nil {
		return nil, err
	}
	err = m.fileStore.Put(destPath, data)
	if err != nil {
		return nil, err
	}

	copyObjectOutput := new(s3.CopyObjectOutput)
	return copyObjectOutput, nil
}

func (m *MockClient) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.DisableObjectDeleting {
		return nil, storage.ErrDeleteObject
	}

	if m.MockError != nil {
		return nil, m.MockError
	}

	filePath, err := getFilePathFromS3Input(*input.Bucket, *input.Key)
	if err != nil {
		return nil, err
	}
	err = m.fileStore.Delete(filePath)
	if err != nil {
		return nil, err
	}

	deleteObjectOutput := new(s3.DeleteObjectOutput)
	return deleteObjectOutput, nil
}

func (m *MockClient) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.MockError != nil {
		return nil, m.MockError
	}

	prefix, err := getFilePathFromS3Input(*input.Bucket, *input.Prefix)
	if err != nil {
		return nil, err
	}

	output, err := m.fileStore.List(prefix, nil)
	if err != nil {
		return nil, err
	}

	listObjectsOutput := new(s3.ListObjectsV2Output)

	var outputCount int
	var offset int
	if m.PaginateListResults {
		page := 1000
		if aws.ToInt32(input.MaxKeys) != 0 {
			page = int(aws.ToInt32(input.MaxKeys))
		}
		outputCount = page
		remaining := len(output.Objects)
		if input.ContinuationToken != nil && len(*input.ContinuationToken) > 0 {
			offset, err = strconv.Atoi(*input.ContinuationToken)
			if err == nil {
				remaining = remaining - offset
			}
			if remaining < 0 {
				remaining = 0
			}
		}
		if remaining < outputCount {
			outputCount = remaining
		}
		if remaining > outputCount {
			nextToken := fmt.Sprintf("%d", offset+page)
			listObjectsOutput.NextContinuationToken = &nextToken
		}
	} else {
		outputCount = len(output.Objects)
	}
	listObjectsOutput.Contents = make([]types.Object, 0, outputCount)
	trimmedStorePath := strings.TrimPrefix(m.s3StorePath, "/")
	for i := 0; i < outputCount; i++ {
		r := output.Objects[offset+i]
		key := strings.TrimPrefix(r.Location.Raw, *input.Bucket+"/")
		if key != trimmedStorePath {
			lastModified := r.LastModified
			listObjectsOutput.Contents = append(listObjectsOutput.Contents, types.Object{
				Key:          &key,
				Size:         aws.Int64(r.Size),
				LastModified: &lastModified})
		}
	}
	listObjectsOutput.KeyCount = aws.Int32(int32(len(listObjectsOutput.Contents)))
	return listObjectsOutput, nil
}

// getFilePath returns the path of the location on the baseURI, ignoring the URI scheme
func getFilePath(baseURI storage.Path, location storage.Path) (storage.Path, error) {
	baseURL, err := baseURI.ParseURL()
	if err != nil {
		return storage.NewPath(""), err
	}
	path, err := url.JoinPath(baseURL.Host, baseURL.Path, location.Raw)
	return storage.NewPath(path), err
}

// getFile returns a file from the underlying filestore, for use in unit tests
func (m *MockClient) GetFile(baseURI storage.Path, location storage.Path) ([]byte, error) {
	filePath, err := getFilePath(baseURI, location)
	if err != nil {
		return nil, err
	}
	return m.fileStore.Get(filePath)
}

// putFile writes data to a file in the underlying filestore for use in unit tests
func (m *MockClient) PutFile(baseURI storage.Path, location storage.Path, data []byte) error {
	filePath, err := getFilePath(baseURI, location)
	if err != nil {
		return err
	}
	return m.fileStore.Put(filePath, data)
}

// fileExists checks if a file exists in the underlying filestore for use in unit tests
func (m *MockClient) FileExists(baseURI storage.Path, location storage.Path) (bool, error) {
	filePath, err := getFilePath(baseURI, location)
	if err != nil {
		return false, err
	}
	_, err = m.fileStore.Head(filePath)
	if errors.Is(err, storage.ErrObjectDoesNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
