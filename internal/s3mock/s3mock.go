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
package s3mock

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/filestore"
)

type S3MockClient struct {
	// Use a FileObjectStore to mock S3 storage
	fileStore filestore.FileObjectStore
	// The S3 store path
	s3StorePath string
	// For testing: if MockError is set, any S3ClientAPI function called will return that error
	MockError error
}

// newS3MockClient creates a mock S3 client that uses a filestore in a temporary directory to
// store, retrieve, and manipulate files
func NewS3MockClient(t *testing.T, baseURI *storage.Path) (*S3MockClient, error) {
	tmpDir := t.TempDir()
	tmpPath := storage.NewPath(tmpDir)
	fileStore := filestore.FileObjectStore{BaseURI: tmpPath}
	client := new(S3MockClient)
	client.fileStore = fileStore
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

// getFilePathFromS3Input generates the local file path from the S3 bucket and key
func getFilePathFromS3Input(bucket string, key string) (*storage.Path, error) {
	filePath, err := url.JoinPath(bucket, key)
	if err != nil {
		return nil, err
	}
	return storage.NewPath(filePath), nil
}

func (m *S3MockClient) HeadObject(ctx context.Context, input *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
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
	headObjectOutput.ContentLength = meta.Size
	return headObjectOutput, nil
}

func (m *S3MockClient) PutObject(ctx context.Context, input *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
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

func (m *S3MockClient) GetObject(ctx context.Context, input *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
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

	getObjectOutput := new(s3.GetObjectOutput)
	getObjectOutput.Body = io.NopCloser(bytes.NewReader(data))
	getObjectOutput.ContentLength = int64(len(data))
	return getObjectOutput, nil
}

func (m *S3MockClient) CopyObject(ctx context.Context, input *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
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

func (m *S3MockClient) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
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

func (m *S3MockClient) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
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
	listObjectsOutput.Contents = make([]types.Object, 0, len(output.Objects))
	trimmedStorePath := strings.TrimPrefix(m.s3StorePath, "/")
	for _, r := range output.Objects {
		key := strings.TrimPrefix(r.Location.Raw, *input.Bucket+"/")
		if key != trimmedStorePath {
			lastModified := r.LastModified
			listObjectsOutput.Contents = append(listObjectsOutput.Contents, types.Object{
				Key:          &key,
				Size:         r.Size,
				LastModified: &lastModified})
		}
	}
	listObjectsOutput.KeyCount = int32(len(listObjectsOutput.Contents))
	return listObjectsOutput, nil
}

// getFilePath returns the path of the location on the baseURI, ignoring the URI scheme
func getFilePath(baseURI *storage.Path, location *storage.Path) (*storage.Path, error) {
	baseURL, err := baseURI.ParseURL()
	if err != nil {
		return nil, err
	}
	path, err := url.JoinPath(baseURL.Host, baseURL.Path, location.Raw)
	return storage.NewPath(path), err
}

// getFile returns a file from the underlying filestore, for use in unit tests
func (m *S3MockClient) GetFile(baseURI *storage.Path, location *storage.Path) ([]byte, error) {
	filePath, err := getFilePath(baseURI, location)
	if err != nil {
		return nil, err
	}
	return m.fileStore.Get(filePath)
}

// putFile writes data to a file in the underlying filestore for use in unit tests
func (m *S3MockClient) PutFile(baseURI *storage.Path, location *storage.Path, data []byte) error {
	filePath, err := getFilePath(baseURI, location)
	if err != nil {
		return err
	}
	return m.fileStore.Put(filePath, data)
}

// fileExists checks if a file exists in the underlying filestore for use in unit tests
func (m *S3MockClient) FileExists(baseURI *storage.Path, location *storage.Path) (bool, error) {
	filePath, err := getFilePath(baseURI, location)
	if err != nil {
		return false, err
	}
	_, err = m.fileStore.Head(filePath)
	if errors.Is(err, storage.ErrorObjectDoesNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
