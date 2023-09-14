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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rivian/delta-go/internal/s3utils"
	"github.com/rivian/delta-go/storage"
)

// type filePutter func(key string, data io.ReadSeeker, creds *credentials.Credentials) error
type S3ObjectStore struct {
	// Source object key
	Client  s3utils.S3Client
	BaseURI storage.Path
	baseURL *url.URL
	bucket  string
	path    string
	//s3, http, file
	scheme string
}

// Compile time check that S3ObjectStore implements storage.ObjectStore
var _ storage.ObjectStore = (*S3ObjectStore)(nil)

func New(client s3utils.S3Client, baseURI storage.Path) (*S3ObjectStore, error) {
	store := new(S3ObjectStore)
	store.Client = client
	store.BaseURI = baseURI

	var err error
	store.baseURL, err = baseURI.ParseURL()
	if err != nil {
		return nil, err
	}

	store.bucket = store.baseURL.Host
	store.path = strings.TrimPrefix(store.baseURL.Path, "/")
	store.scheme = store.baseURL.Scheme

	return store, nil
}

func (s *S3ObjectStore) Put(location storage.Path, data []byte) error {
	key, err := url.JoinPath(s.path, location.Raw)
	if err != nil {
		return errors.Join(storage.ErrorURLJoinPath, err)
	}
	_, err = s.Client.PutObject(context.Background(),
		&s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
	if err != nil {
		return errors.Join(storage.ErrorPutObject, err)
	}
	return nil

}

func (s *S3ObjectStore) Get(location storage.Path) ([]byte, error) {
	key, err := url.JoinPath(s.path, location.Raw)
	if err != nil {
		return nil, errors.Join(storage.ErrorURLJoinPath, err)
	}
	// Get the object from S3.
	resp, err := s.Client.GetObject(context.Background(),
		&s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
	// Check for a 404 response, indicating that the object does not exist
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound {
		return nil, errors.Join(storage.ErrorObjectDoesNotExist, err)
	}
	if err != nil {
		return nil, errors.Join(storage.ErrorGetObject, err)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Join(storage.ErrorGetObject, err)
	}
	return bodyBytes, nil
}

func (s *S3ObjectStore) Delete(location storage.Path) error {
	key, err := url.JoinPath(s.path, location.Raw)
	if err != nil {
		return errors.Join(storage.ErrorURLJoinPath, err)
	}
	_, err = s.Client.DeleteObject(context.Background(),
		&s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		return errors.Join(storage.ErrorDeleteObject, err)
	}
	return nil
}

func (s *S3ObjectStore) RenameIfNotExists(from storage.Path, to storage.Path) error {
	// return ErrorObjectAlreadyExists if the destination file exists
	_, err := s.Head(to)
	if !errors.Is(err, storage.ErrorObjectDoesNotExist) {
		return errors.Join(storage.ErrorObjectAlreadyExists, fmt.Errorf("object at location %s already exists", to.Raw))
	}

	err = s.Rename(from, to)
	if err != nil {
		return err
	}
	return nil
}

func (s *S3ObjectStore) Rename(from storage.Path, to storage.Path) error {
	srcKey, err := url.JoinPath(s.path, from.Raw)
	if err != nil {
		return errors.Join(storage.ErrorURLJoinPath, err)
	}
	// The CopySource parameter needs to include the bucket.
	// However, we can't use url.JoinPath() since it will drop any leading / in the key.
	srcKey = s.bucket + "/" + srcKey
	destKey, err := url.JoinPath(s.path, to.Raw)
	if err != nil {
		return errors.Join(storage.ErrorURLJoinPath, err)
	}
	_, err = s.Client.CopyObject(context.Background(),
		&s3.CopyObjectInput{
			Bucket:                aws.String(s.bucket),
			Key:                   aws.String(destKey),
			CopySource:            aws.String(srcKey),
			CopySourceIfNoneMatch: aws.String("null"),
		})
	if err != nil {
		return errors.Join(storage.ErrorCopyObject, err)
	}
	err = s.Delete(from)
	if err != nil {
		return errors.Join(storage.ErrorDeleteObject, err)
	}
	return nil
}

func (s *S3ObjectStore) Head(location storage.Path) (storage.ObjectMeta, error) {
	var m storage.ObjectMeta
	key, err := url.JoinPath(s.path, location.Raw)
	if err != nil {
		return m, errors.Join(storage.ErrorURLJoinPath, err)
	}
	result, err := s.Client.HeadObject(context.Background(),
		&s3.HeadObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
	// Check for a 404 response, indicating that the object does not exist
	var re *awshttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound {
		return m, errors.Join(storage.ErrorObjectDoesNotExist, err)
	}
	if err != nil {
		return m, errors.Join(storage.ErrorHeadObject, err)
	}

	m.Location = location
	m.LastModified = *result.LastModified
	m.Size = result.ContentLength

	return m, nil
}

func getListInputAndTrimPrefix(s *S3ObjectStore, prefix storage.Path, previousResult *storage.ListResult) (s3.ListObjectsV2Input, string, error) {
	// We will need the store path with the trailing / for trimming results
	pathWithSeparators := s.path
	if !strings.HasSuffix(pathWithSeparators, "/") {
		pathWithSeparators = pathWithSeparators + "/"
	}

	// The fullPrefix prepends the store path so that AWS uses the entire path for
	// pattern matching the key.
	var fullPrefix string
	var err error

	if prefix.Raw == "" {
		// If the prefix is empty, use path with a trailing / to avoid listing anything
		// outside of our store path that starts with the same string.
		// (e.g. if our store folder is /data/ and we also have /data_out.txt, etc.)
		fullPrefix = pathWithSeparators
	} else {
		fullPrefix, err = url.JoinPath(s.path, prefix.Raw)
		if err != nil {
			return s3.ListObjectsV2Input{}, "", errors.Join(storage.ErrorURLJoinPath, err)
		}
	}

	listInput := s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	}

	// Request the next page if a token is available
	if previousResult != nil && previousResult.NextToken != "" {
		listInput.ContinuationToken = &previousResult.NextToken
	}
	return listInput, pathWithSeparators, nil
}

func (s *S3ObjectStore) List(prefix storage.Path, previousResult *storage.ListResult) (storage.ListResult, error) {
	listInput, resultsTrimPrefix, err := getListInputAndTrimPrefix(s, prefix, previousResult)
	if err != nil {
		return storage.ListResult{}, err
	}
	results, err := s.Client.ListObjectsV2(context.Background(), &listInput)
	if err != nil {
		return storage.ListResult{}, errors.Join(storage.ErrorListObjects, err)
	}

	listResult := storage.ListResult{Objects: make([]storage.ObjectMeta, 0, results.KeyCount)}

	for _, result := range results.Contents {
		location := strings.TrimPrefix(*result.Key, resultsTrimPrefix)
		listResult.Objects = append(listResult.Objects, storage.ObjectMeta{
			Location:     storage.NewPath(location),
			LastModified: *result.LastModified,
			Size:         result.Size,
		})
	}
	if results.NextContinuationToken != nil {
		listResult.NextToken = *results.NextContinuationToken
	}
	return listResult, nil
}

func (s *S3ObjectStore) ListAll(prefix storage.Path) (storage.ListResult, error) {
	var listResult storage.ListResult
	listInput, resultsTrimPrefix, err := getListInputAndTrimPrefix(s, prefix, nil)
	if err != nil {
		return listResult, err
	}
	p := s3.NewListObjectsV2Paginator(s.Client, &listInput)

	for p.HasMorePages() {
		page, err := p.NextPage(context.TODO())
		if err != nil {
			return listResult, errors.Join(storage.ErrorListObjects, err)
		}

		for _, result := range page.Contents {
			location := strings.TrimPrefix(*result.Key, resultsTrimPrefix)
			listResult.Objects = append(listResult.Objects, storage.ObjectMeta{
				Location:     storage.NewPath(location),
				LastModified: *result.LastModified,
				Size:         result.Size,
			})
		}
	}

	return listResult, nil
}

func (s *S3ObjectStore) IsListOrdered() bool {
	return true
}
