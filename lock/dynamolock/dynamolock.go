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

// Package dynamolock contains the resources required a create a DynamoDB lock.
package dynamolock

import (
	"errors"
	"time"

	"cirello.io/dynamolock/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/rivian/delta-go/dynamodbutils"
	"github.com/rivian/delta-go/lock"
)

// Attribute represents attribute names in DynamoDB items.
type Attribute string

const (
	key                                Attribute     = "key"
	defaultTTL                         time.Duration = 60 * time.Second
	defaultHeartbeat                   time.Duration = 1 * time.Second
	defaultMaxRetryTableCreateAttempts uint16        = 20
	defaultRCU                         int64         = 5
	defaultWCU                         int64         = 5
)

// DynamoLock represents a lock for a key stored as a single DynamoDB entry.
type DynamoLock struct {
	tableName    string
	lockClient   *dynamolock.Client
	lockedItem   *dynamolock.Lock
	key          string
	dynamoClient dynamodbutils.Client
	opts         Options
}

// Compile time check that FileLock implements lock.Locker
var _ lock.Locker = (*DynamoLock)(nil)

// Options contains settings that can be adjusted to change the behavior of a DynamoDB lock.
type Options struct {
	// The amount of time (in seconds) that the owner has this lock for.
	// If lease_duration is None then the lock is non-expirable.
	TTL                         time.Duration
	HeartBeat                   time.Duration
	DeleteOnRelease             bool
	MaxRetryTableCreateAttempts uint16
	// The number of read capacity units which can be consumed per second (https://aws.amazon.com/dynamodb/pricing/provisioned/)
	RCU int64
	// The number of write capacity units which can be consumed per second (https://aws.amazon.com/dynamodb/pricing/provisioned/)
	WCU int64
}

// setOptionsDefaults sets the default options.
func (opts *Options) setOptionsDefaults() {
	if opts.TTL == 0 {
		opts.TTL = defaultTTL
	}
	if opts.MaxRetryTableCreateAttempts == 0 {
		opts.MaxRetryTableCreateAttempts = defaultMaxRetryTableCreateAttempts
	}
	if opts.RCU == 0 {
		opts.RCU = defaultRCU
	}
	if opts.WCU == 0 {
		opts.WCU = defaultWCU
	}
}

// New creates a new DynamoLock instance.
func New(client dynamodbutils.Client, tableName string, key string, opts Options) (*DynamoLock, error) {
	opts.setOptionsDefaults()

	lc, err := dynamolock.New(client,
		tableName,
		dynamolock.WithLeaseDuration(opts.TTL),
		dynamolock.WithHeartbeatPeriod(opts.HeartBeat),
	)
	if err != nil {
		return nil, err
	}

	createTableInput := dynamodb.CreateTableInput{
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(string(key)),
				KeyType:       types.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(opts.RCU),
			WriteCapacityUnits: aws.Int64(opts.WCU),
		},
		TableName: aws.String(tableName),
	}
	if err := dynamodbutils.CreateTableIfNotExists(client, tableName, createTableInput, opts.MaxRetryTableCreateAttempts); err != nil {
		return nil, errors.Join(dynamodbutils.ErrFailedToCreateTable, err)
	}

	l := new(DynamoLock)
	l.tableName = tableName
	l.key = key
	l.lockClient = lc
	l.opts = opts
	l.dynamoClient = client

	return l, nil
}

// NewLock creates a new DynamoLock instance using an existing DynamoLock instance.
func (l *DynamoLock) NewLock(key string) (lock.Locker, error) {
	lc, err := dynamolock.New(l.dynamoClient,
		l.tableName,
		dynamolock.WithLeaseDuration(l.opts.TTL),
		dynamolock.WithHeartbeatPeriod(l.opts.HeartBeat),
	)
	if err != nil {
		return nil, err
	}

	nl := new(DynamoLock)
	nl.tableName = l.tableName
	nl.key = key
	nl.lockClient = lc
	nl.dynamoClient = l.dynamoClient
	nl.opts = l.opts

	return nl, nil
}

// TryLock attempts to acquire a DynamoDB lock.
func (l *DynamoLock) TryLock() (bool, error) {
	lItem, err := l.lockClient.AcquireLock(l.key)
	l.lockedItem = lItem
	if err != nil {
		return false, errors.Join(lock.ErrLockNotObtained, err)
	}
	return true, nil
}

// Unlock releases a DynamoDB lock.
func (l *DynamoLock) Unlock() error {
	success, err := l.lockClient.ReleaseLock(l.lockedItem, dynamolock.WithDeleteLock(l.opts.DeleteOnRelease))
	if !success {
		return lock.ErrUnableToUnlock
	}
	if err != nil {
		return errors.Join(lock.ErrUnableToUnlock, err)
	}

	if err := l.lockClient.Close(); err != nil {
		return errors.Join(lock.ErrUnableToUnlock, err)
	}

	return nil
}
