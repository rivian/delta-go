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
package dynamolock

import (
	"errors"
	"time"

	"cirello.io/dynamolock/v2"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/rivian/delta-go/internal/dynamodbutils"
	"github.com/rivian/delta-go/lock"
)

// Represents attribute names in DynamoDB items
type Attribute string

const (
	Key                                Attribute     = "key"
	DefaultTTL                         time.Duration = 60 * time.Second
	DefaultHeartbeat                   time.Duration = 1 * time.Second
	DefaultMaxRetryTableCreateAttempts uint16        = 20
	DefaultRCU                         int64         = 5
	DefaultWCU                         int64         = 5
)

type DynamoLock struct {
	tableName    string
	lockClient   *dynamolock.Client
	lockedItem   *dynamolock.Lock
	key          string
	dynamoClient dynamodbutils.DynamoDBClient
	opts         Options
}

// Compile time check that FileLock implements lock.Locker
var _ lock.Locker = (*DynamoLock)(nil)

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

// Sets the default options
func (opts *Options) setOptionsDefaults() {
	if opts.TTL == 0 {
		opts.TTL = DefaultTTL
	}
	if opts.MaxRetryTableCreateAttempts == 0 {
		opts.MaxRetryTableCreateAttempts = DefaultMaxRetryTableCreateAttempts
	}
	if opts.RCU == 0 {
		opts.RCU = DefaultRCU
	}
	if opts.WCU == 0 {
		opts.WCU = DefaultWCU
	}
}

// Creates a new DynamoLock instance
func New(client dynamodbutils.DynamoDBClient, tableName string, key string, opts Options) (*DynamoLock, error) {
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
				AttributeName: aws.String(string(Key)),
				KeyType:       types.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(opts.RCU),
			WriteCapacityUnits: aws.Int64(opts.WCU),
		},
		TableName: aws.String(tableName),
	}
	dynamodbutils.TryEnsureDynamoDBTableExists(client, tableName, createTableInput, opts.MaxRetryTableCreateAttempts)

	l := new(DynamoLock)
	l.tableName = tableName
	l.key = key
	l.lockClient = lc
	l.opts = opts
	l.dynamoClient = client
	return l, nil
}

// Creates a new DynamoLock instance using an existing DynamoLock instance
func (l *DynamoLock) NewLock(key string) (lock.Locker, error) {
	nl := new(DynamoLock)
	nl.tableName = l.tableName
	nl.lockClient = l.lockClient
	nl.key = key
	nl.dynamoClient = l.dynamoClient
	nl.opts = l.opts

	return nl, nil
}

// Attempts to acquire a DynamoDB lock
func (l *DynamoLock) TryLock() (bool, error) {
	lItem, err := l.lockClient.AcquireLock(l.key)
	l.lockedItem = lItem
	if err != nil {
		return false, errors.Join(lock.ErrorLockNotObtained, err)
	}
	return true, nil
}

// Releases a DynamoDB lock
func (l *DynamoLock) Unlock() error {
	success, err := l.lockClient.ReleaseLock(l.lockedItem, dynamolock.WithDeleteLock(l.opts.DeleteOnRelease))
	if !success {
		return lock.ErrorUnableToUnlock
	}
	l.lockClient.Close()
	if err != nil {
		return errors.Join(lock.ErrorUnableToUnlock, err)
	}
	return nil
}
