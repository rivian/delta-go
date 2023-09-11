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
	log "github.com/sirupsen/logrus"
)

type DynamoLock struct {
	tableName    string
	lockClient   *dynamolock.Client
	lockedItem   *dynamolock.Lock
	key          string
	dynamoClient dynamodbutils.DynamoDBClient
	opts         Options
}

var (
	// Compile time check that FileLock implements lock.Locker
	_                                     lock.Locker = (*DynamoLock)(nil)
	ErrorExceededTableCreateRetryAttempts error       = errors.New("failed to create table")
)

type Options struct {
	// The amount of time (in seconds) that the owner has this lock for.
	// If lease_duration is None then the lock is non-expirable.
	TTL                         time.Duration
	HeartBeat                   time.Duration
	DeleteOnRelease             bool
	MaxRetryTableCreateAttempts uint16
	RCU                         int64
	WCU                         int64
}

const (
	DefaultTTL                         time.Duration = 60 * time.Second
	DefaultHeartbeat                   time.Duration = 1 * time.Second
	DefaultMaxRetryTableCreateAttempts uint16        = 20
)

// Sets the default options
func (opts *Options) setOptionsDefaults() {
	if opts.TTL == 0 {
		opts.TTL = DefaultTTL
	}
	if opts.MaxRetryTableCreateAttempts == 0 {
		opts.MaxRetryTableCreateAttempts = DefaultMaxRetryTableCreateAttempts
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
				AttributeName: aws.String("key"),
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

func (l *DynamoLock) tryEnsureTableExists() error {
	var attemptNumber int = 0
	var created bool = false

	for {
		if attemptNumber >= int(l.Options.maxRetryTableCreateAttempts) {
			log.Debugf("delta-go: Table create attempt failed. Attempts exhausted beyond maxRetryDynamoDbTableCreateAttempts of %d so failing.", l.Options.maxRetryTableCreateAttempts)
			return ErrorExceededTableCreateRetryAttempts
		}

		var status string = "CREATING"

		result, err := l.DynamoClient.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(l.TableName),
		})
		if err != nil {
			var rcu int64 = 5
			var wcu int64 = 5

			log.Infof("delta-go: DynamoDB table %s does not exist. Creating it now with provisioned throughput of %d and %d WCUs.", l.TableName, rcu, wcu)
			_, err := l.LockClient.CreateTable(l.TableName,
				dynamolock.WithProvisionedThroughput(&dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(5),
					WriteCapacityUnits: aws.Int64(5),
				}),
				dynamolock.WithCustomPartitionKeyName("key"),
			)
			if err != nil {
				log.Debugf("delta-go: Table %s just created by concurrent process. %v", l.TableName, err)
			}
		}

		if result == nil || result.Table == nil {
			attemptNumber++
			log.Infof("delta-go: Waiting for %s table creation", l.TableName)
			time.Sleep(1 * time.Second)
			continue
		} else {
			status = string(*result.Table.TableStatus)
		}

		if status == "ACTIVE" {
			if created {
				log.Infof("delta-go: Successfully created DynamoDB table %s", l.TableName)
			} else {
				log.Infof("delta-go: Table %s already exists", l.TableName)
			}
		} else if status == "CREATING" {
			attemptNumber++
			log.Infof("delta-go: Waiting for %s table creation", l.TableName)
			time.Sleep(1000 * time.Millisecond)
		} else {
			attemptNumber++
			log.Debugf("delta-go: Table %s status: %s. Incrementing attempt number to %d and retrying. %v", l.TableName, status, attemptNumber, err)
			continue
		}

		return nil
	}
}
