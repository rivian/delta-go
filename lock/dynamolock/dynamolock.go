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

	"cirello.io/dynamolock"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rivian/delta-go/lock"
	log "github.com/sirupsen/logrus"
)

type DynamoLock struct {
	TableName    string
	LockClient   *dynamolock.Client
	LockedItem   *dynamolock.Lock
	Key          string
	DynamoClient dynamodbiface.DynamoDBAPI
	Options      LockOptions
}

var (
	// Compile time check that FileLock implements lock.Locker
	_                                     lock.Locker = (*DynamoLock)(nil)
	ErrorExceededTableCreateRetryAttempts error       = errors.New("failed to create table")
)

type LockOptions struct {
	// The amount of time (in seconds) that the owner has this lock for.
	// If lease_duration is None then the lock is non-expirable.
	TTL                         time.Duration
	HeartBeat                   time.Duration
	maxRetryTableCreateAttempts uint16
}

const (
	TTL                         time.Duration = 60 * time.Second
	HEARTBEAT                   time.Duration = 1 * time.Second
	MaxRetryTableCreateAttempts               = 20
)

func New(client dynamodbiface.DynamoDBAPI, tableName string, key string, opt LockOptions) (*DynamoLock, error) {
	if opt.TTL == 0 {
		opt.TTL = TTL
	}
	if opt.HeartBeat == 0 {
		opt.HeartBeat = HEARTBEAT
	}
	if opt.maxRetryTableCreateAttempts == 0 {
		opt.maxRetryTableCreateAttempts = MaxRetryTableCreateAttempts
	}

	lc, err := dynamolock.New(client,
		tableName,
		dynamolock.WithLeaseDuration(opt.TTL),
		dynamolock.WithHeartbeatPeriod(opt.HeartBeat),
	)
	if err != nil {
		return nil, err
	}

	dl := new(DynamoLock)
	dl.TableName = tableName
	dl.Key = key
	dl.LockClient = lc
	dl.Options = opt
	dl.DynamoClient = client

	err = dl.tryEnsureTableExists()
	if err != nil {
		return nil, err
	}

	return dl, nil
}

func (l *DynamoLock) TryLock() (bool, error) {
	lItem, err := l.LockClient.AcquireLock(l.Key)
	l.LockedItem = lItem
	if err != nil {
		return false, errors.Join(lock.ErrorLockNotObtained, err)
	}
	return true, nil
}

func (l *DynamoLock) Unlock() error {
	success, err := l.LockClient.ReleaseLock(l.LockedItem)
	if !success {
		return lock.ErrorUnableToUnlock
	}
	l.LockClient.Close()
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

		status = *result.Table.TableStatus
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
