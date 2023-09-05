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
	"context"
	"errors"
	"time"

	"cirello.io/dynamolock/v2"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/rivian/delta-go/lock"
)

type DynamoDBClient interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
}

type DynamoLock struct {
	tableName    string
	lockClient   *dynamolock.Client
	lockedItem   *dynamolock.Lock
	key          string
	dynamoClient DynamoDBClient
	options      Options
}

// Compile time check that FileLock implements lock.Locker
var _ lock.Locker = (*DynamoLock)(nil)

type Options struct {
	// The amount of time (in seconds) that the owner has this lock for.
	// If lease_duration is None then the lock is non-expirable.
	TTL       time.Duration
	HeartBeat time.Duration
}

const (
	TTL       time.Duration = 60 * time.Second
	Heartbeat time.Duration = 1 * time.Second
)

// Sets the default options
func (options *Options) setOptionsDefaults() {
	if options.TTL == 0 {
		options.TTL = TTL
	}
}

// Creates a new DynamoDB lock object
func New(client DynamoDBClient, tableName string, key string, options Options) (*DynamoLock, error) {
	options.setOptionsDefaults()

	lc, err := dynamolock.New(client,
		tableName,
		dynamolock.WithLeaseDuration(options.TTL),
		dynamolock.WithHeartbeatPeriod(options.HeartBeat),
	)
	if err != nil {
		return nil, err
	}

	l := new(DynamoLock)
	l.tableName = tableName
	l.key = key
	l.lockClient = lc
	l.options = options
	l.dynamoClient = client
	return l, nil
}

// Creates a new DynamoDB lock object using an existing DynamoDB lock object
func (l *DynamoLock) NewLock(key string) (lock.Locker, error) {
	nl := new(DynamoLock)
	nl.tableName = l.tableName
	nl.lockClient = l.lockClient
	nl.key = key
	nl.dynamoClient = l.dynamoClient
	nl.options = l.options

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
	success, err := l.lockClient.ReleaseLock(l.lockedItem)
	if !success {
		return lock.ErrorUnableToUnlock
	}
	l.lockClient.Close()
	if err != nil {
		return errors.Join(lock.ErrorUnableToUnlock, err)
	}
	return nil
}
