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
	"fmt"
	"time"

	"cirello.io/dynamolock"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rivian/delta-go/lock"
)

type DynamoLock struct {
	TableName    string
	LockClient   *dynamolock.Client
	LockedItem   *dynamolock.Lock
	Key          string
	DynamoClient dynamodbiface.DynamoDBAPI
	Options      LockOptions
}

// Compile time check that FileLock implements lock.Locker
var _ lock.Locker = (*DynamoLock)(nil)

type LockOptions struct {
	// The amount of time (in seconds) that the owner has this lock for.
	// If lease_duration is None then the lock is non-expirable.
	TTL       time.Duration
	HeartBeat time.Duration
}

const (
	TTL       time.Duration = 60 * time.Second
	HEARTBEAT time.Duration = 1 * time.Second
)

func New(client dynamodbiface.DynamoDBAPI, tableName string, key string, opt LockOptions) (*DynamoLock, error) {

	if opt.TTL == 0 {
		opt.TTL = TTL
	}
	if opt.HeartBeat == 0 {
		opt.HeartBeat = HEARTBEAT
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
		return fmt.Errorf("%w", lock.ErrorUnableToUnlock)
	}
	l.LockClient.Close()
	if err != nil {
		return errors.Join(lock.ErrorUnableToUnlock, err)
	}
	return nil
}
