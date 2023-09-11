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
package redislock

import (
	"errors"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	goredislib "github.com/redis/go-redis/v9"
	"github.com/rivian/delta-go/lock"
)

type RedisLock struct {
	key             string
	redsyncInstance *redsync.Redsync
	redsyncMutex    *redsync.Mutex
	opts            Options
}

type Options struct {
	// The amount of time (in seconds) that the owner has this lock for.
	TTL time.Duration
	// Maximum number of tries when trying to acquire a lock
	MaxTries int
}

// Compile time check that MutexWrapper implements lock.Locker
var _ lock.Locker = (*RedisLock)(nil)

const (
	DefaultTTL      time.Duration = 60 * time.Second
	DefaultMaxTries int           = 20
)

// Sets the default options
func (opts *Options) setOptionsDefaults() {
	if opts.TTL == 0 {
		opts.TTL = DefaultTTL
	}
	if opts.MaxTries == 0 {
		opts.MaxTries = DefaultMaxTries
	}
}

// Creates a new RedisLock instance using a Redis client
func NewFromClient(client goredislib.UniversalClient, key string, opts Options) *RedisLock {
	pool := goredis.NewPool(client)
	rs := redsync.New(pool)

	l := New(rs, key, Options{TTL: opts.TTL, MaxTries: opts.MaxTries})

	return l
}

// Creates a new RedisLock instance using a Redsync instance
func New(rs *redsync.Redsync, key string, opts Options) *RedisLock {
	opts.setOptionsDefaults()

	// Obtain a new mutex by using the same name for all instances wanting the
	// same lock.
	l := new(RedisLock)
	l.key = key
	l.redsyncInstance = rs
	l.redsyncMutex = rs.NewMutex(key, redsync.WithExpiry(opts.TTL), redsync.WithTries(opts.MaxTries))
	l.opts = opts

	return l
}

// Creates a new RedisLock instance using an existing RedisLock instance
func (l *RedisLock) NewLock(key string) (lock.Locker, error) {
	nl := new(RedisLock)
	nl.key = key
	nl.redsyncInstance = l.redsyncInstance
	nl.redsyncMutex = l.redsyncInstance.NewMutex(key, redsync.WithExpiry(l.opts.TTL), redsync.WithTries(l.opts.MaxTries))
	nl.opts = l.opts

	return nl, nil
}

// Attempts to acquire a Redis lock
func (l *RedisLock) TryLock() (bool, error) {
	// Obtain a lock for our given mutex. After this is successful, no one else
	// can obtain the same lock (the same mutex name) until we unlock it.
	if err := l.redsyncMutex.Lock(); err != nil {
		return false, errors.Join(lock.ErrorLockNotObtained, err)
	}

	return true, nil
}

// Releases a Redis lock
func (l *RedisLock) Unlock() error {
	// Release the lock so other processes or threads can obtain a lock.
	if ok, err := l.redsyncMutex.Unlock(); !ok || err != nil {
		return errors.Join(lock.ErrorUnableToUnlock, err)
	}

	return nil
}
