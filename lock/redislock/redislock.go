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
	"math"
	"math/rand"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	goredislib "github.com/redis/go-redis/v9"
	"github.com/rivian/delta-go/lock"
)

type RedisLock struct {
	Key          string
	redsyncMutex *redsync.Mutex
}

type Options struct {
	// The amount of time (in seconds) that the owner has this lock for.
	TTL time.Duration
}

// Compile time check that MutexWrapper implements lock.Locker
var _ lock.Locker = (*RedisLock)(nil)

const (
	TTL                    time.Duration = 60 * time.Second
	baseMilliSec           float64       = 100
	multiplier             float64       = 1.5
	minRandomNoiseMilliSec float64       = 50
	maxRandomNoiseMilliSec float64       = 250
)

func (opt *Options) setOptionsDefaults() {
	// Set the default TTL.
	if opt.TTL == 0 {
		opt.TTL = TTL
	}
}

func NewFromClient(client goredislib.UniversalClient, key string, opt *Options) *RedisLock {
	pool := goredis.NewPool(client)
	rs := redsync.New(pool)

	mutex := New(rs, key, &Options{TTL: opt.TTL})

	return mutex
}

func New(rs *redsync.Redsync, key string, opt *Options) *RedisLock {
	opt.setOptionsDefaults()

	// Obtain a new mutex by using the same name for all instances wanting the
	// same lock.
	mutex := new(RedisLock)
	mutex.redsyncMutex = rs.NewMutex(key, redsync.WithExpiry(opt.TTL),
		redsync.WithRetryDelayFunc(exponentialBackoff))
	mutex.Key = key

	return mutex
}

func (mutex *RedisLock) TryLock() (bool, error) {
	// Obtain a lock for our given mutex. After this is successful, no one else
	// can obtain the same lock (the same mutex name) until we unlock it.
	if err := mutex.redsyncMutex.Lock(); err != nil {
		return false, errors.Join(lock.ErrorLockNotObtained, err)
	}

	return true, nil
}

func (mutex *RedisLock) Unlock() error {
	// Release the lock so other processes or threads can obtain a lock.
	if ok, err := mutex.redsyncMutex.Unlock(); !ok || err != nil {
		return errors.Join(lock.ErrorUnableToUnlock, err)
	}

	return nil
}

func exponentialBackoff(tries int) time.Duration {
	// Computes base * (multiplier ^ tries) + random_number_milliseconds
	return time.Duration(baseMilliSec*math.Pow(multiplier, float64(tries))+
		rand.Float64()*(maxRandomNoiseMilliSec-minRandomNoiseMilliSec)+
		minRandomNoiseMilliSec) * time.Millisecond
}
