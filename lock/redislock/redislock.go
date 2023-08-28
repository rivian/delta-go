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

type LockOptions struct {
	// The amount of time (in seconds) that the owner has this lock for.
	TTL time.Duration
	// Maximum number of tries when trying to acquire a lock
	MaxTries int
}

type LockMetadata struct {
	Client goredislib.UniversalClient
}

// Compile time check that MutexWrapper implements lock.Locker
var _ lock.Locker = (*RedisLock)(nil)

const (
	TTL                    time.Duration = 60 * time.Second
	maxTries               int           = 20
	baseMilliSec           float64       = 100
	multiplier             float64       = 1.5
	minRandomNoiseMilliSec float64       = 50
	maxRandomNoiseMilliSec float64       = 250
)

func (opt *LockOptions) setOptionsDefaults() {
	// Set default options
	if opt.TTL == 0 {
		opt.TTL = TTL
	}
	if opt.MaxTries == 0 {
		opt.MaxTries = maxTries
	}
}

func (*RedisLock) NewLock(key string, opt interface{}, metadata interface{}) (interface{}, error) {
	pool := goredis.NewPool(metadata.(LockMetadata).Client)
	rs := redsync.New(pool)

	mutex := New(rs, key, &LockOptions{TTL: opt.(LockOptions).TTL, MaxTries: opt.(LockOptions).MaxTries})

	return mutex, nil
}

func NewFromClient(client goredislib.UniversalClient, key string, opt *LockOptions) *RedisLock {
	pool := goredis.NewPool(client)
	rs := redsync.New(pool)

	mutex := New(rs, key, &LockOptions{TTL: opt.TTL, MaxTries: opt.MaxTries})

	return mutex
}

func New(rs *redsync.Redsync, key string, opt *LockOptions) *RedisLock {
	opt.setOptionsDefaults()

	// Obtain a new mutex by using the same name for all instances wanting the
	// same lock.
	mutex := new(RedisLock)
	mutex.redsyncMutex = rs.NewMutex(key, redsync.WithExpiry(opt.TTL), redsync.WithTries(opt.MaxTries))
	mutex.Key = key

	return mutex
}

func (rl *RedisLock) TryLock() (bool, error) {
	// Obtain a lock for our given mutex. After this is successful, no one else
	// can obtain the same lock (the same mutex name) until we unlock it.
	if err := rl.redsyncMutex.Lock(); err != nil {
		return false, errors.Join(lock.ErrorLockNotObtained, err)
	}

	return true, nil
}

func (rl *RedisLock) Unlock() error {
	// Release the lock so other processes or threads can obtain a lock.
	if ok, err := rl.redsyncMutex.Unlock(); !ok || err != nil {
		return errors.Join(lock.ErrorUnableToUnlock, err)
	}

	return nil
}

// Currently not used
func exponentialBackoff(tries int) time.Duration {
	// Computes base * (multiplier ^ tries) + random_number_milliseconds
	return time.Duration(baseMilliSec*math.Pow(multiplier, float64(tries))+
		rand.Float64()*(maxRandomNoiseMilliSec-minRandomNoiseMilliSec)+
		minRandomNoiseMilliSec) * time.Millisecond
}
