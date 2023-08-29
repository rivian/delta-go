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
	Key             string
	redsyncInstance *redsync.Redsync
	redsyncMutex    *redsync.Mutex
	Options         Options
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
	TTL                    time.Duration = 60 * time.Second
	maxTries               int           = 20
	baseMilliSec           float64       = 100
	multiplier             float64       = 1.5
	minRandomNoiseMilliSec float64       = 50
	maxRandomNoiseMilliSec float64       = 250
)

func (options *Options) setOptionsDefaults() {
	// Set default options
	if options.TTL == 0 {
		options.TTL = TTL
	}
	if options.MaxTries == 0 {
		options.MaxTries = maxTries
	}
}

func (rl *RedisLock) NewLock(key string) (lock.Locker, error) {
	newRl := new(RedisLock)
	newRl.redsyncMutex = rl.redsyncInstance.NewMutex(key, redsync.WithExpiry(rl.Options.TTL), redsync.WithTries(rl.Options.MaxTries))
	newRl.Key = key

	return newRl, nil
}

func NewFromClient(client goredislib.UniversalClient, key string, options Options) *RedisLock {
	pool := goredis.NewPool(client)
	rs := redsync.New(pool)

	rl := New(rs, key, Options{TTL: options.TTL, MaxTries: options.MaxTries})

	return rl
}

func New(rs *redsync.Redsync, key string, options Options) *RedisLock {
	options.setOptionsDefaults()

	// Obtain a new mutex by using the same name for all instances wanting the
	// same lock.
	rl := new(RedisLock)
	rl.Key = key
	rl.redsyncInstance = rs
	rl.redsyncMutex = rs.NewMutex(key, redsync.WithExpiry(options.TTL), redsync.WithTries(options.MaxTries))
	rl.Options = options

	return rl
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
