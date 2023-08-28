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
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	goredislib "github.com/redis/go-redis/v9"
	"github.com/stvp/tempredis"
)

var servers []*tempredis.Server

const (
	ServerPools    = 1
	ServerPoolSize = 8
)

func TestMain(m *testing.M) {
	for i := 0; i < ServerPoolSize*ServerPools; i++ {
		server, err := tempredis.Start(tempredis.Config{
			"port": strconv.Itoa(51200 + i),
		})

		if err != nil {
			panic(err)
		}

		servers = append(servers, server)
	}

	result := m.Run()

	for _, server := range servers {
		_ = server.Term()
	}

	os.Exit(result)
}

// To find more extensive test cases, visit
// https://github.com/go-redsync/redsync/blob/master/mutex_test.go.
func TestRedsyncMultiplePools(t *testing.T) {
	pools := newMockPoolsGoredis(8)

	// Create an instance of redisync to be used to obtain a mutual exclusion
	// lock.
	rs := redsync.New(pools...)
	mutex := New(rs, "multiple-pools-mutex", &LockOptions{})

	locked, err := mutex.TryLock()
	if !locked {
		t.Error("TryLock failed")
	}
	if err != nil {
		t.Error(err)
	}

	// Perform an operation.
	fmt.Println(mutex.Key + ": I have a lock!")

	err = mutex.Unlock()
	if err != nil {
		t.Error(err)
	}
}

func TestRedsyncSimpleClient(t *testing.T) {
	client := goredislib.NewUniversalClient(&goredislib.UniversalOptions{
		Addrs: []string{servers[0].Socket()},
	})

	mutex := NewFromClient(client, "simple-client-mutex", &LockOptions{TTL: 100 * time.Second})

	locked, err := mutex.TryLock()
	if !locked {
		t.Error("TryLock failed")
	}
	if err != nil {
		t.Error(err)
	}

	fmt.Println(mutex.Key + ": I have a lock!")

	err = mutex.Unlock()
	if err != nil {
		t.Error(err)
	}
}

func TestNewLock(t *testing.T) {
	client := goredislib.NewUniversalClient(&goredislib.UniversalOptions{
		Addrs: []string{servers[0].Socket()},
	})

	mutex, _ := (&RedisLock{}).NewLock("simple-client-mutex", LockOptions{TTL: 100 * time.Second}, LockMetadata{Client: client})

	locked, err := mutex.(*RedisLock).TryLock()
	if !locked {
		t.Error("TryLock failed")
	}
	if err != nil {
		t.Error(err)
	}

	fmt.Println(mutex.(*RedisLock).Key + ": I have a lock!")

	err = mutex.(*RedisLock).Unlock()
	if err != nil {
		t.Error(err)
	}
}

func newMockPoolsGoredis(n int) []redis.Pool {
	pools := make([]redis.Pool, n)

	for i := 0; i < n; i++ {
		client := goredislib.NewClient(&goredislib.Options{
			Network: "unix",
			Addr:    servers[i].Socket(),
		})

		pools[i] = goredis.NewPool(client)
	}

	return pools
}
