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
package redisstate

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/rivian/delta-go/state"
	"github.com/stvp/tempredis"
)

var servers []*tempredis.Server

// Add more servers to accommodate more tests.
var numServers int = 2

func TestMain(m *testing.M) {
	for i := 0; i < numServers; i++ {
		server, err := tempredis.Start(tempredis.Config{})
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

func TestGetPutData(t *testing.T) {
	var redisOpts = &redis.Options{
		Network: "unix",
		Addr:    servers[0].Socket(),
	}

	client := redis.NewClient(redisOpts)
	defer teardown(t, client)

	rs := New(client, "_delta_log/commit.state")

	data := state.CommitState{Version: 0}
	err := rs.Put(data)
	if err != nil {
		t.Errorf("err = %e;", err)
	}

	out, err := rs.Get()
	if err != nil {
		t.Error(err)
	}
	if out.Version != 0 {
		t.Error("Version is not 0")
	}

	err = rs.Put(state.CommitState{Version: 1})
	if err != nil {
		t.Errorf("err = %e;", err)
	}

	out, err = rs.Get()
	if err != nil {
		t.Error(err)
	}
	if out.Version != 1 {
		t.Error("Version is not 1")
	}

	rs.Key = ""
	//if lock is not held, should return ErrorLockNotObtained
	out, err = rs.Get()
	if !errors.Is(err, state.ErrorCanNotReadState) {
		t.Errorf("err = %e;", err)
	}
	if out.Version != 0 {
		t.Error("Version is not 0")
	}

}

func teardown(t *testing.T, rc *redis.Client) {
	t.Helper()

	if err := rc.Del(context.Background(), "_delta_log/commit.lock").Err(); err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
}
