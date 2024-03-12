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

// Package redisstate contains the resources required to create a Redis state store.
package redisstate

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/redis/go-redis/v9"
	"github.com/rivian/delta-go/state"
)

// Store stores a table's commit state in Redis.
type Store struct {
	Key         string
	RedisClient redis.UniversalClient
	ctx         context.Context
}

// Compile time check that FileStateStore implements state.StateStore
var _ state.Store = (*Store)(nil)

// New creates a new Store instance.
func New(client redis.UniversalClient, key string) *Store {
	s := new(Store)
	s.RedisClient = client
	s.Key = key
	s.ctx = context.TODO()
	return s
}

// Get retrieves a state store's commit state.
func (s *Store) Get() (state.CommitState, error) {
	var commitState state.CommitState

	data, err := s.RedisClient.Get(s.ctx, s.Key).Result()
	if err != nil {
		return commitState, errors.Join(state.ErrorCanNotReadState, err)
	}
	if len(data) == 0 {
		return commitState, errors.Join(state.ErrorStateIsEmpty, err)
	}

	err = json.Unmarshal([]byte(data), &commitState)
	if err != nil {
		return commitState, errors.Join(state.ErrorCanNotReadState, err)
	}

	return commitState, nil
}

// Put sets a state store's current commit state.
func (s *Store) Put(commitState state.CommitState) error {
	data, _ := json.Marshal(commitState)
	err := s.RedisClient.Set(s.ctx, s.Key, data, 0).Err()
	if err != nil {
		return errors.Join(state.ErrorCanNotWriteState, err)
	}
	return nil
}
