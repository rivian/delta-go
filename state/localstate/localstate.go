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

// Package localstate contains the resources required to create a localstate.
package localstate

import (
	"github.com/rivian/delta-go/state"
)

// Store stores the version locally.
// WARNING
// There is no concurrency support for multiple goroutines or processes.
// There is no persistence.
// This is intended for local use and testing only.
type Store struct {
	version int64
}

// New creates a new Store with the current table version.
func New(currentTableVersion int64) *Store {
	ls := new(Store)
	ls.version = currentTableVersion
	return ls
}

// Compile time check that Store implements state.StateStore
var _ state.Store = (*Store)(nil)

// Get retrieves a state store's commit state.
func (stateStore *Store) Get() (state.CommitState, error) {
	return state.CommitState{Version: stateStore.version}, nil
}

// Put sets a state store's current commit state.
func (stateStore *Store) Put(commitState state.CommitState) error {
	stateStore.version = commitState.Version
	return nil
}
