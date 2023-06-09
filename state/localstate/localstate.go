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
package localstate

import (
	"github.com/rivian/delta-go/state"
)

// / LocalStateStore stores the version locally.
// / WARNING
// / There is no concurrency support for multiple goroutines or processes.
// / There is no persistence.
// / This is intended for local use and testing only.
type LocalStateStore struct {
	version state.DeltaDataTypeVersion
}

// / Create a new LocalStateStore with the current table version
func New(currentTableVersion state.DeltaDataTypeVersion) *LocalStateStore {
	ls := new(LocalStateStore)
	ls.version = currentTableVersion
	return ls
}

// Compile time check that LocalStateStore implements state.StateStore
var _ state.StateStore = (*LocalStateStore)(nil)

func (stateStore *LocalStateStore) Get() (state.CommitState, error) {
	return state.CommitState{Version: stateStore.version}, nil
}

func (stateStore *LocalStateStore) Put(commitState state.CommitState) error {
	stateStore.version = commitState.Version
	return nil
}
