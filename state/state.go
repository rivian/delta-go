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

// Package state contains the resources required to create a state store.
package state

import (
	"errors"
)

var (
	// ErrorStateIsEmpty is returned when a state is empty.
	ErrorStateIsEmpty error = errors.New("the state is empty")
	// ErrorCanNotReadState is returned when a state cannot be read.
	ErrorCanNotReadState error = errors.New("the state is could not be read")
	// ErrorCanNotWriteState is returned when a state cannot be written.
	ErrorCanNotWriteState error = errors.New("the state is could not be written")
)

// CommitState stores an attempt to  `source` into `destination` and `version` for the latest commit.
type CommitState struct {
	// Version of the commit
	Version int64 `json:"version"`
}

// Store provides remote state storage for fast lookup on the current commit version.
type Store interface {
	// GetData() retrieves the data cached in the lock.
	// for a DeltaTable, the data will contain the current or prior locked commit version.
	Get() (CommitState, error)

	// GetData() retrieves the data cached in the lock.
	// for a DeltaTable, the data will contain the current or prior locked commit version.
	Put(CommitState) error
}
