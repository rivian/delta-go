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

// Package logstore contains the resources required to create a log store.
package logstore

import (
	"errors"

	"github.com/rivian/delta-go/storage"
)

var (
	// ErrLatestDoesNotExist is returned when the latest item in a log store does not exist.
	ErrLatestDoesNotExist error = errors.New("the latest item does not exist")
)

// LogStore uses optimistic concurrency control to commit transactions.
type LogStore interface {
	// Put puts a commit entry into a log store in an exclusive way.
	Put(entry *CommitEntry, overwrite bool) error

	// Get gets a commit entry corresponding to the commit log identified by the given table path and file name.
	Get(tablePath storage.Path, fileName storage.Path) (*CommitEntry, error)

	// Latest gets the commit entry corresponding to the latest commit log for a given table path.
	Latest(tablePath storage.Path) (*CommitEntry, error)

	// Client gets a log store client.
	Client() any

	// ExpirationDelaySeconds gets the number of seconds until a commit entry expires.
	ExpirationDelaySeconds() uint64
}
