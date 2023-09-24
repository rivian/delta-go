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
package logstore

import (
	"github.com/rivian/delta-go/internal/dynamodbutils"
	"github.com/rivian/delta-go/storage"
)

type LogStore interface {
	// Puts an entry into a log store in an exclusive way
	Put(entry *CommitEntry, overwrite bool) error

	// Gets an entry corresponding to the Delta log file with given `tablePath` and `fileName`
	Get(tablePath storage.Path, fileName storage.Path) (*CommitEntry, error)

	// Gets the latest entry corresponding to the Delta log file for given `tablePath`
	Latest(tablePath storage.Path) (*CommitEntry, error)

	// Gets the log store client
	Client() dynamodbutils.Client

	// Gets the number of seconds until a log store entry expires
	ExpirationDelaySeconds() uint64
}
