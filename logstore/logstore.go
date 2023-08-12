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

type LogStore interface {
	// Writes to external store in exclusive way.
	PutExternalEntry(entry *ExternalCommitEntry, overwrite bool) error

	// Returns external store entry corresponding to delta log file with given `tablePath` and `fileName`.
	GetExternalEntry(tablePath string, fileName string) (*ExternalCommitEntry, error)

	// Returns the latest external store entry corresponding to the delta log for given `tablePath`.
	GetLatestExternalEntry(tablePath string) (*ExternalCommitEntry, error)

	// Returns the number of expiration delay seconds.
	GetExpirationDelaySeconds() (uint64, error)
}
