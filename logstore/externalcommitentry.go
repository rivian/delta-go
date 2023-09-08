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
	"time"

	"github.com/rivian/delta-go/storage"
)

// Wrapper struct representing an entry in an external store for a given commit into the Delta log
// Contains relevant fields and helper methods have been defined
type ExternalCommitEntry struct {
	// Absolute path for Delta table
	TablePath storage.Path
	// File name of this commit, e.g. "000000N.json"
	FileName storage.Path
	// Path to temp file for this commit, relative to the `_delta_log`
	TempPath storage.Path
	// true if Delta JSON file is successfully copied to its destination location, else false
	Complete bool
	// Epoch seconds at which this external commit entry is safe to be deleted if complete = true, else 0
	ExpireTime uint64
}

func NewExternalCommitEntry(tablePath storage.Path, fileName storage.Path, tempPath storage.Path, complete bool, expireTime uint64) (*ExternalCommitEntry, error) {
	ece := new(ExternalCommitEntry)
	ece.TablePath = tablePath
	ece.FileName = fileName
	ece.TempPath = tempPath
	ece.Complete = complete
	ece.ExpireTime = expireTime
	return ece, nil
}

// Returns this entry with `complete=true` and a valid `expireTime`
func (ece *ExternalCommitEntry) AsComplete(expirationDelaySeconds uint64) (*ExternalCommitEntry, error) {
	return NewExternalCommitEntry(ece.TablePath, ece.FileName, ece.TempPath, true, uint64(time.Now().Unix())+expirationDelaySeconds)
}

// Returns the absolute path to the file for this entry
func (ece *ExternalCommitEntry) AbsoluteFilePath() (storage.Path, error) {
	return storage.PathFromIter([]string{ece.TablePath.Raw, "_delta_log", ece.FileName.Raw}), nil
}

// Returns the absolute path to the temp file for this entry
func (ece *ExternalCommitEntry) AbsoluteTempPath() (storage.Path, error) {
	return storage.PathFromIter([]string{ece.TablePath.Raw, "_delta_log", ece.TempPath.Raw}), nil
}
