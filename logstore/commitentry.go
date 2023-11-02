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

// CommitEntry represents an entry in a log store for a given commit in the Delta log.
type CommitEntry struct {
	// Absolute path of a Delta table
	tablePath storage.Path
	// File name for a commit, e.g. "000000N.json"
	fileName storage.Path
	// Path to the temp file for a commit, relative to the Delta log
	tempPath storage.Path
	// True if the temp file has been successfully copied to its destination location, otherwise false
	isComplete bool
	// Epoch seconds at which a completed commit entry is safe to be deleted
	expirationTime uint64
}

// New creates a new CommitEntry instance.
func New(tablePath storage.Path, fileName storage.Path, tempPath storage.Path, isComplete bool, expirationTime uint64) *CommitEntry {
	ce := new(CommitEntry)
	ce.tablePath = tablePath
	ce.fileName = fileName
	ce.tempPath = tempPath
	ce.isComplete = isComplete
	ce.expirationTime = expirationTime

	return ce
}

// TablePath gets the table path for a commit entry.
func (ce *CommitEntry) TablePath() storage.Path {
	return ce.tablePath
}

// FileName gets the file name for a commit entry.
func (ce *CommitEntry) FileName() storage.Path {
	return ce.fileName
}

// TempPath gets the temp path for a commit entry.
func (ce *CommitEntry) TempPath() storage.Path {
	return ce.tempPath
}

// IsComplete gets the completion status of a commit entry.
func (ce *CommitEntry) IsComplete() bool {
	return ce.isComplete
}

// ExpireTime gets the expiration time of a commit entry.
func (ce *CommitEntry) ExpirationTime() uint64 {
	return ce.expirationTime
}

// Complete completes a commit entry.
func (ce *CommitEntry) Complete(expirationDelaySeconds uint64) *CommitEntry {
	return New(ce.tablePath, ce.fileName, ce.tempPath, true, uint64(time.Now().Unix())+expirationDelaySeconds)
}

// AbsoluteFilePath gets the absolute file path for a commit entry.
func (ce *CommitEntry) AbsoluteFilePath() (storage.Path, error) {
	return storage.PathFromIter([]string{ce.tablePath.Raw, "_delta_log", ce.fileName.Raw}), nil
}

// AbsoluteTempPath gets the absolute temp path for a commit entry.
func (ce *CommitEntry) AbsoluteTempPath() (storage.Path, error) {
	return storage.PathFromIter([]string{ce.tablePath.Raw, "_delta_log", ce.tempPath.Raw}), nil
}
