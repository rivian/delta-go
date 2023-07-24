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
	"github.com/rivian/delta-go"
	"github.com/rivian/delta-go/storage"
)

type LogStore interface {
	// Load the given file and return a list of actions.
	Read(path *storage.Path) error

	// Write the given `actions` to the given `path` with or without overwrite as indicated.
	// Implementation must throw FileAlreadyExistsException exception if the file already
	// exists and overwrite = false. Furthermore, if isPartialWriteVisible returns false,
	// implementation must ensure that the entire file is made visible atomically, that is,
	// it should not generate partial files.
	Write(path *storage.Path, actions []delta.Action, overwrite bool) error

	// List the paths in the same directory that are lexicographically greater or equal to
	// (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
	ListFrom(path *storage.Path) error
}
