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
package filestate

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/rivian/delta-go/state"
	"github.com/rivian/delta-go/storage"
)

type FileStateStore struct {
	BaseURI storage.Path
	Key     string
}

// Compile time check that FileStateStore implements state.StateStore
var _ state.StateStore = (*FileStateStore)(nil)

func New(baseURI storage.Path, key string) *FileStateStore {
	fs := new(FileStateStore)
	fs.BaseURI = baseURI
	fs.Key = key
	return fs
}

func (s FileStateStore) Get() (state.CommitState, error) {
	getPath := filepath.Join(s.BaseURI.Raw, s.Key)
	var commitState state.CommitState
	data, err := os.ReadFile(getPath)
	if err != nil {
		return commitState, errors.Join(state.ErrorCanNotReadState, err)
	}
	if len(data) == 0 {
		return commitState, errors.Join(state.ErrorStateIsEmpty, err)
	}

	err = json.Unmarshal(data, &commitState)
	if err != nil {
		return commitState, errors.Join(state.ErrorCanNotReadState, err)
	}

	return commitState, nil
}

func (s FileStateStore) Put(commitState state.CommitState) error {
	putPath := filepath.Join(s.BaseURI.Raw, s.Key)
	err := os.MkdirAll(filepath.Dir(putPath), 0755)
	if err != nil {
		return errors.Join(state.ErrorCanNotWriteState, err)
	}
	data, _ := json.Marshal(commitState)
	err = os.WriteFile(putPath, data, 0755)
	if err != nil {
		return errors.Join(state.ErrorCanNotWriteState, err)
	}
	return nil
}
