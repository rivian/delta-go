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
	"errors"
	"testing"

	"github.com/rivian/delta-go/state"
	"github.com/rivian/delta-go/storage"
)

func TestGetPutData(t *testing.T) {

	tmpDir := t.TempDir()
	// fileLockKey := filepath.Join(tmpDir, "_commit.state")
	path := storage.NewPath(tmpDir)
	fl := New(path, "_delta_log/_commit.state")
	data := state.CommitState{Version: 0}
	err := fl.Put(data)
	if err != nil {
		t.Errorf("err = %e;", err)
	}

	out, err := fl.Get()
	if err != nil {
		t.Error(err)
	}
	if out.Version != 0 {
		t.Error("Version is not 0")
	}

	err = fl.Put(state.CommitState{Version: 1})
	if err != nil {
		t.Errorf("err = %e;", err)
	}

	out, err = fl.Get()
	if err != nil {
		t.Error(err)
	}
	if out.Version != 1 {
		t.Error("Version is not 1")
	}

	fl.Key = ""
	//if lock is not held, should return ErrorLockNotObtained
	out, err = fl.Get()
	if !errors.Is(err, state.ErrorCanNotReadState) {
		t.Errorf("err = %e;", err)
	}
	if out.Version != 0 {
		t.Error("Version is not 0")
	}

}
