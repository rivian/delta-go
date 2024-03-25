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
	"testing"

	"github.com/rivian/delta-go/state"
)

func TestGetPutVersion(t *testing.T) {
	localState := new(Store)

	err := localState.Put(state.CommitState{Version: 1})
	if err != nil {
		t.Error(err)
	}

	result, err := localState.Get()
	if err != nil {
		t.Error(err)
	}

	if result.Version != 1 {
		t.Errorf("expected version %d, got %d", 1, result.Version)
	}

	err = localState.Put(state.CommitState{Version: 200})
	if err != nil {
		t.Error(err)
	}

	result, err = localState.Get()
	if err != nil {
		t.Error(err)
	}

	if result.Version != 200 {
		t.Errorf("expected version %d, got %d", 200, result.Version)
	}
}
