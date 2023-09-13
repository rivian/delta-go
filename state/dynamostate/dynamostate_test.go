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
package dynamostate

import (
	"fmt"
	"testing"

	"github.com/rivian/delta-go/internal/dynamodbutils"
	"github.com/rivian/delta-go/state"
)

func TestGet(t *testing.T) {
	dynamoState, err := New(dynamodbutils.NewMockClient(), "storage-table", "_commit.state", Options{})
	if err != nil {
		t.Errorf("Error occurred in retrieving version.")
	}
	commitS, err := dynamoState.Get()
	if err != nil {
		t.Errorf("Error occurred in retrieving version.")
	}
	versionString := fmt.Sprintf("%v", commitS.Version)
	if len(string(versionString)) < 1 {
		t.Errorf("Not a correct version")
	}
}

func TestPut(t *testing.T) {
	dynamoState, err := New(dynamodbutils.NewMockClient(), "storage-table", "_commit.state", Options{})
	if err != nil {
		t.Errorf("Error occurred in retrieving version.")
	}
	commitState := state.CommitState{Version: 0}
	err = dynamoState.Put(commitState)
	if err != nil {
		t.Errorf("Error occurred in PUT.")
	}
}
