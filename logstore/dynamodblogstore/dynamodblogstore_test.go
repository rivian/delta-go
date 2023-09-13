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
	"testing"

	"github.com/rivian/delta-go/internal/dynamodbutils"
	"github.com/rivian/delta-go/logstore"
	"github.com/rivian/delta-go/storage"
)

func TestGet(t *testing.T) {
	lso := DynamoDBLogStoreOptions{Client: dynamodbutils.NewMockClient(), TableName: "log_store"}
	ls, err := NewDynamoDBLogStore(lso)
	if err != nil {
		t.Error("failed to create DynamoDB log store")
	}

	ece, err := logstore.NewCommitEntry(storage.NewPath("usr/local/"), storage.NewPath("01.json"), storage.NewPath("01.tmp"), false, uint64(0))
	if err != nil {
		t.Error("failed to create commit entry")
	}
	err = ls.Put(ece, false)
	if err != nil {
		t.Error("failed to put commit entry")
	}

	ece, err = logstore.NewCommitEntry(storage.NewPath("usr/local/"), storage.NewPath("01.json"), storage.NewPath("01.tmp"), false, uint64(0))
	if err != nil {
		t.Error("failed to create commit entry")
	}
	err = ls.Put(ece, false)
	if err == nil {
		t.Error("commit entry already exists")
	}

	ece, err = ls.Get(storage.NewPath("usr/local/"), storage.NewPath("01.json"))
	if err != nil || ece == nil {
		t.Error("failed to get commit entry")
	}

	ece, err = ls.Get(storage.NewPath("usr/local/A"), storage.NewPath("01.json"))
	if err == nil || ece != nil {
		t.Error("no commit entry should be returned")
	}

	_, err = ls.Get(storage.NewPath("usr/local/"), storage.NewPath("02.json"))
	if err == nil || ece != nil {
		t.Error("no commit entry should be returned")
	}
}

func TestGetLatest(t *testing.T) {
	lso := DynamoDBLogStoreOptions{Client: dynamodbutils.NewMockClient(), TableName: "log_store"}
	ls, err := NewDynamoDBLogStore(lso)
	if err != nil {
		t.Error("failed to create DynamoDB log store")
	}

	eceFirst, err := logstore.NewCommitEntry(storage.NewPath("usr/local/"), storage.NewPath("01.json"), storage.NewPath("01.tmp"), false, uint64(0))
	if err != nil {
		t.Error("failed to create commit entry")
	}
	err = ls.Put(eceFirst, false)
	if err != nil {
		t.Error("failed to put commit entry")
	}

	eceSecond, err := logstore.NewCommitEntry(storage.NewPath("usr/local/"), storage.NewPath("02.json"), storage.NewPath("02.tmp"), false, uint64(0))
	if err != nil {
		t.Error("failed to create commit entry")
	}
	err = ls.Put(eceSecond, false)
	if err != nil {
		t.Error("failed to put commit entry")
	}

	eceLatest, err := ls.GetLatest(storage.NewPath("usr/local/"))
	if err != nil || eceLatest == nil {
		t.Error("failed to get latest commit entry")
	}
	if eceSecond.FileName.Raw != eceLatest.FileName.Raw || eceSecond.TempPath.Raw != eceLatest.TempPath.Raw {
		t.Error("got incorrect latest commit entry")
	}

	_, err = ls.GetLatest(storage.NewPath("usr/local/A"))
	if err == nil {
		t.Error("no commit entry should be returned")
	}
}

func TestPutOverwrite(t *testing.T) {
	lso := DynamoDBLogStoreOptions{Client: dynamodbutils.NewMockClient(), TableName: "log_store"}
	ls, err := NewDynamoDBLogStore(lso)
	if err != nil {
		t.Error("failed to create DynamoDB log store")
	}

	ece, err := logstore.NewCommitEntry(storage.NewPath("usr/local/"), storage.NewPath("01.json"), storage.NewPath("01.tmp"), false, uint64(0))
	if err != nil {
		t.Error("failed to create commit entry")
	}
	err = ls.Put(ece, true)
	if err != nil {
		t.Error("failed to put commit entry")
	}

	ece, err = logstore.NewCommitEntry(storage.NewPath("usr/local/"), storage.NewPath("01.json"), storage.NewPath("01.tmp"), true, uint64(0))
	if err != nil {
		t.Error("failed to create commit entry")
	}
	err = ls.Put(ece, true)
	if err != nil {
		t.Error("failed to overwrite commit entry")
	}

	if len(ls.client.(*dynamodbutils.MockDynamoDBClient).GetTablesToItems()["log_store"]) != 1 {
		t.Error("incorrect number of items in table")
	}

	ece, err = ls.GetLatest(storage.NewPath("usr/local/"))
	if err != nil {
		t.Error("failed to get latest commit entry")
	}
	if ece.Complete != true {
		t.Error("commit entry should be complete")
	}
}
