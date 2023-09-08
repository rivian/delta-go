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

func TestGetExternalEntry(t *testing.T) {
	lso := DynamoDBLogStoreOptions{Client: dynamodbutils.NewMockClient(), TableName: "log_store"}
	ls, err := NewDynamoDBLogStore(lso)
	if err != nil {
		t.Error("failed to create DynamoDB log store")
	}

	ece, err := logstore.NewExternalCommitEntry(*storage.NewPath("usr/local/"), *storage.NewPath("01.json"), *storage.NewPath("01.tmp"), false, uint64(0))
	if err != nil {
		t.Error("failed to create external commit entry")
	}
	err = ls.PutExternalEntry(ece, false)
	if err != nil {
		t.Error("failed to put external commit entry")
	}

	ece, err = logstore.NewExternalCommitEntry(*storage.NewPath("usr/local/"), *storage.NewPath("01.json"), *storage.NewPath("01.tmp"), false, uint64(0))
	if err != nil {
		t.Error("failed to create external commit entry")
	}
	err = ls.PutExternalEntry(ece, false)
	if err == nil {
		t.Error("external commit entry already exists")
	}

	ece, err = ls.GetExternalEntry(storage.NewPath("usr/local/"), storage.NewPath("01.json"))
	if err != nil || ece == nil {
		t.Error("failed to get external commit entry")
	}

	ece, err = ls.GetExternalEntry(storage.NewPath("usr/local/A"), storage.NewPath("01.json"))
	if err == nil || ece != nil {
		t.Error("no external commit entry should be returned")
	}

	_, err = ls.GetExternalEntry(storage.NewPath("usr/local/"), storage.NewPath("02.json"))
	if err == nil || ece != nil {
		t.Error("no external commit entry should be returned")
	}
}

func TestGetLatestExternalEntry(t *testing.T) {
	lso := DynamoDBLogStoreOptions{Client: dynamodbutils.NewMockClient(), TableName: "log_store"}
	ls, err := NewDynamoDBLogStore(lso)
	if err != nil {
		t.Error("failed to create DynamoDB log store")
	}

	eceFirst, err := logstore.NewExternalCommitEntry(*storage.NewPath("usr/local/"), *storage.NewPath("01.json"), *storage.NewPath("01.tmp"), false, uint64(0))
	if err != nil {
		t.Error("failed to create external commit entry")
	}
	err = ls.PutExternalEntry(eceFirst, false)
	if err != nil {
		t.Error("failed to put external commit entry")
	}

	eceSecond, err := logstore.NewExternalCommitEntry(*storage.NewPath("usr/local/"), *storage.NewPath("02.json"), *storage.NewPath("02.tmp"), false, uint64(0))
	if err != nil {
		t.Error("failed to create external commit entry")
	}
	err = ls.PutExternalEntry(eceSecond, false)
	if err != nil {
		t.Error("failed to put external commit entry")
	}

	eceLatest, err := ls.GetLatestExternalEntry(storage.NewPath("usr/local/"))
	if err != nil || eceLatest == nil {
		t.Error("failed to get latest external commit entry")
	}
	if eceSecond.FileName.Raw != eceLatest.FileName.Raw || eceSecond.TempPath.Raw != eceLatest.TempPath.Raw {
		t.Error("got incorrect latest external commit entry")
	}

	_, err = ls.GetLatestExternalEntry(storage.NewPath("usr/local/A"))
	if err == nil {
		t.Error("no external commit entry should be returned")
	}
}

func TestPutExternalEntryOverwrite(t *testing.T) {
	lso := DynamoDBLogStoreOptions{Client: dynamodbutils.NewMockClient(), TableName: "log_store"}
	ls, err := NewDynamoDBLogStore(lso)
	if err != nil {
		t.Error("failed to create DynamoDB log store")
	}

	ece, err := logstore.NewExternalCommitEntry(*storage.NewPath("usr/local/"), *storage.NewPath("01.json"), *storage.NewPath("01.tmp"), false, uint64(0))
	if err != nil {
		t.Error("failed to create external commit entry")
	}
	err = ls.PutExternalEntry(ece, true)
	if err != nil {
		t.Error("failed to put external commit entry")
	}

	ece, err = logstore.NewExternalCommitEntry(*storage.NewPath("usr/local/"), *storage.NewPath("01.json"), *storage.NewPath("01.tmp"), true, uint64(0))
	if err != nil {
		t.Error("failed to create external commit entry")
	}
	err = ls.PutExternalEntry(ece, true)
	if err != nil {
		t.Error("failed to overwrite external commit entry")
	}

	if len(ls.client.(*dynamodbutils.MockDynamoDBClient).GetTablesToItems()["log_store"]) != 1 {
		t.Error("incorrect number of items in table")
	}

	ece, err = ls.GetLatestExternalEntry(storage.NewPath("usr/local/"))
	if err != nil {
		t.Error("failed to get latest external commit entry")
	}
	if ece.Complete != true {
		t.Error("external commit entry should be complete")
	}
}
