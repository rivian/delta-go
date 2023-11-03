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
package dynamodblogstore

import (
	"testing"

	"github.com/rivian/delta-go/internal/dynamodbutils"
	"github.com/rivian/delta-go/logstore"
	"github.com/rivian/delta-go/storage"
)

func TestPut(t *testing.T) {
	o := Options{Client: dynamodbutils.NewMockClient(), TableName: "log_store"}
	ls, err := New(o)
	if err != nil {
		t.Error("Failed to create DynamoDB log store")
	}

	ce := logstore.New(storage.NewPath("usr/local/"), storage.NewPath("01.json"), storage.NewPath("01.tmp"), false, uint64(0))
	if err != nil {
		t.Error("Failed to create commit entry")
	}
	if err := ls.Put(ce, true); err != nil {
		t.Error("Failed to put commit entry")
	}

	ce = logstore.New(storage.NewPath("usr/local/"), storage.NewPath("01.json"), storage.NewPath("01.tmp"), true, uint64(0))
	if err != nil {
		t.Error("Failed to create commit entry")
	}
	if err := ls.Put(ce, true); err != nil {
		t.Error("Failed to overwrite commit entry")
	}

	items, ok := ls.client.(*dynamodbutils.MockClient).TablesToItems().Get("log_store")
	if !ok {
		t.Error("Failed to get table items")
	}
	if len(items) != 1 {
		t.Error("Incorrect number of items in table")
	}

	ce, err = ls.Latest(storage.NewPath("usr/local/"))
	if err != nil {
		t.Error("Failed to get latest commit entry")
	}
	if ce.IsComplete() != true {
		t.Error("Commit entry should be complete")
	}
}

func TestGet(t *testing.T) {
	o := Options{Client: dynamodbutils.NewMockClient(), TableName: "log_store"}
	ls, err := New(o)
	if err != nil {
		t.Error("Failed to create new DynamoDB log store")
	}

	ce := logstore.New(storage.NewPath("usr/local/"), storage.NewPath("01.json"), storage.NewPath("01.tmp"), false, uint64(0))
	if err != nil {
		t.Error("Failed to create commit entry")
	}
	if err := ls.Put(ce, false); err != nil {
		t.Error("Failed to put commit entry")
	}

	ce = logstore.New(storage.NewPath("usr/local/"), storage.NewPath("01.json"), storage.NewPath("01.tmp"), false, uint64(0))
	if err != nil {
		t.Error("Failed to create commit entry")
	}
	if err := ls.Put(ce, false); err == nil {
		t.Error("Commit entry already exists")
	}

	ce, err = ls.Get(storage.NewPath("usr/local/"), storage.NewPath("01.json"))
	if err != nil || ce == nil {
		t.Error("Failed to get commit entry")
	}

	ce, err = ls.Get(storage.NewPath("usr/local/A"), storage.NewPath("01.json"))
	if err == nil || ce != nil {
		t.Error("No commit entry should be returned")
	}

	_, err = ls.Get(storage.NewPath("usr/local/"), storage.NewPath("02.json"))
	if err == nil || ce != nil {
		t.Error("No commit entry should be returned")
	}
}

func TestLatest(t *testing.T) {
	o := Options{Client: dynamodbutils.NewMockClient(), TableName: "log_store"}
	ls, err := New(o)
	if err != nil {
		t.Error("Failed to create DynamoDB log store")
	}

	firstEntry := logstore.New(storage.NewPath("usr/local/"), storage.NewPath("01.json"), storage.NewPath("01.tmp"), false, uint64(0))
	if err != nil {
		t.Error("Failed to create commit entry")
	}
	if err := ls.Put(firstEntry, false); err != nil {
		t.Error("Failed to put commit entry")
	}

	secondEntry := logstore.New(storage.NewPath("usr/local/"), storage.NewPath("02.json"), storage.NewPath("02.tmp"), false, uint64(0))
	if err != nil {
		t.Error("Failed to create commit entry")
	}
	if err := ls.Put(secondEntry, false); err != nil {
		t.Error("Failed to put commit entry")
	}

	latest, err := ls.Latest(storage.NewPath("usr/local/"))
	if err != nil || latest == nil {
		t.Error("Failed to get latest commit entry")
	}
	if secondEntry.FileName().Raw != latest.FileName().Raw || secondEntry.TempPath().Raw != latest.TempPath().Raw {
		t.Error("Got incorrect latest commit entry")
	}

	if _, err = ls.Latest(storage.NewPath("usr/local/A")); err == nil {
		t.Error("No commit entry should be returned")
	}
}
