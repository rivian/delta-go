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
package dynamolock

import (
	"testing"
	"time"

	"github.com/rivian/delta-go/dynamodbutils"
)

func TestLock(t *testing.T) {
	client := dynamodbutils.NewMockClient()
	options := Options{
		TTL:       2 * time.Second,
		HeartBeat: 10 * time.Millisecond,
	}
	l, err := New(client, "delta_lock_table", "_commit.lock", options)
	if err != nil {
		t.Errorf("Failed to create lock: %v", err)
	}

	haslock, err := l.TryLock()
	if err != nil {
		t.Errorf("Failed to acquire lock: %v", err)
	}
	if haslock {
		t.Log("Acquired lock")
	}

	if err = l.Unlock(); err != nil {
		t.Error(err)
	}
}

func TestNewLock(t *testing.T) {
	client := dynamodbutils.NewMockClient()
	options := Options{
		TTL: 3 * time.Second,
	}
	l, err := New(client, "delta_lock_table", "_commit.lock", options)
	if err != nil {
		t.Errorf("Failed to create lock: %v", err)
	}
	nl, err := l.NewLock("_new_commit.lock")
	if err != nil {
		t.Error(err)
	}

	if nl.(*DynamoLock).key != "_new_commit.lock" {
		t.Error("Name of key should be updated")
	}

	haslock, err := nl.TryLock()
	if err != nil {
		t.Errorf("Failed to acquire lock: %v", err)
	}
	if haslock {
		t.Log("Acquired lock")
	}

	time.Sleep(500 * time.Millisecond)

	isExpired := nl.(*DynamoLock).lockedItem.IsExpired()
	if isExpired {
		t.Error("Lock should not be expired")
	}

	time.Sleep(4 * time.Second)

	isExpired = nl.(*DynamoLock).lockedItem.IsExpired()
	if !isExpired {
		t.Error("Lock should be expired")
	}
}

func TestDeleteOnRelease(t *testing.T) {
	client := dynamodbutils.NewMockClient()
	opts := Options{
		TTL:             2 * time.Second,
		HeartBeat:       10 * time.Millisecond,
		DeleteOnRelease: true,
	}
	l, err := New(client, "delta_lock_table", "_commit.lock", opts)
	if err != nil {
		t.Error(err)
	}

	haslock, err := l.TryLock()
	if err != nil {
		t.Error(err)
	}
	if haslock {
		t.Log("Acquired lock")
	}

	if err := l.Unlock(); err != nil {
		t.Errorf("Failed to unlock: %v", err)
	}

	isExpired := l.lockedItem.IsExpired()
	if !isExpired {
		t.Error("Lock should be expired")
	}

	items, ok := client.TablesToItems().Get("delta_lock_table")
	if !ok {
		t.Error("Failed to get table items")
	}
	if len(items) != 0 {
		t.Error("Lock should be deleted on release")
	}

	opts = Options{
		TTL:       2 * time.Second,
		HeartBeat: 10 * time.Millisecond,
	}
	l, err = New(client, "delta_lock_table", "_new_commit.lock", opts)
	if err != nil {
		t.Error(err)
	}

	haslock, err = l.TryLock()
	if err != nil {
		t.Error(err)
	}
	if haslock {
		t.Log("Acquired lock")
	}

	if err := l.Unlock(); err != nil {
		t.Errorf("Failed to unlock: %v", err)
	}

	isExpired = l.lockedItem.IsExpired()
	if !isExpired {
		t.Error("Lock should be expired")
	}

	items, ok = client.TablesToItems().Get("delta_lock_table")
	if !ok {
		t.Error("Failed to get table items")
	}
	if len(items) != 1 {
		t.Error("Lock should not be deleted on release")
	}
}
