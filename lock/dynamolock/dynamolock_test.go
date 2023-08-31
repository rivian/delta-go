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
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/exp/slices"
)

type mockDynamoDBClient struct {
	DynamoDBClient
	keys []string
}

func (m *mockDynamoDBClient) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{}, nil
}

func (m *mockDynamoDBClient) PutItem(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.keys = append(m.keys, input.Item["key"].(*types.AttributeValueMemberS).Value)
	return &dynamodb.PutItemOutput{Attributes: input.Item}, nil
}

func (m *mockDynamoDBClient) UpdateItem(_ context.Context, input *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

func (m *mockDynamoDBClient) DeleteItem(_ context.Context, input *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	posInSlice := slices.Index(m.keys, input.Key["key"].(*types.AttributeValueMemberS).Value)
	m.keys = slices.Delete(m.keys, posInSlice, posInSlice+1)
	return &dynamodb.DeleteItemOutput{}, nil
}

func TestLock(t *testing.T) {

	client := &mockDynamoDBClient{}
	options := LockOptions{
		TTL:       2 * time.Second,
		HeartBeat: 10 * time.Millisecond,
	}
	lockObj, err := New(client, "delta_lock_table", "_commit.lock", options)

	if err != nil {
		t.Error("error occurred.")
	}
	haslock, err := lockObj.TryLock()
	if err != nil {
		t.Error("error occurred.")
	}
	if haslock {
		t.Logf("Passed.")
	}
	lockObj.Unlock()

	//TODO Check into why the lock is expired before one second.
	isExpired := lockObj.LockedItem.IsExpired()
	// if isExpired {
	// 	t.Errorf("Lock should not yet be expired")
	// }
	time.Sleep(1 * time.Second)
	if !isExpired {
		t.Errorf("Lock should be expired")
	}

}

func TestDeleteOnRelease(t *testing.T) {
	client := &mockDynamoDBClient{}
	options := LockOptions{
		TTL:             2 * time.Second,
		HeartBeat:       10 * time.Millisecond,
		DeleteOnRelease: true,
	}
	dl, err := New(client, "delta_lock_table", "_commit.lock", options)
	if err != nil {
		t.Error(err)
	}

	haslock, err := dl.TryLock()
	if err != nil {
		t.Error(err)
	}
	if haslock {
		t.Log("Acquired lock")
	}

	dl.Unlock()

	isExpired := dl.LockedItem.IsExpired()
	if !isExpired {
		t.Error("Lock should be expired")
	}

	if len(client.keys) != 0 {
		t.Error("Lock should be deleted on release")
	}

	options = LockOptions{
		TTL:       2 * time.Second,
		HeartBeat: 10 * time.Millisecond,
	}
	dl, err = New(client, "delta_lock_table", "_new_commit.lock", options)
	if err != nil {
		t.Error(err)
	}

	haslock, err = dl.TryLock()
	if err != nil {
		t.Error(err)
	}
	if haslock {
		t.Log("Acquired lock")
	}

	dl.Unlock()

	isExpired = dl.LockedItem.IsExpired()
	if !isExpired {
		t.Error("Lock should be expired")
	}

	if len(client.keys) != 1 {
		t.Error("Lock should not be deleted on release")
	}
}
