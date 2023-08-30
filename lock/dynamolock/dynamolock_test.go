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

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rivian/delta-go/internal/utils"
)

type mockDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
	keys []*dynamodb.AttributeValue
}

func (m *mockDynamoDBClient) PutItemWithContext(ctx context.Context, input *dynamodb.PutItemInput, _ ...request.Option) (*dynamodb.PutItemOutput, error) {
	m.keys = append(m.keys, input.Item["key"])
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDynamoDBClient) GetItemWithContext(ctx context.Context, input *dynamodb.GetItemInput, _ ...request.Option) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{}, nil
}

func (m *mockDynamoDBClient) UpdateItemWithContext(ctx context.Context, input *dynamodb.UpdateItemInput, _ ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

func (m *mockDynamoDBClient) DeleteItemWithContext(ctx context.Context, input *dynamodb.DeleteItemInput, _ ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	m.keys = utils.RemoveElementFromSlice(m.keys, utils.FindElementPosInSlice(m.keys, input.Key["key"]))
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
		t.Error()
	}

	haslock, err := dl.TryLock()
	if err != nil {
		t.Error()
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
		t.Error()
	}

	haslock, err = dl.TryLock()
	if err != nil {
		t.Error()
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
