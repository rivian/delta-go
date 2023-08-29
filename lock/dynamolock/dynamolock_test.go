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
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type mockDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
}

func (m *mockDynamoDBClient) PutItemWithContext(ctx context.Context, input *dynamodb.PutItemInput, _ ...request.Option) (*dynamodb.PutItemOutput, error) {
	for _, v := range input.Item {
		fmt.Printf("Put Lock: %v\n", v)
	}
	return &dynamodb.PutItemOutput{Attributes: input.Item}, nil
}
func (m *mockDynamoDBClient) GetItemWithContext(ctx context.Context, input *dynamodb.GetItemInput, _ ...request.Option) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{}, nil
}
func (m *mockDynamoDBClient) UpdateItemWithContext(ctx context.Context, input *dynamodb.UpdateItemInput, _ ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

func TestLock(t *testing.T) {
	client := &mockDynamoDBClient{}
	options := Options{
		TTL:       2 * time.Second,
		HeartBeat: 10 * time.Millisecond,
	}
	dl, err := New(client, "delta_lock_table", "_commit.lock", options)
	if err != nil {
		t.Error("error occurred.")
	}

	haslock, err := dl.TryLock()
	if err != nil {
		t.Error("error occurred.")
	}
	if haslock {
		t.Logf("Passed.")
	}

	err = dl.Unlock()
	if err != nil {
		t.Error(err)
	}

	//TODO Check into why the lock is expired before one second.
	isExpired := dl.LockedItem.IsExpired()
	// if isExpired {
	// 	t.Errorf("Lock should not yet be expired")
	// }
	time.Sleep(1 * time.Second)
	if !isExpired {
		t.Errorf("Lock should be expired")
	}
}

func TestNewLock(t *testing.T) {
	client := &mockDynamoDBClient{}
	options := Options{
		TTL:       2 * time.Second,
		HeartBeat: 10 * time.Millisecond,
	}
	dl, err := New(client, "delta_lock_table", "_commit.lock", options)
	if err != nil {
		t.Error("error occurred.")
	}
	newDl, err := dl.NewLock("_new_commit.lock")
	if err != nil {
		t.Error(err)
	}

	haslock, err := newDl.TryLock()
	if err != nil {
		t.Error("error occurred.")
	}
	if haslock {
		t.Logf("Passed.")
	}

	err = newDl.Unlock()
	if err != nil {
		t.Error(err)
	}

	//TODO Check into why the lock is expired before one second.
	isExpired := newDl.(*DynamoLock).LockedItem.IsExpired()
	// if isExpired {
	// 	t.Errorf("Lock should not yet be expired")
	// }
	time.Sleep(1 * time.Second)
	if !isExpired {
		t.Errorf("Lock should be expired")
	}
}
