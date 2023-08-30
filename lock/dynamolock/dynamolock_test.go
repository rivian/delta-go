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
	l, err := New(client, "delta_lock_table", "_commit.lock", options)
	if err != nil {
		t.Error("Failed to create lock")
	}

	haslock, err := l.TryLock()
	if err != nil {
		t.Error("Failed to acquire lock")
	}
	if haslock {
		t.Log("Acquired lock")
	}

	err = l.Unlock()
	if err != nil {
		t.Error(err)
	}
}

func TestNewLock(t *testing.T) {
	client := &mockDynamoDBClient{}
	options := Options{
		TTL: 3 * time.Second,
	}
	l, err := New(client, "delta_lock_table", "_commit.lock", options)
	if err != nil {
		t.Error("Failed to create lock")
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
		t.Error("Failed to acquire lock")
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
