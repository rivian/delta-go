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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rivian/delta-go/state"
)

type mockDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
}

func (m *mockDynamoDBClient) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return nil, nil
}

func (m *mockDynamoDBClient) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"version": {
				S: aws.String("0"),
			},
		},
	}, nil
}

// Create a function to return the mock client
func createMockDynamoDBClient() *mockDynamoDBClient {
	return &mockDynamoDBClient{}
}

func TestGet(t *testing.T) {
	// svc, err := dynamolock.GetDynamoDBClient()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	dynamoState, err := New(createMockDynamoDBClient(), "storage-table", "_commit.state")
	if err != nil {
		t.Errorf("Error occurred in retriving  version.")
	}
	commitS, err := dynamoState.Get()
	if err != nil {
		t.Errorf("Error occurred in retriving  version.")
	}
	versionString := fmt.Sprintf("%v", commitS.Version)
	if len(string(versionString)) < 1 {
		t.Errorf("Not a correct version")
	}
}

func TestPut(t *testing.T) {
	dynamoState, err := New(createMockDynamoDBClient(), "storage-table", "_commit.state")
	if err != nil {
		t.Errorf("Error occurred in retriving  version.")
	}
	commitState := state.CommitState{Version: 0}
	err = dynamoState.Put(commitState)
	if err != nil {
		t.Errorf("Error occurred in PUT.")
	}
}
