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
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rivian/delta-go/state"
)

const KEY string = "key"
const VERSION string = "version"

type DynamoState struct {
	Table  string
	Key    string
	Client dynamodbiface.DynamoDBAPI
}

// Compile time check that DynamoState implements state.StateStore
var _ state.StateStore = (*DynamoState)(nil)

func New(client dynamodbiface.DynamoDBAPI, tableName string, key string) (*DynamoState, error) {
	tb := new(DynamoState)
	tb.Table = tableName
	tb.Key = key
	tb.Client = client
	//TODO Check if table exists and handle exception
	// describeTableInput := &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}
	// desciribeTableOutput, err := client.DescribeTable(describeTableInput)
	// if desciribeTableOutput.Table.TableStatus != aws.String(dynamodb.TableStatusActive) {
	// 	return nil, errors.New("dynamodb table is not active")
	// }
	// if err != nil {
	// 	return nil, errors.New("dynamodb table is not active")
	// }
	return tb, nil
}

func (l *DynamoState) Get() (state.CommitState, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(l.Table),
		Key: map[string]*dynamodb.AttributeValue{
			KEY: {
				S: aws.String(l.Key),
			},
		},
	}

	// Call the GetItem operation to retrieve the item with the specified key value.
	result, err := l.Client.GetItem(input)
	if err != nil {
		//TODO wrap error rather than printing
		fmt.Println("Error retrieving item.", err)
		return state.CommitState{Version: -1}, err
	}
	if result.Item == nil {
		//TODO wrap error rather than printing
		fmt.Println("Couldn't retrieve any item.", err)
		return state.CommitState{Version: -1}, err
	}

	versionValue := *result.Item["version"].S
	version, err := strconv.Atoi(versionValue)
	if err != nil {
		//TODO wrap error rather than printing
		fmt.Println("Error converting attribute to int:", err)
		return state.CommitState{Version: -1}, err
	}

	commit := state.CommitState{Version: int64(version)}
	return commit, nil
}

func (l *DynamoState) Put(commitS state.CommitState) error {
	versionString := fmt.Sprintf("%v", commitS.Version)

	// Create a PutItemInput object with the item data
	input := &dynamodb.PutItemInput{
		TableName: aws.String(l.Table),
		Item: map[string]*dynamodb.AttributeValue{
			KEY: {
				S: aws.String(l.Key),
			},
			VERSION: {
				S: aws.String(versionString),
			},
		},
	}

	// Call PutItem to insert the new item into the table
	_, err := l.Client.PutItem(input)
	if err != nil {
		//TODO wrap error rather than printing
		fmt.Println("Error inserting item.", err)
		return err
	}
	return nil
}
