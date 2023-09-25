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
	"context"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/rivian/delta-go/internal/dynamodbutils"
	"github.com/rivian/delta-go/state"
)

// Represents attribute names in DynamoDB items
type Attribute string

const (
	Key                                Attribute = "key"
	Version                            Attribute = "version"
	DefaultMaxRetryTableCreateAttempts uint16    = 20
	DefaultRCU                         int64     = 5
	DefaultWCU                         int64     = 5
)

type DynamoState struct {
	Table  string
	Key    string
	Client dynamodbutils.Client
}

type Options struct {
	MaxRetryTableCreateAttempts uint16
	// The number of read capacity units which can be consumed per second (https://aws.amazon.com/dynamodb/pricing/provisioned/)
	RCU int64
	// The number of write capacity units which can be consumed per second (https://aws.amazon.com/dynamodb/pricing/provisioned/)
	WCU int64
}

// Sets the default options
func (opts *Options) setOptionsDefaults() {
	if opts.MaxRetryTableCreateAttempts == 0 {
		opts.MaxRetryTableCreateAttempts = DefaultMaxRetryTableCreateAttempts
	}
	if opts.RCU == 0 {
		opts.RCU = DefaultRCU
	}
	if opts.WCU == 0 {
		opts.WCU = DefaultWCU
	}
}

// Compile time check that DynamoState implements state.StateStore
var _ state.StateStore = (*DynamoState)(nil)

func New(client dynamodbutils.Client, tableName string, key string, opts Options) (*DynamoState, error) {
	opts.setOptionsDefaults()

	createTableInput := dynamodb.CreateTableInput{
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(string(Key)),
				KeyType:       types.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(opts.RCU),
			WriteCapacityUnits: aws.Int64(opts.WCU),
		},
		TableName: aws.String(tableName),
	}
	dynamodbutils.CreateTableIfNotExists(client, tableName, createTableInput, opts.MaxRetryTableCreateAttempts)

	tb := new(DynamoState)
	tb.Table = tableName
	tb.Key = key
	tb.Client = client
	return tb, nil
}

func (l *DynamoState) Get() (state.CommitState, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(l.Table),
		Key: map[string]types.AttributeValue{
			string(Key): &types.AttributeValueMemberS{Value: l.Key},
		},
	}

	// Call the GetItem operation to retrieve the item with the specified key value.
	result, err := l.Client.GetItem(context.TODO(), input)
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

	versionValue := result.Item[string(Version)].(*types.AttributeValueMemberS).Value
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
		Item: map[string]types.AttributeValue{
			string(Key):     &types.AttributeValueMemberS{Value: l.Key},
			string(Version): &types.AttributeValueMemberS{Value: versionString},
		},
	}

	// Call PutItem to insert the new item into the table
	_, err := l.Client.PutItem(context.TODO(), input)
	if err != nil {
		//TODO wrap error rather than printing
		fmt.Println("Error inserting item.", err)
		return err
	}
	return nil
}
