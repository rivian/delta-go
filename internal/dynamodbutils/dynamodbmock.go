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
package dynamodbutils

import (
	"context"
	"errors"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/exp/slices"
)

var (
	ErrorConditionExpressionNotSatisfied error = errors.New("condition expression not satisfied")
	ErrorTableDoesNotExist               error = errors.New("table does not exist")
	ErrorCannotFindItems                 error = errors.New("cannot find items")
)

// Stores the partition and sort key for a DynamoDB table (a primary key is composed of a partition and sort key)
type DynamoDBPrimaryKey struct {
	partitionKey string
	sortKey      string
}

// Stores the data structures used to mock DynamoDB
type MockDynamoDBClient struct {
	DynamoDBClient
	tablesToPrimaryKeys map[string]DynamoDBPrimaryKey
	tablesToItems       map[string][]map[string]types.AttributeValue
}

// Creates a new MockDynamoDBClient instance
func NewMockClient() *MockDynamoDBClient {
	m := new(MockDynamoDBClient)
	m.tablesToPrimaryKeys = make(map[string]DynamoDBPrimaryKey)
	m.tablesToItems = make(map[string][]map[string]types.AttributeValue)
	return m
}

// Gets the map of DynamoDB tables to primary keys
func (m *MockDynamoDBClient) GetTablesToPrimaryKeys() map[string]DynamoDBPrimaryKey {
	return m.tablesToPrimaryKeys
}

// Gets the map of DynamoDB tables to DynamoDB items
func (m *MockDynamoDBClient) GetTablesToItems() map[string][]map[string]types.AttributeValue {
	return m.tablesToItems
}

// Gets an item from a mock DynamoDB table
func (m *MockDynamoDBClient) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	_, ok := m.tablesToItems[*input.TableName]
	if !ok {
		return &dynamodb.GetItemOutput{}, ErrorTableDoesNotExist
	}

	for _, item := range m.tablesToItems[*input.TableName] {
		if IsMapSubset[string, types.AttributeValue](item, input.Key, cmp.AllowUnexported(types.AttributeValueMemberS{})) {
			return &dynamodb.GetItemOutput{Item: item}, nil
		}
	}

	return &dynamodb.GetItemOutput{}, nil
}

// Puts an item into a mock DynamoDB table
func (m *MockDynamoDBClient) PutItem(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	_, ok := m.tablesToPrimaryKeys[*input.TableName]
	if !ok {
		return &dynamodb.PutItemOutput{}, ErrorTableDoesNotExist
	}
	_, ok = m.tablesToItems[*input.TableName]
	if !ok {
		return &dynamodb.PutItemOutput{}, ErrorTableDoesNotExist
	}

	matched := false
	if input.ConditionExpression != nil {
		matched, _ = regexp.MatchString(`attribute_not_exists\(([A-z]+)\)`, *input.ConditionExpression)
	}
	if matched {
		pattern := regexp.MustCompile(`attribute_not_exists\(([A-z]+)\)`)
		subStrs := pattern.FindStringSubmatch(*input.ConditionExpression)
		gio, _ := m.GetItem(context.TODO(), &dynamodb.GetItemInput{TableName: input.TableName, Key: map[string]types.AttributeValue{subStrs[1]: input.Item[subStrs[1]]}})
		if gio.Item != nil {
			return &dynamodb.PutItemOutput{}, ErrorConditionExpressionNotSatisfied
		}
	}

	gio, _ := m.GetItem(context.TODO(), &dynamodb.GetItemInput{TableName: input.TableName, Key: map[string]types.AttributeValue{m.tablesToPrimaryKeys[*input.TableName].partitionKey: input.Item[m.tablesToPrimaryKeys[*input.TableName].partitionKey], m.tablesToPrimaryKeys[*input.TableName].sortKey: input.Item[m.tablesToPrimaryKeys[*input.TableName].sortKey]}})
	if gio.Item != nil {
		posInSlice := slices.IndexFunc(m.tablesToItems[*input.TableName], func(i map[string]types.AttributeValue) bool {
			return cmp.Equal(i, gio.Item, cmp.AllowUnexported(types.AttributeValueMemberS{}))
		})
		m.tablesToItems[*input.TableName] = slices.Replace[[]map[string]types.AttributeValue](m.tablesToItems[*input.TableName], posInSlice, posInSlice+1, input.Item)
		return &dynamodb.PutItemOutput{}, nil
	}

	m.tablesToItems[*input.TableName] = append(m.tablesToItems[*input.TableName], input.Item)
	return &dynamodb.PutItemOutput{}, nil
}

// Updates an item in a mock DynamoDB table
func (m *MockDynamoDBClient) UpdateItem(_ context.Context, input *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

// Deletes an item from a mock DynamoDB table
func (m *MockDynamoDBClient) DeleteItem(_ context.Context, input *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	_, ok := m.tablesToItems[*input.TableName]
	if !ok {
		return &dynamodb.DeleteItemOutput{}, ErrorTableDoesNotExist
	}

	var itemToDelete map[string]types.AttributeValue
	for _, item := range m.tablesToItems[*input.TableName] {
		if IsMapSubset[string, types.AttributeValue](item, input.Key, cmp.AllowUnexported(types.AttributeValueMemberS{})) {
			itemToDelete = item
		}
	}

	posInSlice := slices.IndexFunc(m.tablesToItems[*input.TableName], func(v map[string]types.AttributeValue) bool {
		return cmp.Equal(v, itemToDelete, cmp.AllowUnexported(types.AttributeValueMemberS{}))
	})
	m.tablesToItems[*input.TableName] = slices.Delete(m.tablesToItems[*input.TableName], posInSlice, posInSlice+1)
	return &dynamodb.DeleteItemOutput{}, nil
}

// Creates a mock DynamoDB table
func (m *MockDynamoDBClient) CreateTable(_ context.Context, input *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	m.tablesToPrimaryKeys[*input.TableName] = DynamoDBPrimaryKey{}

	posInSlice := slices.IndexFunc(input.KeySchema, func(kse types.KeySchemaElement) bool {
		return kse.KeyType == types.KeyTypeHash
	})
	primaryKey, ok := m.tablesToPrimaryKeys[*input.TableName]
	if ok && posInSlice != -1 {
		primaryKey.partitionKey = *input.KeySchema[posInSlice].AttributeName
		m.tablesToPrimaryKeys[*input.TableName] = primaryKey
	}

	posInSlice = slices.IndexFunc(input.KeySchema, func(kse types.KeySchemaElement) bool {
		return kse.KeyType == types.KeyTypeRange
	})
	primaryKey, ok = m.tablesToPrimaryKeys[*input.TableName]
	if ok && posInSlice != -1 {
		primaryKey.sortKey = *input.KeySchema[posInSlice].AttributeName
		m.tablesToPrimaryKeys[*input.TableName] = primaryKey
	}

	m.tablesToItems[*input.TableName] = []map[string]types.AttributeValue{}
	return &dynamodb.CreateTableOutput{}, nil
}

// Describes a mock DynamoDB table
func (m *MockDynamoDBClient) DescribeTable(_ context.Context, input *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	_, ok := m.tablesToItems[*input.TableName]
	if ok {
		return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: "ACTIVE"}}, nil
	}

	return &dynamodb.DescribeTableOutput{}, ErrorTableDoesNotExist
}

// Queries a mock DynamoDB table
func (m *MockDynamoDBClient) Query(_ context.Context, input *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	_, ok := m.tablesToItems[*input.TableName]
	if !ok {
		return &dynamodb.QueryOutput{}, ErrorTableDoesNotExist
	}

	pattern := regexp.MustCompile("([A-z]+) = (:[A-z]+)")
	subStrs := pattern.FindStringSubmatch(*input.KeyConditionExpression)

	items := []map[string]types.AttributeValue{}
	for _, item := range m.tablesToItems[*input.TableName] {
		if IsMapSubset[string, types.AttributeValue](item, map[string]types.AttributeValue{subStrs[1]: input.ExpressionAttributeValues[subStrs[2]]}, cmp.AllowUnexported(types.AttributeValueMemberS{})) {
			items = append(items, item)
		}
	}

	if len(items) != 0 {
		slices.Reverse[[]map[string]types.AttributeValue](items)
		return &dynamodb.QueryOutput{Items: items}, nil
	}

	return &dynamodb.QueryOutput{}, ErrorCannotFindItems
}

// Checks if a map is a subset of another map
func IsMapSubset[K, V comparable](m map[K]V, sub map[K]V, opts ...cmp.Option) bool {
	if len(sub) > len(m) {
		return false
	}

	for k, vsub := range sub {
		if vm, found := m[k]; !found || !cmp.Equal(vm, vsub, opts...) {
			return false
		}
	}

	return true
}
