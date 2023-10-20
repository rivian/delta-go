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
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	cmap "github.com/orcaman/concurrent-map/v2"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

// PrimaryKey stores the partition and sort key for a DynamoDB table (a primary key is composed of a partition and sort key).
type PrimaryKey struct {
	partitionKey string
	sortKey      string
}

// MockClient stores the data structures used to mock DynamoDB.
type MockClient struct {
	Client
	tablesToKeys  cmap.ConcurrentMap[string, PrimaryKey]
	tablesToItems cmap.ConcurrentMap[string, []map[string]types.AttributeValue]
	mu            sync.Mutex
}

// Compile time check that MockClient implements DynamoDBClient
var _ Client = (*MockClient)(nil)

// NewMockClient creates a new MockClient instance.
func NewMockClient() *MockClient {
	m := new(MockClient)
	m.tablesToKeys = cmap.New[PrimaryKey]()
	m.tablesToItems = cmap.New[[]map[string]types.AttributeValue]()

	return m
}

// TablesToKeys gets the map of DynamoDB tables to primary keys.
func (m *MockClient) TablesToKeys() cmap.ConcurrentMap[string, PrimaryKey] {
	return m.tablesToKeys
}

// TablesToItems gets the map of DynamoDB tables to DynamoDB items.
func (m *MockClient) TablesToItems() cmap.ConcurrentMap[string, []map[string]types.AttributeValue] {
	return m.tablesToItems
}

// GetItem gets a shallow clone of an item in a mock DynamoDB table.
func (m *MockClient) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	items, ok := m.tablesToItems.Get(*input.TableName)
	if !ok {
		return &dynamodb.GetItemOutput{}, errors.New("table does not exist")
	}

	for _, item := range items {
		if m.isMatch(item, input.Key, cmp.AllowUnexported(types.AttributeValueMemberS{})) {
			copiedItem := maps.Clone(item)

			return &dynamodb.GetItemOutput{Item: copiedItem}, nil
		}
	}

	return &dynamodb.GetItemOutput{}, nil
}

// getNonClonedItem gets a non-cloned item from a mock DynamoDB table.
func (m *MockClient) getNonClonedItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	items, ok := m.tablesToItems.Get(*input.TableName)
	if !ok {
		return &dynamodb.GetItemOutput{}, errors.New("table does not exist")
	}

	for _, item := range items {
		if m.isMatch(item, input.Key, cmp.AllowUnexported(types.AttributeValueMemberS{})) {
			return &dynamodb.GetItemOutput{Item: item}, nil
		}
	}

	return &dynamodb.GetItemOutput{}, nil
}

// PutItem puts an item into a mock DynamoDB table.
func (m *MockClient) PutItem(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tablesToKeys.Get(*input.TableName); !ok {
		return &dynamodb.PutItemOutput{}, errors.New("table does not exist")
	}
	items, ok := m.tablesToItems.Get(*input.TableName)
	if !ok {
		return &dynamodb.PutItemOutput{}, errors.New("table does not exist")
	}

	matched := false
	if input.ConditionExpression != nil {
		matched, _ = regexp.MatchString(`attribute_not_exists\(([A-z]+)\)`, *input.ConditionExpression)
	}
	if matched {
		var (
			pattern = regexp.MustCompile(`attribute_not_exists\(([A-z]+)\)`)
			subStrs = pattern.FindStringSubmatch(*input.ConditionExpression)
		)
		if gio, _ := m.getNonClonedItem(&dynamodb.GetItemInput{TableName: input.TableName, Key: map[string]types.AttributeValue{subStrs[1]: input.Item[subStrs[1]]}}); gio.Item != nil {
			return &dynamodb.PutItemOutput{}, errors.New("condition expression not satisfied")
		}
	}

	key, ok := m.tablesToKeys.Get(*input.TableName)
	if !ok {
		return &dynamodb.PutItemOutput{}, errors.New("table does not exist")
	}
	if gio, _ := m.getNonClonedItem(&dynamodb.GetItemInput{TableName: input.TableName, Key: map[string]types.AttributeValue{key.partitionKey: input.Item[key.partitionKey], key.sortKey: input.Item[key.sortKey]}}); gio.Item != nil {
		items, ok := m.tablesToItems.Get(*input.TableName)
		if !ok {
			return &dynamodb.PutItemOutput{}, errors.New("table does not exist")
		}

		pos := slices.IndexFunc(items, func(i map[string]types.AttributeValue) bool {
			return cmp.Equal(i, gio.Item, cmp.AllowUnexported(types.AttributeValueMemberS{}), cmp.AllowUnexported(types.AttributeValueMemberN{}))
		})
		m.tablesToItems.Set(*input.TableName, slices.Replace[[]map[string]types.AttributeValue](items, pos, pos+1, input.Item))

		return &dynamodb.PutItemOutput{}, nil
	}

	m.tablesToItems.Set(*input.TableName, append(items, input.Item))

	return &dynamodb.PutItemOutput{}, nil
}

// UpdateItem does nothing since it is not required to be implemented.
func (m *MockClient) UpdateItem(_ context.Context, input *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

// DeleteItem deletes an item from a mock DynamoDB table.
func (m *MockClient) DeleteItem(_ context.Context, input *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	items, ok := m.tablesToItems.Get(*input.TableName)
	if !ok {
		return &dynamodb.DeleteItemOutput{}, errors.New("table does not exist")
	}

	var itemToDelete map[string]types.AttributeValue
	for _, item := range items {
		if m.isMatch(item, input.Key, cmp.AllowUnexported(types.AttributeValueMemberS{})) {
			itemToDelete = item
			break
		}
	}

	pos := slices.IndexFunc(items, func(v map[string]types.AttributeValue) bool {
		return cmp.Equal(v, itemToDelete, cmp.AllowUnexported(types.AttributeValueMemberS{}))
	})
	m.tablesToItems.Set(*input.TableName, slices.Delete(items, pos, pos+1))

	return &dynamodb.DeleteItemOutput{}, nil
}

// CreateTable creates a mock DynamoDB table.
func (m *MockClient) CreateTable(_ context.Context, input *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tablesToKeys.Set(*input.TableName, PrimaryKey{})

	pos := slices.IndexFunc(input.KeySchema, func(kse types.KeySchemaElement) bool {
		return kse.KeyType == types.KeyTypeHash
	})
	key, ok := m.tablesToKeys.Get(*input.TableName)
	if ok && pos != -1 {
		key.partitionKey = *input.KeySchema[pos].AttributeName
		m.tablesToKeys.Set(*input.TableName, key)
	}

	pos = slices.IndexFunc(input.KeySchema, func(kse types.KeySchemaElement) bool {
		return kse.KeyType == types.KeyTypeRange
	})
	key, ok = m.tablesToKeys.Get(*input.TableName)
	if ok && pos != -1 {
		key.sortKey = *input.KeySchema[pos].AttributeName
		m.tablesToKeys.Set(*input.TableName, key)
	}

	m.tablesToItems.Set(*input.TableName, []map[string]types.AttributeValue{})

	return &dynamodb.CreateTableOutput{}, nil
}

// DescribeTable describes a mock DynamoDB table.
func (m *MockClient) DescribeTable(_ context.Context, input *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tablesToItems.Get(*input.TableName); ok {
		return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: "ACTIVE"}}, nil
	}

	return &dynamodb.DescribeTableOutput{}, errors.New("table does not exist")
}

// Query queries a mock DynamoDB table.
func (m *MockClient) Query(_ context.Context, input *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	items, ok := m.tablesToItems.Get(*input.TableName)
	if !ok {
		return &dynamodb.QueryOutput{}, errors.New("table does not exist")
	}

	pattern := regexp.MustCompile("([A-z]+) = (:[A-z]+)")
	submatches := pattern.FindStringSubmatch(*input.KeyConditionExpression)

	matchingItems := []map[string]types.AttributeValue{}
	for _, item := range items {
		if m.isMatch(item, map[string]types.AttributeValue{submatches[1]: input.ExpressionAttributeValues[submatches[2]]}, cmp.AllowUnexported(types.AttributeValueMemberS{})) {
			matchingItems = append(matchingItems, item)
		}
	}

	if len(matchingItems) != 0 {
		slices.Reverse[[]map[string]types.AttributeValue](matchingItems)

		return &dynamodb.QueryOutput{Items: matchingItems}, nil
	}

	return &dynamodb.QueryOutput{}, nil
}

// isMatch checks if an item possesses a certain primary key.
func (m *MockClient) isMatch(item map[string]types.AttributeValue, key map[string]types.AttributeValue, opts ...cmp.Option) bool {
	if len(key) > len(item) {
		return false
	}

	for i, key := range key {
		if attributeName, found := item[i]; !found || !cmp.Equal(attributeName, key, opts...) {
			return false
		}
	}

	return true
}
