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
package dynamodbmock

import (
	"context"
	"errors"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/rivian/delta-go/internal/utils"
	"golang.org/x/exp/slices"
)

var (
	ErrorTableDoesNotExist               error = errors.New("table does not exist")
	ErrorConditionExpressionNotSatisfied error = errors.New("condition expression not satisfied")
)

type MockDynamoDBClient struct {
	utils.DynamoDBClient
	tables map[string][]map[string]types.AttributeValue
}

func New() *MockDynamoDBClient {
	m := new(MockDynamoDBClient)
	m.tables = make(map[string][]map[string]types.AttributeValue)
	return m
}

func (m *MockDynamoDBClient) GetTables() map[string][]map[string]types.AttributeValue {
	return m.tables
}

func (m *MockDynamoDBClient) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	for _, item := range m.tables[*input.TableName] {
		if utils.IsMapSubset[string, types.AttributeValue](item, input.Key) {
			return &dynamodb.GetItemOutput{Item: item}, nil
		}
	}

	return &dynamodb.GetItemOutput{}, nil
}

func (m *MockDynamoDBClient) PutItem(_ context.Context, input *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	matched, _ := regexp.MatchString("attribute_not_exists(([A-z]+))", *input.ConditionExpression)
	if matched {
		pattern := regexp.MustCompile("attribute_not_exists(([A-z]+))")
		subStrs := pattern.FindStringSubmatch(*input.ConditionExpression)
		_, err := m.GetItem(context.TODO(), &dynamodb.GetItemInput{Key: map[string]types.AttributeValue{subStrs[1]: input.Item[subStrs[1]]}})
		if err == nil {
			return &dynamodb.PutItemOutput{}, ErrorConditionExpressionNotSatisfied
		}
	}

	m.tables[*input.TableName] = append(m.tables[*input.TableName], input.Item)
	return &dynamodb.PutItemOutput{}, nil
}

func (m *MockDynamoDBClient) UpdateItem(_ context.Context, input *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

func (m *MockDynamoDBClient) DeleteItem(_ context.Context, input *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	var itemToDelete map[string]types.AttributeValue
	for _, item := range m.tables[*input.TableName] {
		if utils.IsMapSubset[string, types.AttributeValue](item, input.Key, cmp.AllowUnexported(types.AttributeValueMemberS{})) {
			itemToDelete = item
		}
	}

	posInSlice := slices.IndexFunc(m.tables[*input.TableName], func(v map[string]types.AttributeValue) bool {
		return cmp.Equal(v, itemToDelete, cmp.AllowUnexported(types.AttributeValueMemberS{}))
	})
	m.tables[*input.TableName] = slices.Delete(m.tables[*input.TableName], posInSlice, posInSlice+1)
	return &dynamodb.DeleteItemOutput{}, nil
}

func (m *MockDynamoDBClient) CreateTable(_ context.Context, input *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	m.tables[*input.TableName] = []map[string]types.AttributeValue{}
	return &dynamodb.CreateTableOutput{}, nil
}

func (m *MockDynamoDBClient) DescribeTable(_ context.Context, input *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	_, ok := m.tables[*input.TableName]
	if ok {
		return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: "ACTIVE"}}, nil
	}

	return &dynamodb.DescribeTableOutput{}, ErrorTableDoesNotExist
}

func (m *MockDynamoDBClient) Query(_ context.Context, input *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	pattern := regexp.MustCompile("([A-z]+) (:[A-z]+)")
	subStrs := pattern.FindStringSubmatch(*input.KeyConditionExpression)

	items := []map[string]types.AttributeValue{}
	for _, item := range m.tables[*input.TableName] {
		if utils.IsMapSubset[string, types.AttributeValue](item, map[string]types.AttributeValue{subStrs[1]: input.ExpressionAttributeValues[subStrs[2]]}) {
			items = append(items, item)
		}
	}
	slices.Reverse[[]map[string]types.AttributeValue](items)

	return &dynamodb.QueryOutput{Items: items}, nil
}