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
package utils

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/go-cmp/cmp"
	log "github.com/sirupsen/logrus"
)

var (
	ErrorExceededTableCreateRetryAttempts error = errors.New("failed to create table")
)

type DynamoDBClient interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

func TryEnsureDynamoDbTableExists(client DynamoDBClient, tableName string, createTableInput dynamodb.CreateTableInput, maxRetryTableCreateAttempts uint16) error {
	attemptNumber := 0
	created := false

	for {
		if attemptNumber >= int(maxRetryTableCreateAttempts) {
			log.Debugf("delta-go: Table create attempt failed. Attempts exhausted beyond maxRetryDynamoDbTableCreateAttempts of %d so failing.", maxRetryTableCreateAttempts)
			return ErrorExceededTableCreateRetryAttempts
		}

		status := "CREATING"

		result, err := client.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			log.Infof("delta-go: DynamoDB table %s does not exist. Creating it now with provisioned throughput of %d and %d WCUs.", tableName, *createTableInput.ProvisionedThroughput.ReadCapacityUnits, *createTableInput.ProvisionedThroughput.ReadCapacityUnits)
			_, err := client.CreateTable(context.TODO(), &createTableInput)
			if err != nil {
				log.Debugf("delta-go: Table %s just created by concurrent process. %v", tableName, err)
			}

			created = true
		}

		if result == nil || result.Table == nil {
			attemptNumber++
			log.Infof("delta-go: Waiting for %s table creation", tableName)
			time.Sleep(1 * time.Second)
			continue
		} else {
			status = string(result.Table.TableStatus)
		}

		if status == "ACTIVE" {
			if created {
				log.Infof("delta-go: Successfully created DynamoDB table %s", tableName)
			} else {
				log.Infof("delta-go: Table %s already exists", tableName)
			}
		} else if status == "CREATING" {
			attemptNumber++
			log.Infof("delta-go: Waiting for %s table creation", tableName)
			time.Sleep(1 * time.Second)
		} else {
			attemptNumber++
			log.Debugf("delta-go: Table %s status: %s. Incrementing attempt number to %d and retrying. %v", tableName, status, attemptNumber, err)
			continue
		}

		return nil
	}
}

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