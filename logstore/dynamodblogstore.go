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
package logstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/rivian/delta-go/storage"
	log "github.com/sirupsen/logrus"
)

var (
	// Compile time check that DynamoDBLogStore implements KeyValueStore
	_                                     LogStore = (*DynamoDBLogStore)(nil)
	ErrorExceededTableCreateRetryAttempts error    = errors.New("failed to create table")
	ErrorUnableToGetExternalEntry         error    = errors.New("unable to get external entry")
)

const (
	// DynamoDB table attribute keys
	attrTablePath  string = "tablePath"
	attrFileName   string = "fileName"
	attrTempPath   string = "tempPath"
	attrComplete   string = "complete"
	attrExpireTime string = "expireTime"

	// The delay, in seconds, after an external entry has been committed to the delta log at which
	// point it is safe to be deleted from the external store.

	// We want a delay long enough such that, after the external entry has been deleted, another
	// write attempt for the SAME delta log commit can FAIL using ONLY the FileSystem's existence
	// check (e.g. `fs.exists(path)`). Recall we assume that the FileSystem does not provide mutual
	// exclusion.

	// We use a value of 1 day.

	// If we choose too small of a value, like 0 seconds, then the following scenario is possible:
	// - t0:  Writers W1 and W2 start writing data files
	// - t1:  W1 begins to try and write into the _delta_log.
	// - t2:  W1 checks if N.json exists in FileSystem. It doesn't.
	// - t3:  W1 writes actions into temp file T1(N)
	// - t4:  W1 writes to external store entry E1(N, complete=false)
	// - t5:  W1 copies (with overwrite=false) T1(N) into N.json.
	// - t6:  W1 overwrites entry in external store E1(N, complete=true, expireTime=now+0)
	// - t7:  E1 is safe to be deleted, and some external store TTL mechanism deletes E1
	// - t8:  W2 begins to try and write into the _delta_log.
	// - t9:  W1 checks if N.json exists in FileSystem, but too little time has transpired between
	//        t5 and t9 that the FileSystem check (fs.exists(path)) returns FALSE.
	//        Note: This isn't possible on S3 (which provides strong consistency) but could be
	//        possible on eventually-consistent systems.
	// - t10: W2 writes actions into temp file T2(N)
	// - t11: W2 writes to external store entry E2(N, complete=false)
	// - t12: W2 successfully copies (with overwrite=false) T2(N) into N.json. FileSystem didn't
	//        provide the necessary mutual exclusion, so the copy succeeded. Thus, DATA LOSS HAS
	//        OCCURRED.

	// By using an expiration delay of 1 day, we ensure one of the steps at t9 or t12 will fail.
	defaultExternalEntryExpirationDelaySeconds uint64 = 86400
	defaultMaxRetryTableCreateAttempts         uint16 = 20
)

// A concrete implementation of LogStore that uses an external DynamoDB table
// to provide the mutual exclusion during calls to `PutExternalEntry`.

// DynamoDB entries are of form
// - key
// -- tablePath (HASH, STRING)
// -- filename (RANGE, STRING)

// - attributes
// -- tempPath (STRING, relative to _delta_log)
// -- complete (STRING, representing boolean, "true" or "false")
// -- commitTime (NUMBER, epoch seconds)
type DynamoDBLogStore struct {
	client                      *dynamodb.Client
	tableName                   string
	expirationDelaySeconds      uint64
	maxRetryTableCreateAttempts uint16
}

type DynamoDBLogStoreOptions struct {
	Config                      aws.Config
	TableName                   string
	ExpirationDelaySeconds      uint64
	MaxRetryTableCreateAttempts uint16
}

func NewDynamoDBLogStore(lso DynamoDBLogStoreOptions) (*DynamoDBLogStore, error) {
	ls := new(DynamoDBLogStore)
	ls.tableName = lso.TableName

	if lso.ExpirationDelaySeconds != 0 {
		ls.expirationDelaySeconds = lso.ExpirationDelaySeconds
	} else {
		ls.expirationDelaySeconds = defaultExternalEntryExpirationDelaySeconds
	}

	if lso.MaxRetryTableCreateAttempts != 0 {
		ls.maxRetryTableCreateAttempts = lso.MaxRetryTableCreateAttempts
	} else {
		ls.maxRetryTableCreateAttempts = defaultMaxRetryTableCreateAttempts
	}

	log.Infof("delta-go: Using table name %s", ls.tableName)
	log.Infof("delta-go: Using TTL (seconds) %d", ls.expirationDelaySeconds)

	var err error
	ls.client, err = ls.getClient(lso.Config)
	if err != nil {
		log.Debugf("delta-go: Failed to get DynamoDB client. %v", err)
		return nil, err
	}
	ls.tryEnsureTableExists()

	return ls, nil
}

func (ls *DynamoDBLogStore) GetExpirationDelaySeconds() (uint64, error) {
	return ls.expirationDelaySeconds, nil
}

func (ls *DynamoDBLogStore) PutExternalEntry(entry *ExternalCommitEntry, overwrite bool) error {
	log.Debugf("delta-go: PutItem (tablePath %s, fileName %s, tempPath %s, complete %t, expireTime %d, overwrite %t)", entry.TablePath, entry.FileName, entry.TempPath, entry.Complete, entry.ExpireTime, overwrite)

	pir, err := ls.createPutItemRequest(entry, overwrite)
	if err != nil {
		log.Debugf("delta-go: Failed to create PutItem request. %v", err)
		return err
	}

	ls.client.PutItem(context.TODO(), pir)

	return err
}

func (ls *DynamoDBLogStore) GetExternalEntry(tablePath *storage.Path, fileName *storage.Path) (*ExternalCommitEntry, error) {
	attributes := map[string]types.AttributeValue{attrTablePath: &types.AttributeValueMemberS{Value: tablePath.Raw}, attrFileName: &types.AttributeValueMemberS{Value: fileName.Raw}}

	gii := dynamodb.GetItemInput{Key: attributes, TableName: aws.String(ls.tableName), ConsistentRead: aws.Bool(true)}
	gio, err := ls.client.GetItem(context.TODO(), &gii)
	if err != nil || gio.Item == nil {
		log.Debugf("delta-go: Failed GetItem. %v", err)
		return nil, errors.Join(err, ErrorUnableToGetExternalEntry)
	}

	ece, err := ls.dbResultToCommitEntry(gio.Item)
	if err != nil {
		log.Debugf("delta-go: Failed to map a DBB query result item to an ExternalCommitEntry. %v", err)
		return nil, err
	}

	return ece, err
}

func (ls *DynamoDBLogStore) GetLatestExternalEntry(tablePath *storage.Path) (*ExternalCommitEntry, error) {
	qi := dynamodb.QueryInput{TableName: &ls.tableName, ConsistentRead: aws.Bool(true), ScanIndexForward: aws.Bool(false), Limit: aws.Int32(1), ExpressionAttributeValues: map[string]types.AttributeValue{
		":partitionKey": &types.AttributeValueMemberS{Value: tablePath.Raw},
	}, KeyConditionExpression: aws.String(fmt.Sprintf("%s = :partitionKey", attrTablePath))}
	qo, err := ls.client.Query(context.TODO(), &qi)
	if err != nil {
		log.Debugf("delta-go: Failed Query. %v", err)
		return nil, err
	}

	ece, err := ls.dbResultToCommitEntry(qo.Items[0])
	if err != nil {
		log.Debugf("delta-go: Failed to map a DBB query result item to an ExternalCommitEntry. %v", err)
		return nil, err
	}

	return ece, nil
}

// Maps a DBB query result item to an ExternalCommitEntry.
func (ls *DynamoDBLogStore) dbResultToCommitEntry(item map[string]types.AttributeValue) (*ExternalCommitEntry, error) {
	var expireTimeAttr uint64
	var err error

	_, ok := item[attrExpireTime]
	if !ok {
		expireTimeAttr = 0
	} else {
		expireTimeAttr, err = strconv.ParseUint(item[attrExpireTime].(*types.AttributeValueMemberN).Value, 10, 64)
		if err != nil {
			log.Debugf("delta-go: Failed to interpet expire time attribute as uint64. %v", err)
			return nil, err
		}
	}

	return NewExternalCommitEntry(
		item[attrTablePath].(*types.AttributeValueMemberS).Value,
		item[attrFileName].(*types.AttributeValueMemberS).Value,
		item[attrTempPath].(*types.AttributeValueMemberS).Value,
		item[attrComplete].(*types.AttributeValueMemberS).Value == "true",
		expireTimeAttr,
	)
}

func (ls *DynamoDBLogStore) createPutItemRequest(entry *ExternalCommitEntry, overwrite bool) (*dynamodb.PutItemInput, error) {
	attributes := map[string]types.AttributeValue{attrTablePath: &types.AttributeValueMemberS{Value: entry.TablePath}, attrFileName: &types.AttributeValueMemberS{Value: entry.FileName}, attrTempPath: &types.AttributeValueMemberS{Value: entry.TempPath}, attrComplete: &types.AttributeValueMemberS{Value: *aws.String(strconv.FormatBool(entry.Complete))}}

	if entry.ExpireTime != 0 {
		attributes[attrExpireTime] = &types.AttributeValueMemberN{Value: *aws.String(fmt.Sprint(entry.ExpireTime))}
	} else {
		attributes[attrExpireTime] = &types.AttributeValueMemberN{Value: *aws.String(strconv.FormatUint(ls.expirationDelaySeconds, 10))}
	}

	pir := &dynamodb.PutItemInput{
		TableName: aws.String(ls.tableName),
		Item:      attributes}

	if !overwrite {
		pir.ConditionExpression = aws.String(fmt.Sprintf("attribute_not_exists(%s)", attrFileName))
	}

	return pir, nil
}

func (ls *DynamoDBLogStore) tryEnsureTableExists() error {
	var attemptNumber int = 0
	var created bool = false

	for {
		if attemptNumber >= int(ls.maxRetryTableCreateAttempts) {
			log.Debugf("delta-go: Table create attempt failed. Attempts exhausted beyond maxRetryDynamoDbTableCreateAttempts of %d so failing.", ls.maxRetryTableCreateAttempts)
			return ErrorExceededTableCreateRetryAttempts
		}

		var status string = "CREATING"

		result, err := ls.client.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(ls.tableName),
		})
		if err != nil {
			var rcu int64 = 5
			var wcu int64 = 5

			log.Infof("delta-go: DynamoDB table %s does not exist. Creating it now with provisioned throughput of %d and %d WCUs.", ls.tableName, rcu, wcu)
			_, err := ls.client.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
				AttributeDefinitions: []types.AttributeDefinition{
					{
						AttributeName: aws.String(attrTablePath),
						AttributeType: types.ScalarAttributeTypeS,
					},
					{
						AttributeName: aws.String(attrFileName),
						AttributeType: types.ScalarAttributeTypeS,
					},
				},
				KeySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String(attrTablePath),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String(attrFileName),
						KeyType:       types.KeyTypeRange,
					},
				},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(rcu),
					WriteCapacityUnits: aws.Int64(wcu),
				},
				TableName: aws.String(ls.tableName),
			})
			if err != nil {
				log.Debugf("delta-go: Table %s just created by concurrent process. %v", ls.tableName, err)
			}
		}

		status = string(result.Table.TableStatus)
		if status == "ACTIVE" {
			if created {
				log.Infof("delta-go: Successfully created DynamoDB table %s", ls.tableName)
			} else {
				log.Infof("delta-go: Table %s already exists", ls.tableName)
			}
		} else if status == "CREATING" {
			attemptNumber++
			log.Infof("delta-go: Waiting for %s table creation", ls.tableName)
			time.Sleep(1000 * time.Millisecond)
		} else {
			attemptNumber++
			log.Debugf("delta-go: Table %s status: %s. Incrementing attempt number to %d and retrying. %v", ls.tableName, status, attemptNumber, err)
			continue
		}

		return nil
	}
}

func (ls *DynamoDBLogStore) getClient(config aws.Config) (*dynamodb.Client, error) {
	return dynamodb.NewFromConfig(config), nil
}
