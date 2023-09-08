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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/rivian/delta-go/internal/utils"
	"github.com/rivian/delta-go/storage"
	log "github.com/sirupsen/logrus"
)

var (
	// Compile time check that DynamoDBLogStore implements LogStore
	_                             LogStore = (*DynamoDBLogStore)(nil)
	ErrorUnableToGetExternalEntry error    = errors.New("unable to get external entry")
)

const (
	// DynamoDB table attribute keys
	AttrTablePath  string = "tablePath"
	AttrFileName   string = "fileName"
	AttrTempPath   string = "tempPath"
	AttrComplete   string = "complete"
	AttrExpireTime string = "expireTime"

	// The delay, in seconds, after an external entry has been committed to the delta log at which
	// point it is safe to be deleted from the external store.

	// We want a delay long enough such that, after the external entry has been deleted, another
	// write attempt for the SAME delta log commit can FAIL using ONLY the file system's existence
	// check (e.g. `Stat(fs, path)`). Recall we assume that the file system does not provide mutual
	// exclusion.

	// We use a value of 1 day.

	// If we choose too small of a value, like 0 seconds, then the following scenario is possible:
	// - t0:  Writers W1 and W2 start writing data files
	// - t1:  W1 begins to try and write into the `_delta_log`
	// - t2:  W1 checks if N.json exists in file system. It doesn't.
	// - t3:  W1 writes actions into temp file T1(N)
	// - t4:  W1 writes to external store entry E1(N, complete=false)
	// - t5:  W1 copies (with overwrite=false) T1(N) into N.json
	// - t6:  W1 overwrites entry in external store E1(N, complete=true, expireTime=now+0)
	// - t7:  E1 is safe to be deleted, and some external store TTL mechanism deletes E1
	// - t8:  W2 begins to try and write into the `_delta_log`
	// - t9:  W1 checks if N.json exists in file system, but too little time has transpired between
	//        t5 and t9 that the file system check (fs.exists(path)) returns FALSE.
	//        Note: This isn't possible on S3 (which provides strong consistency) but could be
	//        possible on eventually-consistent systems.
	// - t10: W2 writes actions into temp file T2(N)
	// - t11: W2 writes to external store entry E2(N, complete=false)
	// - t12: W2 successfully copies (with overwrite=false) T2(N) into N.json. File system didn't
	//        provide the necessary mutual exclusion, so the copy succeeded. Thus, DATA LOSS HAS
	//        OCCURRED.

	// By using an expiration delay of 1 day, we ensure one of the steps at t9 or t12 will fail.
	DefaultExternalEntryExpirationDelaySeconds uint64 = 24 * 60 * 60
	DefaultMaxRetryTableCreateAttempts         uint16 = 20
	DefaultRCU                                 int64  = 5
	DefaultWCU                                 int64  = 5
)

// A concrete implementation of LogStore that uses an external DynamoDB table
// to provide the mutual exclusion during calls to `PutExternalEntry`.

// DynamoDB entries are of form
// - key
// -- tablePath (HASH, STRING)
// -- fileName (RANGE, STRING)

// - attributes
// -- tempPath (STRING, relative to `_delta_log`)
// -- complete (STRING, representing boolean, "true" or "false")
// -- expireTime (NUMBER, epoch seconds)
type DynamoDBLogStore struct {
	client                      utils.DynamoDBClient
	tableName                   string
	expirationDelaySeconds      uint64
	maxRetryTableCreateAttempts uint16
	rcu                         int64
	wcu                         int64
}

type DynamoDBLogStoreOptions struct {
	Config                      aws.Config
	Client                      utils.DynamoDBClient
	TableName                   string
	ExpirationDelaySeconds      uint64
	MaxRetryTableCreateAttempts uint16
	RCU                         int64
	WCU                         int64
}

func (ls DynamoDBLogStore) GetClient() utils.DynamoDBClient {
	return ls.client
}

func (ls DynamoDBLogStore) GetTableName() string {
	return ls.tableName
}

func (ls DynamoDBLogStore) GetExpirationDelaySeconds() uint64 {
	return ls.expirationDelaySeconds
}

func (ls DynamoDBLogStore) GetMaxRetryTableCreateAttempts() uint16 {
	return ls.maxRetryTableCreateAttempts
}

func NewDynamoDBLogStore(lso DynamoDBLogStoreOptions) (*DynamoDBLogStore, error) {
	ls := new(DynamoDBLogStore)
	ls.tableName = lso.TableName

	if lso.ExpirationDelaySeconds != 0 {
		ls.expirationDelaySeconds = lso.ExpirationDelaySeconds
	} else {
		ls.expirationDelaySeconds = DefaultExternalEntryExpirationDelaySeconds
	}

	if lso.MaxRetryTableCreateAttempts != 0 {
		ls.maxRetryTableCreateAttempts = lso.MaxRetryTableCreateAttempts
	} else {
		ls.maxRetryTableCreateAttempts = DefaultMaxRetryTableCreateAttempts
	}

	if lso.RCU != 0 {
		ls.rcu = lso.RCU
	} else {
		ls.rcu = DefaultRCU
	}

	if lso.WCU != 0 {
		ls.wcu = lso.WCU
	} else {
		ls.wcu = DefaultWCU
	}

	log.Infof("delta-go: Using table name %s", ls.tableName)
	log.Infof("delta-go: Using TTL (seconds) %d", ls.expirationDelaySeconds)

	var err error
	if lso.Client == nil {
		ls.client, err = ls.getClient(lso.Config)
		if err != nil {
			log.Debugf("delta-go: Failed to get DynamoDB client. %v", err)
			return nil, err
		}
	} else {
		ls.client = lso.Client
	}

	createTableInput := dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(AttrTablePath),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(AttrFileName),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(AttrTablePath),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(AttrFileName),
				KeyType:       types.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(ls.rcu),
			WriteCapacityUnits: aws.Int64(ls.wcu),
		},
		TableName: aws.String(ls.tableName),
	}
	utils.TryEnsureDynamoDBTableExists(ls.client, ls.tableName, createTableInput, ls.maxRetryTableCreateAttempts)

	return ls, nil
}

func (ls *DynamoDBLogStore) PutExternalEntry(entry *ExternalCommitEntry, overwrite bool) error {
	log.Debugf("delta-go: PutItem (tablePath %s, fileName %s, tempPath %s, complete %t, expireTime %d, overwrite %t)", entry.TablePath, entry.FileName, entry.TempPath, entry.Complete, entry.ExpireTime, overwrite)

	pir, err := ls.createPutItemRequest(entry, overwrite)
	if err != nil {
		log.Debugf("delta-go: Failed to create PutItem request. %v", err)
		return err
	}

	_, err = ls.client.PutItem(context.TODO(), pir)

	return err
}

func (ls *DynamoDBLogStore) GetExternalEntry(tablePath *storage.Path, fileName *storage.Path) (*ExternalCommitEntry, error) {
	attributes := map[string]types.AttributeValue{AttrTablePath: &types.AttributeValueMemberS{Value: tablePath.Raw}, AttrFileName: &types.AttributeValueMemberS{Value: fileName.Raw}}

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
	}, KeyConditionExpression: aws.String(fmt.Sprintf("%s = :partitionKey", AttrTablePath))}
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

// Maps a DBB query result item to an ExternalCommitEntry
func (ls *DynamoDBLogStore) dbResultToCommitEntry(item map[string]types.AttributeValue) (*ExternalCommitEntry, error) {
	var expireTimeAttr uint64
	var err error

	_, ok := item[AttrExpireTime]
	if !ok {
		expireTimeAttr = 0
	} else {
		expireTimeAttr, err = strconv.ParseUint(item[AttrExpireTime].(*types.AttributeValueMemberN).Value, 10, 64)
		if err != nil {
			log.Debugf("delta-go: Failed to interpet expire time attribute as uint64. %v", err)
			return nil, err
		}
	}

	return NewExternalCommitEntry(
		*storage.NewPath(item[AttrTablePath].(*types.AttributeValueMemberS).Value),
		*storage.NewPath(item[AttrFileName].(*types.AttributeValueMemberS).Value),
		*storage.NewPath(item[AttrTempPath].(*types.AttributeValueMemberS).Value),
		item[AttrComplete].(*types.AttributeValueMemberS).Value == "true",
		expireTimeAttr,
	)
}

func (ls *DynamoDBLogStore) createPutItemRequest(entry *ExternalCommitEntry, overwrite bool) (*dynamodb.PutItemInput, error) {
	attributes := map[string]types.AttributeValue{AttrTablePath: &types.AttributeValueMemberS{Value: entry.TablePath.Raw}, AttrFileName: &types.AttributeValueMemberS{Value: entry.FileName.Raw}, AttrTempPath: &types.AttributeValueMemberS{Value: entry.TempPath.Raw}, AttrComplete: &types.AttributeValueMemberS{Value: *aws.String(strconv.FormatBool(entry.Complete))}}

	if entry.ExpireTime != 0 {
		attributes[AttrExpireTime] = &types.AttributeValueMemberN{Value: *aws.String(fmt.Sprint(entry.ExpireTime))}
	}

	pir := &dynamodb.PutItemInput{
		TableName: aws.String(ls.tableName),
		Item:      attributes}

	if !overwrite {
		pir.ConditionExpression = aws.String(fmt.Sprintf("attribute_not_exists(%s)", AttrFileName))
	}

	return pir, nil
}

func (ls *DynamoDBLogStore) getClient(config aws.Config) (utils.DynamoDBClient, error) {
	return dynamodb.NewFromConfig(config), nil
}
