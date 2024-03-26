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

// Package dynamodblogstore contains the resources required to create a DynamoDB log store.
package dynamodblogstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/rivian/delta-go/internal/dynamodbutils"
	"github.com/rivian/delta-go/logstore"
	"github.com/rivian/delta-go/storage"
	log "github.com/sirupsen/logrus"
)

// attribute represents attribute names in DynamoDB items.
type attribute string

const (
	// TablePath is the DynamoDB table attribute representing table path.
	TablePath attribute = "tablePath"
	// FileName is the DynamoDB table attribute representing file name.
	FileName attribute = "fileName"
	// TempPath is the DynamoDB table attribute representing temp path.
	TempPath attribute = "tempPath"
	// Complete is the DynamoDB table attribute representing transaction completeness.
	Complete attribute = "complete"
	// ExpireTime is the DynamoDB table attribute representing expire time.
	ExpireTime attribute = "expireTime"

	// The delay, in seconds, after a commit entry has been committed to a Delta log at which
	// point it is safe to be deleted from a log store.

	// We want a delay long enough such that, after a commit entry has been deleted, another
	// write attempt for the SAME Delta log commit can FAIL using ONLY the file system's existence
	// check. Recall we assume that the file system does not provide mutual exclusion.

	// We use a value of 1 day.

	// If we choose too small of a value, like 0 seconds, then the following scenario is possible:
	// - t0:  Writers W1 and W2 start writing data files.
	// - t1:  W1 begins to try and write to the Delta log.
	// - t2:  W1 checks if N.json exists in file system. It doesn't.
	// - t3:  W1 writes actions into temp file T1(N).
	// - t4:  W1 puts uncompleted commit entry E1(N).
	// - t5:  W1 copies, with overwriting disabled, T1(N) into N.json.
	// - t6:  W1 overwrites and completes commit entry E1(N), setting the expiration time to the
	//        current time.
	// - t7:  E1 is safe to be deleted, and some log store TTL mechanism deletes E1.
	// - t8:  W2 begins to try and write to the Delta log.
	// - t9:  W1 checks if N.json exists in the file system, but too little time has transpired
	//        between t5 and t9 that the file system existence check returns FALSE.
	//        Note: This isn't possible on S3 (which provides strong consistency) but could be
	//        possible on eventually-consistent systems.
	// - t10: W2 writes actions into temp file T2(N).
	// - t11: W2 puts uncompleted commit entry E2(N).
	// - t12: W2 successfully copies, with overwriting disabled, T2(N) into N.json. The file system
	//        didn't provide the ncessary mutual exclusion, so the copy succeeded. Thus, DATA LOSS
	//        HAS OCCURRED.

	// By using an expiration delay of 1 day, we ensure one of the steps at t9 or t12 will fail.
	defaultEntryExpirationDelaySeconds uint64 = 24 * 60 * 60
	defaultMaxTableCreateAttempts      uint16 = 20
	defaultRCU                         int64  = 5
	defaultWCU                         int64  = 5
)

// LogStore uses a DynamoDB table to provide mutual exclusion during calls to `Put`.
//
// DynamoDB entries are of the form
// - key
// -- tablePath (HASH, STRING)
// -- fileName (RANGE, STRING)
//
// - attributes
// -- tempPath (STRING, relative to the Delta log)
// -- complete (STRING, representing boolean, "true" or "false")
// -- expireTime (NUMBER, epoch seconds)
type LogStore struct {
	client                 dynamodbutils.Client
	tableName              string
	expirationDelaySeconds uint64
	maxTableCreateAttempts uint16
	rcu                    int64
	wcu                    int64
}

// Compile time check that LogStore implements logstore.LogStore
var _ logstore.LogStore = (*LogStore)(nil)

// Options contains settings that can be adjusted to change the behavior of LogStore.
type Options struct {
	Client                 dynamodbutils.Client
	TableName              string
	ExpirationDelaySeconds uint64
	MaxTableCreateAttempts uint16
	// The number of read capacity units which can be consumed per second (https://aws.amazon.com/dynamodb/pricing/provisioned/).
	RCU int64
	// The number of write capacity units which can be consumed per second (https://aws.amazon.com/dynamodb/pricing/provisioned/).
	WCU int64
}

// Client gets the DynamoDB client.
func (ls LogStore) Client() any {
	return ls.client
}

// TableName gets the DynamoDB table name.
func (ls LogStore) TableName() string {
	return ls.tableName
}

// ExpirationDelaySeconds gets the number of seconds until a commit entry expires.
func (ls LogStore) ExpirationDelaySeconds() uint64 {
	return ls.expirationDelaySeconds
}

// MaxRetryTableCreateAttempts gets the maximum number of table creation attempts.
func (ls LogStore) MaxRetryTableCreateAttempts() uint16 {
	return ls.maxTableCreateAttempts
}

// New creates a new LogStore instance.
func New(o Options) (*LogStore, error) {
	ls := new(LogStore)
	ls.tableName = o.TableName

	if o.ExpirationDelaySeconds != 0 {
		ls.expirationDelaySeconds = o.ExpirationDelaySeconds
	} else {
		ls.expirationDelaySeconds = defaultEntryExpirationDelaySeconds
	}

	if o.MaxTableCreateAttempts != 0 {
		ls.maxTableCreateAttempts = o.MaxTableCreateAttempts
	} else {
		ls.maxTableCreateAttempts = defaultMaxTableCreateAttempts
	}

	if o.RCU != 0 {
		ls.rcu = o.RCU
	} else {
		ls.rcu = defaultRCU
	}

	if o.WCU != 0 {
		ls.wcu = o.WCU
	} else {
		ls.wcu = defaultWCU
	}

	ls.client = o.Client

	log.Debugf("delta-go: Using table name %s", ls.tableName)
	log.Debugf("delta-go: Using TTL (seconds) %d", ls.expirationDelaySeconds)

	cti := dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(string(TablePath)),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(string(FileName)),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(string(TablePath)),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(string(FileName)),
				KeyType:       types.KeyTypeRange,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(ls.rcu),
			WriteCapacityUnits: aws.Int64(ls.wcu),
		},
		TableName: aws.String(ls.tableName),
	}
	if err := dynamodbutils.CreateTableIfNotExists(ls.client, ls.tableName, cti, ls.maxTableCreateAttempts); err != nil {
		return nil, errors.Join(dynamodbutils.ErrFailedToCreateTable, err)
	}

	return ls, nil
}

// Put puts a commit entry into a log store in an exclusive way.
func (ls *LogStore) Put(entry *logstore.CommitEntry, overwrite bool) error {
	log.Debugf("delta-go: PutItem (tablePath %s, fileName %s, tempPath %s, complete %t, expireTime %d, overwrite %t)",
		entry.TablePath(), entry.FileName(), entry.TempPath(), entry.IsComplete(), entry.ExpirationTime(), overwrite)

	pir, err := ls.createPutItemRequest(entry, overwrite)
	if err != nil {
		return errors.Join(errors.New("failed to create put item request"), err)
	}

	if _, err := ls.client.PutItem(context.TODO(), pir); err != nil {
		return errors.Join(errors.New("failed to put item"), err)
	}

	return nil
}

// Get gets a commit entry corresponding to the commit log identified by the given table path and file name.
func (ls *LogStore) Get(tablePath storage.Path, fileName storage.Path) (*logstore.CommitEntry, error) {
	attributes := map[string]types.AttributeValue{string(TablePath): &types.AttributeValueMemberS{Value: tablePath.Raw}, string(FileName): &types.AttributeValueMemberS{Value: fileName.Raw}}

	gii := dynamodb.GetItemInput{Key: attributes, TableName: aws.String(ls.tableName), ConsistentRead: aws.Bool(true)}
	gio, err := ls.client.GetItem(context.TODO(), &gii)
	if err != nil || gio.Item == nil {
		return nil, errors.Join(errors.New("failed to get item"), err)
	}

	ce, err := ls.mapItemToEntry(gio.Item)
	if err != nil {
		return nil, errors.Join(errors.New("failed to map item to entry"), err)
	}

	return ce, err
}

// Latest gets the commit entry corresponding to the latest commit log for a given table path.
func (ls *LogStore) Latest(tablePath storage.Path) (*logstore.CommitEntry, error) {
	qi := dynamodb.QueryInput{TableName: &ls.tableName, ConsistentRead: aws.Bool(true), ScanIndexForward: aws.Bool(false), Limit: aws.Int32(1),
		ExpressionAttributeValues: map[string]types.AttributeValue{":partitionKey": &types.AttributeValueMemberS{Value: tablePath.Raw}},
		KeyConditionExpression:    aws.String(fmt.Sprintf("%s = :partitionKey", TablePath))}
	qo, err := ls.client.Query(context.TODO(), &qi)
	if err != nil {
		return nil, errors.Join(errors.New("failed to query"), err)
	}
	if len(qo.Items) == 0 {
		return nil, logstore.ErrLatestDoesNotExist
	}

	ce, err := ls.mapItemToEntry(qo.Items[0])
	if err != nil {
		return nil, errors.Join(errors.New("failed to map item to entry"), err)
	}

	return ce, nil
}

// mapItemToEntry maps an item to a commit entry.
func (ls *LogStore) mapItemToEntry(item map[string]types.AttributeValue) (*logstore.CommitEntry, error) {
	var time uint64
	var err error

	if _, ok := item[string(ExpireTime)]; !ok {
		time = 0
	} else {
		time, err = strconv.ParseUint(item[string(ExpireTime)].(*types.AttributeValueMemberN).Value, 10, 64)
		if err != nil {
			return nil, errors.Join(errors.New("failed to interpret expire time as uint64"), err)
		}
	}

	return logstore.New(
		storage.NewPath(item[string(TablePath)].(*types.AttributeValueMemberS).Value),
		storage.NewPath(item[string(FileName)].(*types.AttributeValueMemberS).Value),
		storage.NewPath(item[string(TempPath)].(*types.AttributeValueMemberS).Value),
		item[string(Complete)].(*types.AttributeValueMemberS).Value == "true",
		time,
	), nil
}

// createPutItemRequest creates a put item request.
func (ls *LogStore) createPutItemRequest(entry *logstore.CommitEntry, overwrite bool) (*dynamodb.PutItemInput, error) {
	attributes := map[string]types.AttributeValue{string(TablePath): &types.AttributeValueMemberS{Value: entry.TablePath().Raw},
		string(FileName): &types.AttributeValueMemberS{Value: entry.FileName().Raw}, string(TempPath): &types.AttributeValueMemberS{Value: entry.TempPath().Raw},
		string(Complete): &types.AttributeValueMemberS{Value: *aws.String(strconv.FormatBool(entry.IsComplete()))}}

	if entry.ExpirationTime() != 0 {
		attributes[string(ExpireTime)] = &types.AttributeValueMemberN{Value: *aws.String(fmt.Sprint(entry.ExpirationTime()))}
	}

	pir := &dynamodb.PutItemInput{
		TableName: aws.String(ls.tableName),
		Item:      attributes}

	if !overwrite {
		pir.ConditionExpression = aws.String(fmt.Sprintf("attribute_not_exists(%s)", string(FileName)))
	}

	return pir, nil
}
