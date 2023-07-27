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
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/rivian/delta-go"
	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/s3store"
	log "github.com/sirupsen/logrus"
)

// Compile time check that S3ObjectStore implements storage.ObjectStore
var _ LogStore = (*DynamoDBLogStore)(nil)

const (
	// WARNING: setting this value too low can cause data loss. Defaults to a duration of 1 day.
	ttlSeconds string = "ddb.ttl"

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
)

// A concrete implementation of LogStore that uses an external DynamoDB table
// to provide the mutual exclusion during calls to `putExternalEntry`.

// DynamoDB entries are of form
// - key
// -- tablePath (HASH, STRING)
// -- filename (RANGE, STRING)

// - attributes
// -- tempPath (STRING, relative to _delta_log)
// -- complete (STRING, representing boolean, "true" or "false")
// -- commitTime (NUMBER, epoch seconds)
type DynamoDBLogStore struct {
	ObjectStore            *s3store.S3ObjectStore
	client                 *dynamodb.Client
	tableName              string
	expirationDelaySeconds uint64
}

type DynamoDBLogStoreOptions struct {
	cfg                    aws.Config
	tableName              string
	expirationDelaySeconds uint64
}

func NewDynamoDBLogStore(lso DynamoDBLogStoreOptions) (*DynamoDBLogStore, error) {
	ls := new(DynamoDBLogStore)
	ls.tableName = lso.tableName

	if lso.expirationDelaySeconds != 0 {
		ls.expirationDelaySeconds = lso.expirationDelaySeconds
	} else {
		ls.expirationDelaySeconds = defaultExternalEntryExpirationDelaySeconds
	}

	log.Infof("delta-go: Using table name %s", ls.tableName)
	log.Infof("delta-go: Using TTL (seconds) %d", ls.expirationDelaySeconds)

	var err error
	ls.client, err = ls.getClient(lso.cfg)
	if err != nil {
		log.Errorf("delta-go: Failed to get DynamoDB client. %v", err)
	}
	ls.tryEnsureTableExists()

	return ls, nil
}

func (ls *DynamoDBLogStore) Read(path *storage.Path) error {
	return nil
}

// If overwrite=true, then write normally without any interaction with external store.
// Else, to commit for delta version N:
// - Step 0: Fail if N.json already exists in FileSystem.
// - Step 1: Ensure that N-1.json exists. If not, perform a recovery.
// - Step 2: PREPARE the commit.
//   - Write `actions` into temp file T(N)
//   - Write with mutual exclusion to external store and entry E(N, T(N), complete=false)
//
// - Step 3: COMMIT the commit to the delta log.
//   - Copy T(N) into N.json
//
// - Step 4: ACKNOWLEDGE the commit.
//   - Overwrite entry E in external store and set complete=true
func (ls *DynamoDBLogStore) Write(path *storage.Path, actions []delta.Action, overwrite bool) error {
	return nil
}

func (ls *DynamoDBLogStore) ListFrom(path *storage.Path) error {
	return nil
}

func (ls *DynamoDBLogStore) getExpirationDelaySeconds() (uint64, error) {
	return ls.expirationDelaySeconds, nil
}

func (ls *DynamoDBLogStore) putExternalEntry(entry ExternalCommitEntry, overwrite bool) error {
	log.Debugf("delta-go: PutItem (tablePath %s, fileName %s, tempPath %s, complete %t, expireTime %d, overwrite %t)", entry.tablePath, entry.fileName, entry.tempPath, entry.complete, entry.expireTime, overwrite)

	pir, err := ls.createPutItemRequest(entry, overwrite)
	if err != nil {
		log.Errorf("delta-go: Failed to create PutItem request. %v", err)
	}

	ls.client.PutItem(context.TODO(), pir)

	return err
}

func (ls *DynamoDBLogStore) getExternalEntry(tablePath string, fileName string) (*ExternalCommitEntry, error) {
	attributes := map[string]types.AttributeValue{attrTablePath: &types.AttributeValueMemberS{Value: tablePath}, attrFileName: &types.AttributeValueMemberS{Value: fileName}}

	gii := dynamodb.GetItemInput{Key: attributes, TableName: aws.String(ls.tableName), ConsistentRead: aws.Bool(true)}
	gio, err := ls.client.GetItem(context.TODO(), &gii)
	if err != nil {
		log.Errorf("delta-go: Failed GetItem. %v", err)
		return nil, err
	}

	ece, err := ls.dbResultToCommitEntry(gio.Item)
	if err != nil {
		log.Errorf("delta-go: Failed to map a DBB query result item to an ExternalCommitEntry. %v", err)
	}

	return ece, err
}

func (ls *DynamoDBLogStore) getLatestExternalEntry(tablePath string) (*ExternalCommitEntry, error) {
	qi := dynamodb.QueryInput{ConsistentRead: aws.Bool(true), ScanIndexForward: aws.Bool(false), Limit: aws.Int32(1), KeyConditionExpression: aws.String(fmt.Sprintf("%s = :%s", attrTablePath, tablePath))}
	qo, err := ls.client.Query(context.TODO(), &qi)
	if err != nil {
		log.Errorf("delta-go: Failed Query. %v", err)
		return nil, err
	}

	ece, err := ls.dbResultToCommitEntry(qo.Items[0])
	if err != nil {
		log.Errorf("delta-go: Failed to map a DBB query result item to an ExternalCommitEntry. %v", err)
	}

	return ece, nil
}

// Map a DBB query result item to an ExternalCommitEntry
func (ls *DynamoDBLogStore) dbResultToCommitEntry(item map[string]types.AttributeValue) (*ExternalCommitEntry, error) {
	expireTimeAttr, err := strconv.ParseUint(item[attrExpireTime].(*types.AttributeValueMemberN).Value, 10, 64)
	if err != nil {
		log.Errorf("delta-go: Failed to interpet expire time attribute as uint64. %v", err)
	}

	return NewExternalCommitEntry(
		item[attrTablePath].(*types.AttributeValueMemberS).Value,
		item[attrFileName].(*types.AttributeValueMemberS).Value,
		item[attrTempPath].(*types.AttributeValueMemberS).Value,
		item[attrComplete].(*types.AttributeValueMemberS).Value == "true",
		expireTimeAttr,
	)
}

func (ls *DynamoDBLogStore) createPutItemRequest(entry ExternalCommitEntry, overwrite bool) (*dynamodb.PutItemInput, error) {
	attributes := map[string]types.AttributeValue{attrTablePath: &types.AttributeValueMemberS{Value: entry.tablePath}, attrFileName: &types.AttributeValueMemberS{Value: entry.fileName}, attrTempPath: &types.AttributeValueMemberS{Value: entry.tempPath}, attrComplete: &types.AttributeValueMemberS{Value: *aws.String(strconv.FormatBool(entry.complete))}}

	if entry.expireTime != 0 {
		attributes[attrExpireTime] = &types.AttributeValueMemberN{Value: *aws.String(string(entry.expireTime))}
	}

	pir := &dynamodb.PutItemInput{
		TableName: aws.String(ls.tableName),
		Item:      attributes}

	if !overwrite {
		pir.ConditionExpression = aws.String(fmt.Sprintf("%s = false", attrFileName))
	}

	return pir, nil
}

func (ls *DynamoDBLogStore) tryEnsureTableExists() error {
	var retries int = 0
	var created bool = false

	for retries < 20 {
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
				log.Errorf("delta-go: Table %s just created by concurrent process. %v", ls.tableName, err)
			}
		}

		status = string(result.Table.TableStatus)
		if status == "ACTIVE" {
			if created {
				log.Infof("delta-go: Successfully created DynamoDB table %s", ls.tableName)
			} else {
				log.Infof("delta-go: Table %s already exists", ls.tableName)
			}
			break
		} else if status == "CREATING" {
			retries += 1
			log.Infof("delta-go: Waiting for %s table creation", ls.tableName)
			time.Sleep(1000 * time.Millisecond)
		} else {
			log.Errorf("delta-go: Table %s status: %s. %v", ls.tableName, status, err)
			break
		}
	}

	return nil
}

func (ls *DynamoDBLogStore) getClient(cfg aws.Config) (*dynamodb.Client, error) {
	return dynamodb.NewFromConfig(cfg), nil
}
