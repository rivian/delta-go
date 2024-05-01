# Delta Go

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A native implementation of the [Delta](https://delta.io/) protocol in go.
This library started as a port of [delta-rs](https://github.com/delta-io/delta-rs/tree/main/rust).

This project is in alpha and the API is under development.

Current implementation is designed for highly concurrent writes; reads are not yet supported.

Our use case is to ingest data, write it to the Delta table folder as a Parquet file using a Parquet go library,
and then add the Parquet file to the Delta table using delta-go.


## Features

### Cloud Integrations

| Storage              |         Status        | Comment                                                          |
| -------------------- | :-------------------: | ---------------------------------------------------------------- |
| Local                |        ![done]        |                                                                  |
| S3 - AWS             |        ![done]        | Requires lock for concurrent writes                              |


### Supported Operations

| Operation             |         Status        | Description                                 |
| --------------------- | :-------------------: | ------------------------------------------- |
| Create                |        ![done]        | Create a new table                          |
| Append                |        ![done]        | Append data in a Parquet file to a table    |
| Checkpoint            |        ![done]        | Create a V1 checkpoint for a table. Note that the optional log cleanup has not been fully tested.          |


### Protocol Support Level

| Writer Version | Requirement                                   |              Status               |
| -------------- | --------------------------------------------- | :-------------------------------: |
| Version 2      | Append Only Tables                            |              ![done]              |
| Version 2      | Column Invariants                             |                                   |
| Version 3      | Enforce `delta.checkpoint.writeStatsAsJson`   |                                   |
| Version 3      | Enforce `delta.checkpoint.writeStatsAsStruct` |                                   |
| Version 3      | CHECK constraints                             |                                   |
| Version 4      | Change Data Feed                              |                                   |
| Version 4      | Generated Columns                             |                                   |
| Version 5      | Column Mapping                                |                                   |
| Version 6      | Identity Columns                              |                                   |
| Version 7      | Table Features                                |                                   |

| Reader Version | Requirement                         | Status |
| -------------- | ----------------------------------- | ------ |
| Version 2      | Column Mapping                      |        |
| Version 3      | Table Features (requires reader V7) |        |

## Usage

Create a table in S3.  This table is configured to use DynamoDB LogStore locking to enable multi-cluster S3 support.
```golang
	store, err := s3store.New(s3Client, baseURI)
	logStore, err := dynamodblogstore.New(dynamodblogstore.Options{Client: dynamoDBClient, TableName: deltaLogStoreTableName})
	table := delta.NewTableWithLogStore(store, nillock.New(), logStore)
	metadata := delta.NewTableMetaData("Test Table", "test description", new(delta.Format).Default(), schema, []string{}, make(map[string]string))
	err := table.Create(*metadata, new(delta.Protocol).Default(), delta.CommitInfo{}, []delta.Add{})
```

Append data to the table.  The data is in a parquet file located at `parquetRelativePath`; the path is relative to the `baseURI`.
```golang
	add, _, err := delta.NewAdd(store, storage.NewPath(parquetRelativePath), make(map[string]string))
	transaction := table.CreateTransaction(delta.NewTransactionOptions())
	transaction.AddActions([]deltalib.Action{add})
	operation := delta.Write{Mode: delta.Append}
	appMetaData := make(map[string]any)
	appMetaData["isBlindAppend"] = true
	transaction.SetAppMetadata(appMetaData)
	transaction.SetOperation(operation)
	v, err := transaction.CommitLogStore()
```

There are also some simple examples available in the `examples/` folder.

## Storage configuration on S3

If delta-go and other client(s) are being used to write to the same Delta table on S3, then it is important to configure all clients to use [multi-cluster LogStore](https://docs.delta.io/latest/delta-storage.html#-delta-storage-s3-multi-cluster) to avoid write conflicts.




[open]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueNeutral.svg
[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg
