# Delta Go

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Delta[https://delta.io/] native implementation of the delta protocol in go.
This library started as a port of delta-rs[https://github.com/delta-io/delta-rs/tree/main/rust]
This project is in alpha and the api is under development.

Current implementation is designed for highly concurrent writes, reads are not yet supported.


Usage
---


Create a Table with object store, state store and lock
```golang
	store := filestore.New(tmpPath)
	state := filestate.New(tmpPath, "_delta_log/_commit.state")
	lock := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{})
	checkpointLock := filelock.New(tmpPath, "_delta_log/_checkpoint.lock", filelock.Options{})
	table := delta.NewTable(store, lock, state)
	metadata := delta.NewTableMetaData("Test Table", "test description", new(delta.Format).Default(), getSchema(), []string{}, make(map[string]string))
	err := table.Create(*metadata, new(delta.Protocol).Default(), delta.CommitInfo{}, []delta.Add{})
```

Commit an Add transaction to the `_delta_log/`
```golang
	add := delta.Add{
		Path:             fileName,
		Size:             p.Size,
		DataChange:       true,
		ModificationTime: time.Now().UnixMilli(),
		Stats:            string(stats.Json()),
		PartitionValues:  make(map[string]string),
	}
	transaction := table.CreateTransaction(delta.NewTransactionOptions())

	transaction.AddAction(add)
	operation := delta.Write{Mode: delta.Overwrite}
	appMetaData := make(map[string]any)
	appMetaData["test"] = 123

	transaction.SetAppMetadata(appMetaData)
	transaction.SetOperation(operation)
	v, err := transaction.Commit()
```


--- 
Read the data
## Start a pyspark session
```sh
pyspark --packages io.delta:delta-core_2.12:2.2.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```

```python
df = spark.read.format("delta").load("table")
df.show()
```

---
Limitations / TODO

Checkpoints:
- The checkpoint checksum is not being written or validated

Other:
- Nested schemas (containing nested structs, arrays, or maps) are not supported
- Deletion vectors are not supported
- Table features are not supported
- Change data files are not supported
- CDC files are not supported
- Add stats need to be manually generated instead of being read from the parquet file
