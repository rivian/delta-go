package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/chelseajonesr/rfarrow"
	"github.com/google/uuid"
	"github.com/rivian/delta-go"
	"github.com/rivian/delta-go/lock/filelock"
	"github.com/rivian/delta-go/lock/nillock"
	"github.com/rivian/delta-go/state/filestate"
	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/filestore"
	log "github.com/sirupsen/logrus"
)

// To use pyspark locally to inspect the data after:
// pyspark --packages io.delta:delta-core_2.12:2.4.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
func main() {
	dir := "table"
	os.MkdirAll(dir, 0766)

	tmpPath := storage.NewPath(dir)
	store := filestore.New(tmpPath)
	state := filestate.New(tmpPath, "_delta_log/_commit.state")
	lock := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{})
	table := delta.NewTable(store, lock, state)
	metadata := delta.NewTableMetaData("Test Table", "test description", new(delta.Format).Default(), getSchema(), []string{}, make(map[string]string))
	err := table.Create(*metadata, new(delta.Protocol).Default(), delta.CommitInfo{}, []delta.Add{})
	if err != nil {
		log.Error(err)
	}
	// First write
	fileName := fmt.Sprintf("part-%s.snappy.parquet", uuid.New().String())
	filePath := filepath.Join(tmpPath.Raw, fileName)

	// Generate some data
	data := makeData(5)
	_, err = writeParquet(data, filePath)
	if err != nil {
		log.Error(err)
	}

	// Generate a commit that will add the data to the table
	add, _, err := delta.NewAdd(store, storage.NewPath(fileName), make(map[string]string))
	if err != nil {
		log.Error(err)
	}

	// Save the commit.  When this succeeds, the data is now part of the table.
	transaction := table.CreateTransaction(delta.NewTransactionOptions())
	transaction.AddAction(add)
	operation := delta.Write{Mode: delta.Append}
	appMetaData := make(map[string]any)
	appMetaData["test"] = 123
	transaction.SetAppMetadata(appMetaData)
	transaction.SetOperation(operation)
	v, err := transaction.Commit()
	if err != nil {
		log.Error(err)
	}

	log.Infof("version %d", v)

	// Write a checkpoint.
	// If there is any possibility of multiple processes trying to write a checkpoint at the same version,
	// you could use a lock here.
	checkpointed, err := table.CreateCheckpoint(nillock.New(), delta.NewCheckpointConfiguration(), v)
	if err != nil {
		log.Error(err)
	}
	log.Infof("checkpoint created: %v", checkpointed)
}

type data struct {
	Id        int64    `json:"id" parquet:"name=id"`
	Timestamp int64    `json:"timestamp" parquet:"name=timestamp, converted=timestamp_micros"`
	Label     string   `json:"label" parquet:"name=label, converted=UTF8"`
	Value1    float64  `json:"value1" parquet:"name=value1"`
	Value2    *float64 `json:"value2" parquet:"name=value2"`
	Data      []byte   `json:"data" parquet:"name=data"`
}

func getSchema() delta.SchemaTypeStruct {
	schema := delta.SchemaTypeStruct{
		Fields: []delta.SchemaField{
			{Name: "id", Type: delta.Long, Nullable: false, Metadata: make(map[string]any)},
			{Name: "timestamp", Type: delta.Timestamp, Nullable: false, Metadata: make(map[string]any)},
			{Name: "label", Type: delta.String, Nullable: false, Metadata: make(map[string]any)},
			{Name: "value1", Type: delta.Double, Nullable: false, Metadata: make(map[string]any)},
			{Name: "value2", Type: delta.Double, Nullable: true, Metadata: make(map[string]any)},
			{Name: "data", Type: delta.Binary, Nullable: true, Metadata: make(map[string]any)},
		},
	}
	return schema
}

func makeData(n int) []data {
	id := rand.Int()
	var d []data
	for i := 0; i < n; i++ {
		v := rand.Float64()
		var v2 *float64
		if rand.Float64() > 0.5 {
			v2 = &v
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(rand.Int()))
		row := data{
			Id:        int64(id),
			Timestamp: time.Now().UnixMicro(),
			Label:     uuid.NewString(),
			Value1:    v,
			Value2:    v2,
			Data:      b,
		}
		d = append(d, row)
	}
	return d
}

type payload struct {
	File *os.File
	Size int64
}

func writeParquet[T any](data []T, filename string) (*payload, error) {
	p := new(payload)

	file, err := os.Create(filename)
	if err != nil {
		fmt.Println(err)
	}
	buf := new(bytes.Buffer)
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	err = rfarrow.WriteGoStructsToParquet(data, buf, props)
	if err != nil {
		return p, err
	}
	_, err = file.Write(buf.Bytes())
	if err != nil {
		return p, err
	}

	info, _ := file.Stat()
	p.Size = info.Size()
	p.File = file

	if err := file.Close(); err != nil {
		return p, err
	}
	return p, nil
}
