package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow/go/v13/parquet"
	"github.com/chelseajonesr/rfarrow"
	"github.com/google/uuid"
	"github.com/rivian/delta-go"
	"github.com/rivian/delta-go/lock/filelock"
	"github.com/rivian/delta-go/state/filestate"
	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/filestore"
	log "github.com/sirupsen/logrus"
)

func main() {
	dir := "table"
	os.MkdirAll(dir, 0766)

	tmpPath := storage.NewPath(dir)
	store := filestore.New(tmpPath)
	state := filestate.New(tmpPath, "_delta_log/_commit.state")
	lock := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.Options{})
	checkpointLock := filelock.New(tmpPath, "_delta_log/_checkpoint.lock", filelock.Options{})
	table := delta.NewTable(store, lock, state)
	metadata := delta.NewTableMetaData("Test Table", "test description", new(delta.Format).Default(), getSchema(), []string{}, make(map[string]string))
	err := table.Create(*metadata, new(delta.Protocol).Default(), delta.CommitInfo{}, []delta.Add{})
	if err != nil {
		log.Error(err)
	}
	// First write
	fileName := fmt.Sprintf("part-%s.snappy.parquet", uuid.New().String())
	filePath := filepath.Join(tmpPath.Raw, fileName)

	//Make some data
	data := makeData(5)
	stats := makeStats(data)
	p, err := writeParquet(data, filePath)
	if err != nil {
		log.Error(err)
	}

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
	if err != nil {
		log.Error(err)
	}

	log.Infof("version %d", v)
	table.CreateCheckpoint(checkpointLock, delta.NewCheckpointConfiguration(), v)

}

type simpleCheckpointTestPartition struct {
	Date string `json:"date" parquet:"date"`
}

type data struct {
	Id     int64     `parquet:"id,snappy"`
	T1     int64     `parquet:"t1,timestamp(microsecond)"`
	T2     time.Time `parquet:"t2,timestamp"`
	Label  string    `parquet:"label,dict,snappy"`
	Value1 float64   `parquet:"value1,snappy" nullable:"false"`
	Value2 *float64  `parquet:"value2,snappy" nullable:"true"`
	Data   []byte    `parquet:"data,plain,snappy" nullable:"true"`
}

func getSchema() delta.SchemaTypeStruct {

	// schema := GetSchema(data)
	schema := delta.SchemaTypeStruct{
		Fields: []delta.SchemaField{
			{Name: "id", Type: delta.Long, Nullable: false, Metadata: make(map[string]any)},
			{Name: "t1", Type: delta.Timestamp, Nullable: false, Metadata: make(map[string]any)},
			{Name: "t2", Type: delta.Timestamp, Nullable: false, Metadata: make(map[string]any)},
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
			Id:     int64(id),
			T1:     time.Now().UnixMicro(),
			T2:     time.Now(),
			Label:  uuid.NewString(),
			Value1: v,
			Value2: v2,
			Data:   b,
		}
		d = append(d, row)
	}
	return d
}

func makeStats(data []data) delta.Stats {
	stats := delta.Stats{}
	for _, row := range data {
		stats.NumRecords++
		delta.UpdateStats(&stats, "id", &row.Id)
		delta.UpdateStats(&stats, "t1", &row.T1)
		i := row.T2.UnixMicro()
		delta.UpdateStats(&stats, "t2", &i)
		delta.UpdateStats(&stats, "label", &row.Label)
		delta.UpdateStats(&stats, "value1", &row.Value1)
		delta.UpdateStats(&stats, "value2", row.Value2)
	}
	return stats
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
	defer file.Close()

	props := parquet.WriterProperties{}
	rfarrow.WriteGoStructsToParquet(data, file, &props)

	info, _ := file.Stat()
	p.Size = info.Size()
	p.File = file
	return p, nil
}
