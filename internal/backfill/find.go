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
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/rivian/delta-go"
	"github.com/rivian/delta-go/lock/filelock"
	"github.com/rivian/delta-go/state"
	"github.com/rivian/delta-go/state/filestate"
	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/s3store"
	log "github.com/sirupsen/logrus"
)

// Insert RowType and PartitionType structs here (compilation will otherwise fail)

type CommitInfo struct {
	Timestamp float64 `json:"timestamp"`
	Operation string  `json:"operation"`
}

var (
	bucketName        string
	objectPrefix      string
	scriptDir         string
	minDate           string
	maxDate           string
	inputPath         string
	loggingPath       string
	resultsPath       string
	commitWaitMinutes time.Duration = 20
	numVersionsToSkip int
)

func init() {
	flag.StringVar(&bucketName, "bucket", "vehicle-telemetry-rivian-dev", "The `name` of the S3 bucket to list objects from.")
	flag.StringVar(&objectPrefix, "prefix", "tables/v1/vehicle_rivian_delta_go_clone/", "The optional `object prefix` of the S3 Object keys to list.")
	flag.StringVar(&scriptDir, "script-directory", "/Users/rahulmadnawat/delta-go-logs/rivian-dev-backfill", "The `script directory` in which to keep script files.")
	// TODO: The minimum and maximum dates should be automatically generated and overriden by these flags.
	flag.StringVar(&minDate, "min_date", "2023-04-29", "The optional `minimum date` for the date partitions to cover.")
	flag.StringVar(&maxDate, "max_date", "", "The optional `maximum date` for the date partitions to cover.")
	flag.StringVar(&inputPath, "input-path", "files_v3.txt", "The optional `input path` to read script results.")
	flag.StringVar(&loggingPath, "logging-path", "logs.txt", "The optional `logging path` to store script logs.")
	flag.StringVar(&resultsPath, "results-path", "files_v3.txt", "The optional `results path` to store script results.")
	flag.IntVar(&numVersionsToSkip, "num-versions-to-skip", 1000, "The `number of versions to skip` when aggregating the state of a table.")
}

func main() {
	findUncommittedFiles()
}

func removeSnappyFiles() {
	resultsWithSnappy, err := readLines(filepath.Join(scriptDir, inputPath))
	if err != nil {
		log.Fatalf("failed reading results")
	}

	var resultsWithoutSnappy []string

	for _, uncommittedFile := range resultsWithSnappy {
		if strings.Contains(uncommittedFile, "mfi") {
			resultsWithoutSnappy = append(resultsWithoutSnappy, uncommittedFile)
		}
	}

	err = writeLines(resultsWithoutSnappy, filepath.Join(scriptDir, resultsPath))
	if err != nil {
		log.Fatalf("failed writing results: %s", err)
	}
}

func findUncommittedFiles() {
	flag.Parse()
	if len(bucketName) == 0 {
		flag.PrintDefaults()
		log.Fatal("invalid parameters, bucket name required")
	}

	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		fmt.Println(err)
	}

	os.MkdirAll(scriptDir, os.ModePerm)

	loggingPath = "logs_find_v3.txt"
	f, err := os.OpenFile(filepath.Join(scriptDir, loggingPath), os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalf("failed creating file: %v", err)
	}
	log.SetOutput(f)

	objectPrefix = strings.TrimSuffix(objectPrefix, "/")

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("failed to load SDK configuration, %v", err)
	}

	client := s3.NewFromConfig(cfg)
	path := storage.NewPath(fmt.Sprintf("s3://%s/%s", bucketName, objectPrefix))

	store, err := s3store.New(client, storage.NewPath(fmt.Sprintf("s3://%s/%s", bucketName, objectPrefix)))
	if err != nil {
		log.Fatalf("failed to set up S3 store %v", err)
	}

	tmpPath := storage.NewPath("")
	fileState := filestate.New(tmpPath, "_delta_log/_commit.state")
	lock := filelock.New(tmpPath, "_delta_log/_commit.lock", filelock.LockOptions{})

	var committedParquetFiles []string
	var table *delta.DeltaTable[FlatRecord, TestPartitionType]

	table, err = delta.OpenTable[FlatRecord, TestPartitionType](store, lock, fileState)
	if err != nil {
		log.Fatalf("failed to open table %v", err)
	}

	version := int(table.State.Version)

	log.WithFields(log.Fields{"version number": version}).Infof("Current table version")

	for filePath := range table.State.Files {
		committedParquetFiles = append(committedParquetFiles, filePath)
	}

	for filePath := range table.State.Tombstones {
		committedParquetFiles = append(committedParquetFiles, filePath)
	}

	numTables := 1 + (version-1)/numVersionsToSkip
	tables := make([]*delta.DeltaTable[FlatRecord, TestPartitionType], numTables)

	var wg = &sync.WaitGroup{}
	// TODO: 5000 should not be hard-coded here, use a channel to exit from a goroutine instead
	for version = version - numVersionsToSkip; version > 5000; version = version - numVersionsToSkip {
		wg.Add(1)
		go func(version int) {
			tableNum := 1 + (version-1)/numVersionsToSkip

			log.Infof(fmt.Sprint(tableNum))

			tables[tableNum-1], err = delta.OpenTableWithVersion[FlatRecord, TestPartitionType](store, lock, fileState, state.DeltaDataTypeVersion(version))
			if err != nil || tables[tableNum-1] == nil || tables[tableNum-1].State.Files == nil {
				wg.Done()
			}

			for filePath := range tables[tableNum-1].State.Files {
				committedParquetFiles = append(committedParquetFiles, filePath)
			}

			for filePath := range tables[tableNum-1].State.Tombstones {
				committedParquetFiles = append(committedParquetFiles, filePath)
			}

			log.WithFields(log.Fields{"version number": version}).Infof("Processed table version")

			wg.Done()
		}(version)
	}
	wg.Wait()

	committedParquetFiles = removeDuplicates(committedParquetFiles)

	committedParquetFilesByDate := make(map[string][]string)

	for _, committedParquetFile := range committedParquetFiles {
		r, _ := regexp.Compile("(date=.+?)/.+")
		date := r.FindStringSubmatch(committedParquetFile)[1]

		r, _ = regexp.Compile("date=.+?/(.+)")
		key := r.FindStringSubmatch(committedParquetFile)[1]

		committedParquetFilesByDate[date] = append(committedParquetFilesByDate[date], key)
	}

	dates := make([]string, 0, len(committedParquetFilesByDate))

	for date := range committedParquetFilesByDate {
		dates = append(dates, date)
	}

	sort.Strings(dates)

	for _, date := range dates {
		fmt.Println(date, ": ", len(committedParquetFilesByDate[date]))
	}

	_, commonPrefixes, err := ListAll(client, path, storage.NewPath(objectPrefix+"/date="), "/")
	if err != nil {
		log.Fatalf("failed to get common prefixes %v", err)
	}

	var datePartitions []string

	for _, commonPrefix := range commonPrefixes {
		date, ok := getStringInBetweenTwoString(*commonPrefix.Prefix, "date=", "/")
		if !ok {
			log.Fatal("failed to parse date")
		}
		datePartitions = append(datePartitions, date)
	}

	datePartitions = removeDuplicates(datePartitions)

	if len(minDate) > 0 {
		var temp []string

		for _, datePartition := range datePartitions {
			if datePartition >= minDate {
				temp = append(temp, datePartition)
			}
		}

		datePartitions = temp
	}

	if len(maxDate) > 0 {
		var temp []string

		for _, datePartition := range datePartitions {
			if datePartition <= maxDate {
				temp = append(temp, datePartition)
			}
		}

		datePartitions = temp
	}

	commitInfo := table.State.CommitInfos[len(table.State.CommitInfos)-1]

	var ci CommitInfo

	marshaledCommitInfo, err := json.Marshal(commitInfo)
	if err != nil {
		log.Fatalf("failed to marshal commit info %v", err)
	}

	err = json.Unmarshal(marshaledCommitInfo, &ci)
	if err != nil {
		log.Fatalf("failed to unmarshal commit info %v", err)
	}

	lastCommitTimestamp := time.Unix(0, int64(ci.Timestamp)*int64(time.Millisecond)).In(loc)

	var listResults storage.ListResult

	for _, datePartition := range datePartitions {
		datePartitionFiles, _, err := ListAll(client, path, storage.NewPath(fmt.Sprintf("%s/date=%s/", objectPrefix, datePartition)), "")
		if err != nil {
			log.Fatalf("failed to list all %s partition files %v", datePartition, err)
		}
		listResults.Objects = append(listResults.Objects, datePartitionFiles.Objects...)
	}

	var allParquetFiles []string

	for _, listResult := range listResults.Objects {
		if (strings.HasSuffix(listResult.Location.Raw, ".parquet") || strings.HasSuffix(listResult.Location.Raw, ".parquet.zstd")) && listResult.LastModified.In(loc).Before(lastCommitTimestamp.Add(-time.Minute*commitWaitMinutes)) {
			allParquetFiles = append(allParquetFiles, strings.TrimPrefix(listResult.Location.Raw, objectPrefix+"/"))
		}
	}
	fmt.Printf("Number of parquet files within specified date range: %d\n", len(allParquetFiles))

	committedParquetFiles = removeDuplicates(committedParquetFiles)
	fmt.Printf("Number of committed parquet files: %d\n", len(committedParquetFiles))

	uncommittedParquetFiles := findDifference(allParquetFiles, committedParquetFiles)
	fmt.Printf("Number of uncommitted parquet files: %d\n", len(uncommittedParquetFiles))

	err = writeLines(uncommittedParquetFiles, filepath.Join(scriptDir, resultsPath))
	if err != nil {
		log.Fatalf("failed writing results: %s", err)
	}
}

func removeDuplicates(strSlice []string) []string {
	allKeys := make(map[string]bool)
	list := []string{}

	for _, item := range strSlice {
		if _, value := allKeys[item]; !value {
			allKeys[item] = true
			list = append(list, item)
		}
	}

	return list
}

func findDifference(a, b []string) []string {
	mb := make(map[string]struct{}, len(b))

	for _, x := range b {
		mb[x] = struct{}{}
	}

	var diff []string

	for _, x := range a {
		if _, found := mb[x]; !found {
			diff = append(diff, x)
		}
	}

	return diff
}

func readLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func writeLines(lines []string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

func getStringInBetweenTwoString(str string, startS string, endS string) (result string, found bool) {
	s := strings.Index(str, startS)
	if s == -1 {
		return result, false
	}
	newS := str[s+len(startS):]
	e := strings.Index(newS, endS)
	if e == -1 {
		return result, false
	}
	result = newS[:e]
	return result, true
}

func ListAll(client *s3.Client, path *storage.Path, prefix *storage.Path, delimiter string) (storage.ListResult, []types.CommonPrefix, error) {
	var listResult storage.ListResult
	// We will need the store path with the leading and trailing /'s for trimming results
	pathWithSeparators := path.Raw
	if !strings.HasPrefix(pathWithSeparators, "/") {
		pathWithSeparators = "/" + pathWithSeparators
	}
	if !strings.HasSuffix(pathWithSeparators, "/") {
		pathWithSeparators = pathWithSeparators + "/"
	}

	bucketName, ok := getStringInBetweenTwoString(pathWithSeparators, "s3://", "/")
	if !ok {
		log.Fatalf("failed to parse date")
	}
	params := &s3.ListObjectsV2Input{
		Bucket: &bucketName,
	}
	if len(delimiter) != 0 {
		params.Delimiter = &delimiter
	}
	if len(prefix.Raw) != 0 {
		params.Prefix = &prefix.Raw
	}

	p := s3.NewListObjectsV2Paginator(client, params)
	var commonPrefixes []types.CommonPrefix

	var i int
	for p.HasMorePages() {
		i++

		page, err := p.NextPage(context.TODO())
		if err != nil {
			log.Fatalf("failed to get page %v, %v", i, err)
		}

		for _, obj := range page.Contents {
			location := strings.TrimPrefix(*obj.Key, pathWithSeparators)
			listResult.Objects = append(listResult.Objects, storage.ObjectMeta{
				Location:     *storage.NewPath(location),
				LastModified: *obj.LastModified,
				Size:         obj.Size,
			})
		}
		commonPrefixes = append(commonPrefixes, page.CommonPrefixes...)
	}

	return listResult, commonPrefixes, nil
}
