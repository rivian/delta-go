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
package delta

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TODO Make more unit tests for commits

func TestLogEntryFromActions(t *testing.T) {
	add1 := Add{
		Path:             "part-1.snappy.parquet",
		Size:             1,
		ModificationTime: 1675020556534,
		DataChange:       false,
	}
	add2 := &Add{
		Path:             "part-2.snappy.parquet",
		Size:             2,
		ModificationTime: 1675020556534,
		DataChange:       false,
	}

	write := Write{Mode: ErrorIfExists}
	commit := write.GetCommitInfo()
	commit["timestamp"] = 1675020556534

	var data []Action
	data = append(data, commit)
	data = append(data, add1)
	data = append(data, add2)
	logs, err := LogEntryFromActions(data)
	if err != nil {
		t.Error(err)
	}
	println(string(logs))

	expectedStr := `{"commitInfo":{"operation":"delta-go.Write","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]","predicate":"[]"},"timestamp":1675020556534}}
{"add":{"path":"part-1.snappy.parquet","size":1,"partitionValues":null,"modificationTime":1675020556534,"dataChange":false,"stats":""}}
{"add":{"path":"part-2.snappy.parquet","size":2,"partitionValues":null,"modificationTime":1675020556534,"dataChange":false,"stats":""}}`

	if !strings.Contains(string(logs), `{"commitInfo":{"operation":"delta-go.Write"`) {
		t.Errorf("want:\n%s\nhas:\n%s\n", expectedStr, string(logs))
	}

	if !strings.Contains(string(logs), `{"commitInfo":{"operation":"delta-go.Write","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]","predicate":"[]"},"timestamp":1675020556534}}`) {
		t.Errorf("want:\n%s\nhas:\n%s\n", expectedStr, string(logs))
	}

	if !strings.Contains(string(logs), `{"add":{"path":"part-1.snappy.parquet","partitionValues":null,"size":1,"modificationTime":1675020556534,"dataChange":false,"stats":""}}`) {
		t.Errorf("want:\n%s\nhas:\n%s\n", expectedStr, string(logs))
	}

	if !strings.Contains(string(logs), `{"path":"part-2.snappy.parquet","partitionValues":null,"size":2,"modificationTime":1675020556534,"dataChange":false,"stats":""}`) {
		t.Errorf("want:\n%s\nhas:\n%s\n", expectedStr, string(logs))
	}
}

func TestLogEntryFromAction(t *testing.T) {
	commit := make(CommitInfo)
	commit["path"] = "part-1.snappy.parquet"
	commit["size"] = 1
	commit["ModificationTime"] = time.Now().UnixMilli()

	abytes, err := logEntryFromAction(commit)
	if err != nil {
		t.Error(err)
	}

	println(string(abytes))
}

// Test from example at https://github.com/delta-io/delta/blob/master/PROTOCOL.md#change-metadata
func TestLogEntryFromActionChangeMetaData(t *testing.T) {
	expectedStr := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(`
	{
		"metaData":{
		  "id":"af23c9d7-fff1-4a5a-a2c8-55c59bd782aa",
		  "name":"",
		  "description":"",
		  "format":{"provider":"parquet","options":{}},
		  "schemaString":"...",
		  "partitionColumns":[],
		  "createdTime":"0001-01-01T00:00:00Z",
		  "configuration":{
			"appendOnly": "true"
		  }
		}
	  }
	`, "\n", ""), "\t", ""), " ", "")
	provider := "parquet"
	options := make(map[string]string)
	format := Format{
		Provider: provider,
		Options:  options,
	}
	config := make(map[string]string)
	config[string(AppendOnlyDeltaConfigKey)] = "true"
	id, _ := uuid.Parse("af23c9d7-fff1-4a5a-a2c8-55c59bd782aa")
	schemaString := "..."
	action := MetaData{
		Id:               id,
		Format:           format,
		SchemaString:     schemaString,
		PartitionColumns: []string{},
		Configuration:    config,
	}

	b, err := logEntryFromAction(action)
	if err != nil {
		t.Error(err)
	}

	//TODO: Add more comprehensive tests.
	if !strings.Contains(string(b), `{"id":"af23c9d7-fff1-4a5a-a2c8-55c59bd782aa"`) {
		t.Errorf("want:\n%s\nhas:\n%s\n", expectedStr, string(b))
	}

}

// TestUpdateStats tests gathering stats over a data set that includes pointers
func TestUpdateStats(t *testing.T) {
	type rowType struct {
		Id    int      `parquet:"id,snappy"`
		Label string   `parquet:"label,dict,snappy"`
		Value *float64 `parquet:"value,snappy" nullable:"true"`
	}

	// schema := GetSchema(new(rowType))
	// println(string(schema.SchemaBytes()))

	v1 := 1.23
	v2 := 2.13
	data := []rowType{
		{Id: 0, Label: "row0"},
		{Id: 1, Label: "row1", Value: &v1},
		{Id: 2, Label: "row2", Value: &v2},
		{Id: 3, Label: "row3"},
	}

	stats := Stats{}
	for _, row := range data {
		stats.NumRecords++
		UpdateStats(&stats, "id", &row.Id)
		UpdateStats(&stats, "label", &row.Label)
		UpdateStats(&stats, "value", row.Value)
	}

	b, _ := json.Marshal(stats)
	expectedStr := `{"numRecords":4,"tightBounds":false,"minValues":{"id":0,"label":"row0","value":1.23},"maxValues":{"id":3,"label":"row3","value":2.13},"nullCount":{"id":0,"label":0,"value":2}}`
	statsString := string(b)
	if statsString != expectedStr {
		t.Errorf("has:\n%s\nwant:\n%s", statsString, expectedStr)
	}
}

func TestStatsFromJSON(t *testing.T) {
	minValues := make(map[string]any)
	minValues["id"] = 5
	minValues["field1"] = "hello"

	maxValues := make(map[string]any)
	maxValues["id"] = 50
	maxValues["field1"] = "world"

	nullValues := make(map[string]int64)
	nullValues["id"] = 0
	nullValues["field1"] = 2

	expectedStats := Stats{
		NumRecords:  123,
		TightBounds: true,
		MinValues:   minValues,
		MaxValues:   maxValues,
		NullCount:   nullValues,
	}

	statsStr := "{\"numRecords\":123,\"tightBounds\":true,\"minValues\":{\"field1\":\"hello\",\"id\":5},\"maxValues\":{\"field1\":\"world\",\"id\":50},\"nullCount\":{\"field1\":2,\"id\":0}}"

	stats, err := StatsFromJson([]byte(statsStr))
	if err != nil {
		t.Fatalf("Error in StatsFromJson: %v", err)
	}

	if !reflect.DeepEqual(stats.NumRecords, expectedStats.NumRecords) {
		t.Errorf("NumRecords did not match: %d vs %d", stats.NumRecords, expectedStats.NumRecords)
	}
	if !reflect.DeepEqual(stats.TightBounds, expectedStats.TightBounds) {
		t.Errorf("NumRecords did not match: %t vs %t", stats.TightBounds, expectedStats.TightBounds)
	}
	// MinValues and MaxValues will not match because unmarshalling JSON changes the type of the numeric fields
	// if !reflect.DeepEqual(stats.MinValues, expectedStats.MinValues) {
	// 	t.Errorf("MinValues did not match: %v vs %v", stats.MinValues, expectedStats.MinValues)
	// }
	// if !reflect.DeepEqual(stats.MaxValues, expectedStats.MaxValues) {
	// 	t.Errorf("MaxValues did not match: %v vs %v", stats.MaxValues, expectedStats.MaxValues)
	// }
	if !reflect.DeepEqual(stats.NullCount, expectedStats.NullCount) {
		t.Errorf("NullCount did not match: %v vs %v", stats.NullCount, expectedStats.NullCount)
	}
}

func TestFormatDefault(t *testing.T) {
	format := new(Format).Default()
	b, _ := json.Marshal(format)
	expectedStr := `{"provider":"parquet","options":{}}`
	if string(b) != expectedStr {
		t.Errorf("has:\n%s\nwant:\n%s", string(b), expectedStr)
	}
}

func TestWriteOperationParameters(t *testing.T) {
	write := Write{Mode: Append, PartitionBy: []string{"date"}}
	commit := write.GetCommitInfo()
	commit["timestamp"] = 1675020556534

	var data []Action
	data = append(data, commit)
	logs, err := LogEntryFromActions(data)
	if err != nil {
		t.Error(err)
	}
	println(string(logs))
	expectedStr := `{"commitInfo":{"operation":"delta-go.Write","operationParameters":{"mode":"Append","partitionBy":"[\"date\"]","predicate":"[]"},"timestamp":1675020556534}}`

	// compare the JSON strings
	if !reflect.DeepEqual(expectedStr, string(logs)) {
		t.Errorf("expected %s, but got %s", expectedStr, string(logs))
	}
}

func TestWrite_GetCommitInfo(t *testing.T) {
	// create a new Write struct
	write := Write{
		Mode:        Append,
		PartitionBy: []string{"id", "date"},
		Predicate:   []string{"col = 'value'"},
	}

	// call GetCommitInfo()
	commitInfo := write.GetCommitInfo()

	// define expected commitInfo map
	expected := CommitInfo{
		"operation": "delta-go.Write",
		"operationParameters": map[string]interface{}{
			"mode":        "Append",
			"partitionBy": "[\"id\",\"date\"]",
			"predicate":   "[\"col = 'value'\"]",
		},
	}

	// marshal expected and actual maps to JSON strings for comparison
	expectedJSON, _ := json.Marshal(expected)
	actualJSON, _ := json.Marshal(commitInfo)

	// compare the JSON strings
	if !reflect.DeepEqual(expectedJSON, actualJSON) {
		t.Errorf("expected %s, but got %s", expectedJSON, actualJSON)
	}
}

func TestWrite_GetCommitInfoEmptyPartitionBy(t *testing.T) {
	// create a new Write struct
	write := Write{
		Mode: Append,
		// PartitionBy: []string{""},
		// Predicate: []"col = 'value'",
	}

	// call GetCommitInfo()
	commitInfo := write.GetCommitInfo()

	// define expected commitInfo map
	expected := CommitInfo{
		"operation": "delta-go.Write",
		"operationParameters": map[string]interface{}{
			"mode":        "Append",
			"partitionBy": "[]",
			"predicate":   "[]",
		},
	}

	// marshal expected and actual maps to JSON strings for comparison
	expectedJSON, _ := json.Marshal(expected)
	actualJSON, _ := json.Marshal(commitInfo)

	// compare the JSON strings
	if !reflect.DeepEqual(expectedJSON, actualJSON) {
		t.Errorf("expected %s, but got %s", expectedJSON, actualJSON)
	}
}

func TestActionFromLogEntry(t *testing.T) {
	type args struct {
		unstructuredResult map[string]json.RawMessage
	}

	statsString := `{"numRecords":155,"tightBounds":false,"minValues":{"timestamp":1615338375007003},"maxValues":{"timestamp":1615338377517216},"nullCount":null}`

	// Caveats:
	// CommitInfo's operationParameters is not being tested because the result from the unmarshal process is a map[string]interface{} and I haven't
	// been able to set up an expected map that maintains the interface{} type, so DeepEquals() fails
	tests := []struct {
		name    string
		args    args
		want    Action
		wantErr error
	}{
		{name: "Add", args: args{unstructuredResult: map[string]json.RawMessage{"add": []byte(`{"path":"mypath.parquet","size":8382,"partitionValues":{"date":"2021-03-09"},"modificationTime":1679610144893,"dataChange":true,"stats":"{\"numRecords\":155,\"tightBounds\":false,\"minValues\":{\"timestamp\":1615338375007003},\"maxValues\":{\"timestamp\":1615338377517216},\"nullCount\":null}"}`)}},
			want: &Add{Path: "mypath.parquet", Size: 8382, PartitionValues: map[string]string{"date": "2021-03-09"}, ModificationTime: 1679610144893, DataChange: true,
				Stats: statsString}, wantErr: nil},
		{name: "CommitInfo", args: args{unstructuredResult: map[string]json.RawMessage{"commitInfo": []byte(`{"clientVersion":"delta-go.alpha-0.0.0","isBlindAppend":true,"operation":"delta-go.Write","timestamp":1679610144893}`)}},
			want: &CommitInfo{"clientVersion": "delta-go.alpha-0.0.0", "isBlindAppend": true, "operation": "delta-go.Write",
				"timestamp": float64(1679610144893)}, wantErr: nil},
		{name: "Protocol", args: args{unstructuredResult: map[string]json.RawMessage{"protocol": []byte(`{"minReaderVersion":2,"minWriterVersion":7}`)}},
			want: &Protocol{MinReaderVersion: 2, MinWriterVersion: 7}, wantErr: nil},
		{name: "Fail on invalid JSON", args: args{unstructuredResult: map[string]json.RawMessage{"add": []byte(`"path":"s3a://bucket/table","size":8382,"partitionValues":{"date":"2021-03-09"},"modificationTime":1679610144893,"dataChange":true}`)}},
			want: nil, wantErr: ErrActionJSONFormat},
		{name: "Fail on unknown", args: args{unstructuredResult: map[string]json.RawMessage{"fake": []byte(`{}`)}}, want: nil, wantErr: ErrActionUnknown},
		{name: "Fail on CDC", args: args{unstructuredResult: map[string]json.RawMessage{"cdc": []byte(`{}`)}}, want: nil, wantErr: ErrCDCNotSupported},
		{name: "Fail on multiple entries", args: args{unstructuredResult: map[string]json.RawMessage{
			"protocol":   []byte(`{"minReaderVersion":2,"minWriterVersion":7}`),
			"commitInfo": []byte(`{"clientVersion":"delta-go.alpha-0.0.0","isBlindAppend":true,"operation":"delta-go.Write","timestamp":1679610144893}`)}},
			want: nil, wantErr: ErrActionJSONFormat},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := actionFromLogEntry(tt.args.unstructuredResult)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("actionFromLogEntry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("actionFromLogEntry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestActionsFromLogEntries(t *testing.T) {
	stats := Stats{
		NumRecords: 123,
	}

	add := Add{
		Path:             "part-1.snappy.parquet",
		Size:             1,
		ModificationTime: 1675020556534,
		Stats:            string(stats.Json()),
	}

	write := Write{Mode: ErrorIfExists}
	commit := write.GetCommitInfo()
	commit["timestamp"] = 1675020556534

	var data []Action
	data = append(data, commit)
	data = append(data, add)
	logs, err := LogEntryFromActions(data)
	if err != nil {
		t.Fatalf("LogEntryFromActions() error = %v", err)
	}
	logBytes := []byte(logs)

	actions, err := ActionsFromLogEntries(logBytes)
	if err != nil {
		t.Fatalf("ActionsFromLogEntries() error = %v", err)
	}

	if len(actions) != len(data) {
		t.Fatalf("Wrong number of actions returned. Got %d expected %d", len(actions), len(data))
	}

	// resultCommit, ok := actions[0].(*CommitInfo)
	resultCommit, ok := actions[0].(*CommitInfo)
	if !ok {
		t.Error("Expected CommitInfo for first action")
	}
	// JSON unmarshalling changes some types
	commit["timestamp"] = float64(commit["timestamp"].(int))
	commit["operationParameters"].(map[string]interface{})["mode"] = string(commit["operationParameters"].(map[string]interface{})["mode"].(SaveMode))
	if !reflect.DeepEqual(*resultCommit, commit) {
		t.Errorf("Commit did not match.  Got %v expected %v", *resultCommit, commit)
	}

	resultAdd, ok := actions[1].(*Add)
	if !ok {
		t.Error("Expected Add for second action")
	}
	if !reflect.DeepEqual(*resultAdd, add) {
		t.Errorf("Add did not match.  Got %v expected %v", *resultAdd, add)
	}
}

func TestMetadataGetSchema(t *testing.T) {
	// Simple
	schemaString := "{\"type\":\"struct\",\"fields\":[{\"name\":\"value\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ts\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}"
	md := new(MetaData)
	md.SchemaString = schemaString
	schema, err := md.GetSchema()
	if err != nil {
		t.Error(err)
	}
	if len(schema.Fields) != 3 {
		t.Errorf("Expected 3 fields in schema, found %d", len(schema.Fields))
	}

	// TODO nested struct
	// schemaString = `{"type":"struct","fields":[{"name":"some_struct","type":{"type":"struct","fields":[{"name":"some_struct_member","type":"string","nullable":true,"metadata":{}},{"name":"some_struct_timestamp","type":"timestamp","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]}`
	// md.SchemaString = schemaString
	// schema, err = md.GetSchema()
	// if err != nil {
	// 	t.Error(err)
	// }

	// TODO array
	// schemaString = `{"type":"struct","fields":[{"name":"type","type":"string","nullable":true,"metadata":{}},{"name":"names","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{}}]}`
	// md.SchemaString = schemaString
	// schema, err = md.GetSchema()
	// if err != nil {
	// 	t.Error(err)
	// }

	// TODO map
	// schemaString = `{"type":"struct","fields":[{"name":"key","type":"string","nullable":true,"metadata":{}},{"name":"metric","type":{"type":"map","keyType":"string","valueType":"float","valueContainsNull":true},"nullable":true,"metadata":{}}]}`
	// md.SchemaString = schemaString
	// schema, err = md.GetSchema()
	// if err != nil {
	// 	t.Error(err)
	// }
}
