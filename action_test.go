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
		ModificationTime: DeltaDataTypeTimestamp(1675020556534),
	}
	add2 := &Add{
		Path:             "part-2.snappy.parquet",
		Size:             2,
		ModificationTime: DeltaDataTypeTimestamp(1675020556534),
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
	{"add":{"path":"part-1.snappy.parquet","size":1,"partitionValues":null,"modificationTime":{},"dataChange":false,"stats":"","Tags":null}}
	{"path":"part-2.snappy.parquet","size":2,"partitionValues":null,"modificationTime":{},"dataChange":false,"stats":"","Tags":null}`

	if !strings.Contains(string(logs), `{"commitInfo":{"operation":"delta-go.Write"`) {
		t.Errorf("want:\n%s\nhas:\n%s\n", expectedStr, string(logs))
	}

	if !strings.Contains(string(logs), `{"commitInfo":{"operation":"delta-go.Write","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]","predicate":"[]"},"timestamp":1675020556534}}`) {
		t.Errorf("want:\n%s\nhas:\n%s\n", expectedStr, string(logs))
	}

	// if !strings.Contains(string(logs), `{"add":{"path":"part-1.snappy.parquet","size":1,"partitionValues":null,"modificationTime":1675020556534,"dataChange":false,"stats":"","Tags":null}}`) {
	// 	t.Errorf("want:\n%s\nhas:\n%s\n", expectedStr, string(logs))
	// }

	if !strings.Contains(string(logs), `{"path":"part-2.snappy.parquet","size":2,"partitionValues":null,"modificationTime":1675020556534,"dataChange":false,"stats":""}`) {
		t.Errorf("want:\n%s\nhas:\n%s\n", expectedStr, string(logs))
	}
}

func TestLogEntryFromAction(t *testing.T) {

	commit := make(CommitInfo)
	commit["path"] = "part-1.snappy.parquet"
	commit["size"] = 1
	commit["ModificationTime"] = DeltaDataTypeTimestamp(time.Now().UnixMilli())

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
	format := Format{
		Provider: "parquet",
		Options:  make(map[string]string),
	}
	config := make(map[string]string)
	config["appendOnly"] = "true"
	id, _ := uuid.Parse("af23c9d7-fff1-4a5a-a2c8-55c59bd782aa")
	action := MetaData{
		Id:               id,
		Format:           format,
		SchemaString:     "...",
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
	expectedStr := `{"numRecords":4,"tightBounds":false,"minValues":{"id":0,"label":"row0","value":1.23},"maxValues":{"id":3,"label":"row3","value":2.13},"nullCount":{"value":2}}`
	statsString := string(b)
	if statsString != expectedStr {
		t.Errorf("has:\n%s\nwant:\n%s", statsString, expectedStr)
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

	// Caveats:
	// CommitInfo's operationParameters is not being tested because the result from the unmarshal process is a map[string]interface{} and I haven't
	// been able to set up an expected map that maintains the interface{} type, so DeepEquals() fails
	tests := []struct {
		name    string
		args    args
		want    Action
		wantErr bool
	}{
		{name: "Add", args: args{unstructuredResult: map[string]json.RawMessage{"add": []byte(`{"path":"mypath.parquet","size":8382,"partitionValues":{"date":"2021-03-09"},"modificationTime":1679610144893,"dataChange":true,"stats":"{\"numRecords\":155,\"tightBounds\":false,\"minValues\":{\"timestamp\":1615338375007003},\"maxValues\":{\"timestamp\":1615338377517216},\"nullCount\":null}"}`)}},
			want: &Add{Path: "mypath.parquet", Size: 8382, PartitionValues: map[string]string{"date": "2021-03-09"}, ModificationTime: 1679610144893, DataChange: true,
				Stats: `{"numRecords":155,"tightBounds":false,"minValues":{"timestamp":1615338375007003},"maxValues":{"timestamp":1615338377517216},"nullCount":null}`}, wantErr: false},
		{name: "CommitInfo", args: args{unstructuredResult: map[string]json.RawMessage{"commitInfo": []byte(`{"clientVersion":"delta-go.alpha-0.0.0","isBlindAppend":true,"operation":"delta-go.Write","timestamp":1679610144893}`)}},
			want: &CommitInfo{"clientVersion": "delta-go.alpha-0.0.0", "isBlindAppend": true, "operation": "delta-go.Write",
				"timestamp": float64(1679610144893)}, wantErr: false},
		{name: "Protocol", args: args{unstructuredResult: map[string]json.RawMessage{"protocol": []byte(`{"minReaderVersion":2,"minWriterVersion":7}`)}},
			want: &Protocol{MinReaderVersion: 2, MinWriterVersion: 7}, wantErr: false},
		{name: "Fail on invalid JSON", args: args{unstructuredResult: map[string]json.RawMessage{"add": []byte(`"path":"s3a://bucket/table","size":8382,"partitionValues":{"date":"2021-03-09"},"modificationTime":1679610144893,"dataChange":true}`)}},
			want: nil, wantErr: true},
		{name: "Fail on unknown", args: args{unstructuredResult: map[string]json.RawMessage{"fake": []byte(`{}`)}}, want: nil, wantErr: true},
		{name: "Fail on CDC", args: args{unstructuredResult: map[string]json.RawMessage{"cdc": []byte(`{}`)}}, want: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := actionFromLogEntry(tt.args.unstructuredResult)
			if (err != nil) != tt.wantErr {
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
	add := Add{
		Path:             "part-1.snappy.parquet",
		Size:             1,
		ModificationTime: DeltaDataTypeTimestamp(1675020556534),
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

	// TODO Not sure how to compare: action contains a pointer, data[i] does not
	// for i, action := range actions {
	// 	if !reflect.DeepEqual(action, data[i]) {
	// 		t.Errorf("Action did not match. Got %v expected %v", action, data[i])
	// 	}
	// }
}
