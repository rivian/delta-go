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
	"sort"
	"testing"

	"github.com/rivian/delta-go/storage"
	"github.com/rivian/delta-go/storage/filestore"
)

// TestUpdateStats tests gathering stats over a data set that includes pointers
func TestUpdateStats(t *testing.T) {

	type rowType struct {
		ID    int      `parquet:"id,snappy"`
		Label string   `parquet:"label,dict,snappy"`
		Value *float64 `parquet:"value,snappy" nullable:"true"`
	}

	// schema := GetSchema(new(rowType))
	// println(string(schema.SchemaBytes()))

	v1 := 1.23
	v2 := 2.13
	data := []rowType{
		{ID: 0, Label: "row0"},
		{ID: 1, Label: "row1", Value: &v1},
		{ID: 2, Label: "row2", Value: &v2},
		{ID: 3, Label: "row3"},
	}

	stats := Stats{}
	for _, row := range data {

		stats.NumRecords++
		UpdateStats(&stats, "id", &row.ID)
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

	stats, err := StatsFromJSON([]byte(statsStr))
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

	statsStr = "{\"numRecords\":4,\"tightBounds\":false,\"minValues\":{\"bytetype\":12,\"datetype\":\"1950-01-01\",\"decimaltype\":1.23,\"doubletype\":0.001,\"floattype\":\"-Infinity\",\"integertype\":-30,\"longtype\":1234567,\"shorttype\":24,\"stringtype\":\"abcdefghijklmnopqrstuvwxyzABCDEF\"},\"maxValues\":{\"bytetype\":120,\"datetype\":\"2023-10-01\",\"decimaltype\":111.23,\"doubletype\":\"Infinity\",\"floattype\":7.6539998054504395,\"integertype\":4331,\"longtype\":111444333,\"shorttype\":43,\"stringtype\":\"zap\"},\"nullCount\":{\"binarytype\":1,\"bytetype\":2,\"datetype\":1,\"decimaltype\":1,\"doubletype\":0,\"floattype\":1,\"integertype\":1,\"longtype\":1,\"shorttype\":1,\"stringtype\":1}}"
	stats, err = StatsFromJSON([]byte(statsStr))
	if err != nil {
		t.Fatalf("Error in StatsFromJson: %v", err)
	}
	if stats.MinValues["floattype"] != "-Infinity" {
		t.Errorf("Expected -Infinity, found %v", stats.MinValues["floattype"])
	}
}

func TestTruncatedStringStat(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedMax string
		expectedMin string
	}{
		{name: "short and simple", input: "asdf8765>}", expectedMax: "asdf8765>}", expectedMin: "asdf8765>}"},
		{name: "short with unicode", input: "Hello, 世界", expectedMax: "Hello, 世界", expectedMin: "Hello, 世界"},
		{name: "empty", input: "", expectedMax: "", expectedMin: ""},
		{name: "max without truncating", input: "12345678901234567890123456789012", expectedMax: "12345678901234567890123456789012" + string(stringStatMaxTiebreaker), expectedMin: "12345678901234567890123456789012"},
		{name: "minimal truncate", input: "123456789012345678901234567890121", expectedMax: "12345678901234567890123456789012" + string(stringStatMaxTiebreaker), expectedMin: "12345678901234567890123456789012"},
		{name: "longer truncate", input: "1234a56789Z012345!678901?234567890121qwertyuiop", expectedMax: "1234a56789Z012345!678901?2345678" + string(stringStatMaxTiebreaker), expectedMin: "1234a56789Z012345!678901?2345678"},
		{name: "unicode truncate", input: "Hello, 世界9Z012345!678901?234567890121qwertyuiop", expectedMax: "Hello, 世界9Z012345!678901?2345678" + string(stringStatMaxTiebreaker), expectedMin: "Hello, 世界9Z012345!678901?2345678"},
		{name: "truncate with required appends", input: "12345678901234567890123456789012" + string('\uffff') + string('\ufffe') + "123", expectedMax: "12345678901234567890123456789012" + string('\uffff') + string('\ufffe') + string(stringStatMaxTiebreaker), expectedMin: "12345678901234567890123456789012"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := truncatedStringStat(tt.input, true)
			if out != tt.expectedMax {
				t.Errorf("expected %s found %s", tt.expectedMax, out)
			}
			out = truncatedStringStat(tt.input, false)
			if out != tt.expectedMin {
				t.Errorf("expected %s found %s", tt.expectedMin, out)
			}
		})
	}
}

func TestStatsFromSource(t *testing.T) {
	store := filestore.New(storage.NewPath("testdata/stats"))

	type args struct {
		location        string
		partitionValues map[string]string
	}
	tests := []struct {
		name        string
		args        args
		want        string
		wantMissing []string
		wantErr     bool
	}{
		{name: "stats1", args: args{location: "stats1.snappy.parquet", partitionValues: nil}, want: "{\"numRecords\":1,\"tightBounds\":false,\"minValues\":{\"city\":\"Kelowna\",\"id\":2},\"maxValues\":{\"city\":\"Kelowna\",\"id\":2},\"nullCount\":{\"city\":0,\"id\":0}}", wantErr: false},
		{name: "stats2", args: args{location: "stats2.zstd.parquet", partitionValues: nil}, want: "{\"numRecords\":8,\"tightBounds\":false,\"minValues\":{\"city\":\"Abbotsford\",\"id\":1,\"info\":\"Cheakamus River\"},\"maxValues\":{\"city\":\"Victoria\",\"id\":10,\"info\":\"Fraser River\"},\"nullCount\":{\"city\":0,\"id\":0,\"info\":6}}", wantErr: false},
		// There is a timestamp in this spark-generated parquet file, but it is int96 and there are no min/max stats available in the parquet file
		{name: "sparkTypes", args: args{location: "sparkTypes.zstd.parquet", partitionValues: nil}, want: "{\"numRecords\":4,\"tightBounds\":false,\"minValues\":{\"bytetype\":12,\"datetype\":\"1950-01-01\",\"decimaltype\":1.23,\"doubletype\":0.001,\"floattype\":1.234,\"integertype\":-30,\"longtype\":1234567,\"shorttype\":24,\"stringtype\":\"abcdefghijklmnopqrstuvwxyzABCDEF\"},\"maxValues\":{\"bytetype\":120,\"datetype\":\"2023-10-01\",\"decimaltype\":111.23,\"doubletype\":12345.6789,\"floattype\":7.654,\"integertype\":4331,\"longtype\":111444333,\"shorttype\":43,\"stringtype\":\"zap\"},\"nullCount\":{\"binarytype\":1,\"bytetype\":2,\"datetype\":1,\"decimaltype\":1,\"doubletype\":0,\"floattype\":2,\"integertype\":1,\"longtype\":1,\"shorttype\":1,\"stringtype\":1}}", wantErr: false, wantMissing: []string{"timestamptype"}},
		{name: "infinity", args: args{location: "infs.snappy.parquet", partitionValues: nil}, want: "{\"numRecords\":4,\"tightBounds\":false,\"minValues\":{\"bytetype\":12,\"datetype\":\"1950-01-01\",\"decimaltype\":1.23,\"doubletype\":0.001,\"floattype\":\"-Infinity\",\"integertype\":-30,\"longtype\":1234567,\"shorttype\":24,\"stringtype\":\"abcdefghijklmnopqrstuvwxyzABCDEF\"},\"maxValues\":{\"bytetype\":120,\"datetype\":\"2023-10-01\",\"decimaltype\":111.23,\"doubletype\":\"Infinity\",\"floattype\":7.6539998054504395,\"integertype\":4331,\"longtype\":111444333,\"shorttype\":43,\"stringtype\":\"zap\"},\"nullCount\":{\"binarytype\":1,\"bytetype\":2,\"datetype\":1,\"decimaltype\":1,\"doubletype\":0,\"floattype\":1,\"integertype\":1,\"longtype\":1,\"shorttype\":1,\"stringtype\":1}}", wantErr: false, wantMissing: []string{"timestamptype"}},
		{name: "nan", args: args{location: "nans.snappy.parquet", partitionValues: nil}, want: "{\"numRecords\":4,\"tightBounds\":false,\"minValues\":{\"bytetype\":12,\"datetype\":\"1950-01-01\",\"decimaltype\":1.23,\"doubletype\":\"NaN\",\"floattype\":\"-Infinity\",\"integertype\":-30,\"longtype\":1234567,\"shorttype\":24,\"stringtype\":\"abcdefghijklmnopqrstuvwxyzABCDEF\"},\"maxValues\":{\"bytetype\":120,\"datetype\":\"2023-10-01\",\"decimaltype\":111.23,\"doubletype\":\"NaN\",\"floattype\":7.6539998054504395,\"integertype\":4331,\"longtype\":111444333,\"shorttype\":43,\"stringtype\":\"zap\"},\"nullCount\":{\"binarytype\":1,\"bytetype\":2,\"datetype\":1,\"decimaltype\":1,\"doubletype\":0,\"floattype\":1,\"integertype\":1,\"longtype\":1,\"shorttype\":1,\"stringtype\":1}}", wantErr: false, wantMissing: []string{"timestamptype"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			add := new(Add)
			add.Path = tt.args.location
			add.PartitionValues = tt.args.partitionValues
			stats, missingColumns, err := StatsFromParquet(store, add)
			if (err != nil) != tt.wantErr {
				t.Errorf("FileObjectStore.ReadAt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(missingColumns) != 0 || len(tt.wantMissing) != 0 {
				sort.Strings(missingColumns)
				sort.Strings(tt.wantMissing)
				if !reflect.DeepEqual(missingColumns, tt.wantMissing) {
					t.Errorf("Expected missing columns %v returned %v", tt.wantMissing, missingColumns)
				}
			}
			statsJSON, err := stats.JSON()
			if (err != nil) != tt.wantErr {
				t.Errorf("Stats.JSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			statsString := string(statsJSON)
			if statsString != tt.want {
				t.Errorf("Expected\n%s\nReturned\n%s", tt.want, statsString)
			}
		})
	}
}
