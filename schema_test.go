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
	"fmt"
	"testing"
)

func TestGetSchema(t *testing.T) {
	type SubStruct struct {
		Data string `parquet:"data"`
	}

	type RowType struct {
		Id        int       `parquet:"id,snappy" nullable:"false"`
		Label     string    `parquet:"label,dict,snappy"`
		Value     float64   `parquet:"value,snappy"`
		SubStruct SubStruct `parquet:"sub_struct"`
	}

	schema := GetSchema(new(RowType))
	if schema.Fields[0].Name != "id" {
		t.Error("does not contain id field")
	}
	if schema.Fields[0].Type != "integer" {
		t.Error("does not contain id field")
	}
	if schema.Fields[0].Nullable != false {
		t.Error("does not contain id field")
	}

	if schema.Fields[3].Name != "sub_struct" {
		t.Error("does not contain sub_struct field")
	}

	if schema.Fields[3].Type != "struct" {
		t.Error("does not contain sub_struct field")
	}

	if len(schema.Fields[3].Fields) != 1 {
		t.Error("does not contain sub_struct field")
	}

	if schema.Fields[3].Fields[0].Name != "data" {
		t.Error("does not contain sub_struct.data field")
	}
	if schema.Fields[3].Fields[0].Type != "string" {
		t.Error("does not contain sub_struct.data field")
	}

	b := schema.Json()
	schemaString := string(b)
	fmt.Println(schemaString)
	// TODO this is incorrect
	// Nested struct should be formatted like this: "type":{"type":"struct","fields":[...]}
	expectedStr := `{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},{"name":"label","type":"string","nullable":true,"metadata":{}},{"name":"value","type":"double","nullable":true,"metadata":{}},{"name":"sub_struct","type":"struct","nullable":false,"metadata":{},"fields":[{"name":"data","type":"string","nullable":true,"metadata":{}}]}]}`
	if schemaString != expectedStr {
		t.Errorf("has:\n%s\nwant:\n%s", schemaString, expectedStr)
	}

}

func TestGetSchemaWithWeirdTypes(t *testing.T) {
	type RowType struct {
		Bin       []byte `parquet:"bin,snappy"`
		Timestamp int64  `parquet:"timestamp,snappy" type:"timestamp"`
	}

	schema := GetSchema(new(RowType))
	if schema.Fields[0].Name != "bin" {
		t.Error("does not contain id field")
	}
	if schema.Fields[0].Type != "binary" {
		t.Error("does not contain id field")
	}

	if schema.Fields[1].Name != "timestamp" {
		t.Error("does not contain timestamp field")
	}

	if schema.Fields[1].Type != "timestamp" {
		t.Error("does not contain timestamp field")
	}

	b := schema.Json()
	schemaString := string(b)
	fmt.Println(schemaString)
	expectedStr := `{"type":"struct","fields":[{"name":"bin","type":"binary","nullable":true,"metadata":{}},{"name":"timestamp","type":"timestamp","nullable":true,"metadata":{}}]}`
	if schemaString != expectedStr {
		t.Errorf("has:\n%s\nwant:\n%s", schemaString, expectedStr)
	}

}
