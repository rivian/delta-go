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
	"reflect"
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
	expectedSchema := Schema{
		Type: Struct,
		Fields: []SchemaField{
			{Name: "id", Type: Integer, Nullable: false, Metadata: map[string]any{}},
			{Name: "label", Type: String, Nullable: true, Metadata: map[string]any{}},
			{Name: "value", Type: Double, Nullable: true, Metadata: map[string]any{}},
			{Name: "sub_struct", Type: SchemaTypeStruct{
				Type: Struct, Fields: []SchemaField{
					{Name: "data", Type: String, Nullable: true, Metadata: map[string]any{}},
				},
			}, Nullable: true, Metadata: map[string]any{}},
		},
	}
	if !reflect.DeepEqual(schema, expectedSchema) {
		t.Errorf("expected %v got %v", expectedSchema, schema)
	}

	b := schema.Json()
	schemaString := string(b)
	fmt.Println(schemaString)
	expectedStr := `{"type":"struct","fields":[{"name":"id","type":"integer","nullable":false,"metadata":{}},{"name":"label","type":"string","nullable":true,"metadata":{}},{"name":"value","type":"double","nullable":true,"metadata":{}},{"name":"sub_struct","type":{"type":"struct","fields":[{"name":"data","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]}`
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
	if schema.Fields[0].Type != Binary {
		t.Error("does not contain id field")
	}

	if schema.Fields[1].Name != "timestamp" {
		t.Error("does not contain timestamp field")
	}

	if schema.Fields[1].Type != Timestamp {
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
