// ! Delta Table schema implementation.
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
	"strconv"
	"strings"
	"time"

	"github.com/rivian/delta-go/state"
)

// / Type alias for a string expected to match a GUID/UUID format
type Guid string

// / Type alias for i64/Delta long
type DeltaDataTypeLong state.DeltaDataTypeLong

// / Type alias representing the expected type (i64) of a Delta table version.
type DeltaDataTypeVersion state.DeltaDataTypeLong

// / Type alias representing the expected type (i64/ms since Unix epoch) of a Delta timestamp.
type DeltaDataTypeTimestamp int64

// / Type alias for i32/Delta int
type DeltaDataTypeInt int32

const (
	STRUCT_TAG = "struct"
	ARRAY_TAG  = "array"
	MAP_TAG    = "map"
)

// Represents the schema of the delta table.
type Schema = SchemaTypeStruct

// Represents the schema of the delta table.
type SchemaTypeStruct struct {
	Fields []SchemaField //`json:"fields"`
}

func (s *SchemaTypeStruct) Json() []byte {

	type constructorSchemaField struct {
		// will always be struct
		Type SchemaDataType `json:"type"`
		// When Type=Struct, SchemaField will contain an StructType with recursive fields
		Fields []SchemaField `json:"fields"`
	}
	containerStruct := constructorSchemaField{
		Type:   Struct,
		Fields: s.Fields,
	}
	b, _ := json.Marshal(containerStruct)
	return b
}

// Describes a specific field of the Delta table schema.
type SchemaField struct {
	// Name of this (possibly nested) column
	Name string         `json:"name"`
	Type SchemaDataType `json:"type"`
	// Boolean denoting whether this field can be null
	Nullable bool `json:"nullable"`
	// A JSON map containing information about this column. Keys prefixed with Delta are reserved
	// for the implementation.
	Metadata map[string]any `json:"metadata"`

	// When Type=Struct, SchemaField will contain an StructType with recursive fields
	Fields []SchemaField `json:"fields,omitempty"`
}

// / Enum with variants for each top level schema data type.
// / Variant representing non-array, non-map, non-struct fields. Wrapped value will contain the
// / the string name of the primitive type.
type SchemaDataType string

const (
	String    SchemaDataType = "string"    //  * string: utf8
	Long      SchemaDataType = "long"      //  * long  // undocumented, i64?
	Integer   SchemaDataType = "integer"   //  * integer: i32
	Short     SchemaDataType = "short"     //  * short: i16
	Byte      SchemaDataType = "byte"      //  * byte: i8
	Float     SchemaDataType = "float"     //  * float: f32
	Double    SchemaDataType = "double"    //  * double: f64
	Boolean   SchemaDataType = "boolean"   // * boolean: bool
	Binary    SchemaDataType = "binary"    //  * binary: a sequence of binary data
	Date      SchemaDataType = "date"      //  * date: A calendar date, represented as a year-month-day triple without a timezone
	Timestamp SchemaDataType = "timestamp" // * timestamp: Microsecond precision timestamp without a timezone
	Struct    SchemaDataType = "struct"    // * timestamp: Microsecond precision timestamp without a timezone
	Unknown   SchemaDataType = "unknown"
)

// GetSchema recursively walks over the given struct interface i and extracts SchemaTypeStruct StructFields using reflect
// TODO: Handel error cases where types are not compatible with spark types.
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#schema-serialization-format
// i.e. Value int is not currently readable with spark.read.format("delta").load("...")
func GetSchema(i any) SchemaTypeStruct {

	t := reflect.TypeOf(i).Elem()
	numFields := t.NumField()
	var fields []SchemaField
	for index := 0; index < numFields; index++ {

		structField := t.FieldByIndex([]int{index})

		typeTag := structField.Tag.Get("type")
		typetagSplit := strings.Split(typeTag, ",")
		deltatype := ""
		if len(typetagSplit) > 0 {
			if typetagSplit[0] != "" {
				deltatype = typetagSplit[0]
			}
		}

		parquetTag := structField.Tag.Get("parquet")
		parquettagSplit := strings.Split(parquetTag, ",")
		name := structField.Name
		if len(parquettagSplit) > 0 {
			if parquettagSplit[0] != "" {
				name = parquettagSplit[0]
			}
		}

		nullableTag := structField.Tag.Get("nullable")
		nullabletagSplit := strings.Split(nullableTag, ",")
		nullable := true
		if len(nullabletagSplit) > 0 {
			nullableStr := nullabletagSplit[0]
			if nullableStr != "" {
				boolValue, _ := strconv.ParseBool(nullableStr)
				nullable = boolValue
			}
		}

		emptyMetaData := make(map[string]any)
		var field SchemaField
		//Check for timestamp or date delta tag because timestamp is encoded as uint64
		if deltatype != "" {
			switch deltatype {
			case "timestamp":
				field = SchemaField{Name: name, Type: Timestamp, Nullable: nullable, Metadata: emptyMetaData}
			case "date":
				field = SchemaField{Name: name, Type: Date, Nullable: nullable, Metadata: emptyMetaData}
			default:
				field = SchemaField{Name: name, Type: String, Nullable: nullable, Metadata: emptyMetaData}
			}
		} else {
			switch structField.Type.Kind() {
			case reflect.String:
				field = SchemaField{Name: name, Type: String, Nullable: nullable, Metadata: emptyMetaData}
			case reflect.Int64: // long  undocumented, i64?
				field = SchemaField{Name: name, Type: Long, Nullable: nullable, Metadata: emptyMetaData}
			case reflect.Int: //  * integer: i32
				field = SchemaField{Name: name, Type: Integer, Nullable: nullable, Metadata: emptyMetaData}
			case reflect.Int16: //  * short: i16
				field = SchemaField{Name: name, Type: Short, Nullable: nullable, Metadata: emptyMetaData}
			case reflect.Int8: //  * byte: i8
				field = SchemaField{Name: name, Type: Byte, Nullable: nullable, Metadata: emptyMetaData}
			case reflect.Float32: //  * float: f32
				field = SchemaField{Name: name, Type: Float, Nullable: nullable, Metadata: emptyMetaData}
			case reflect.Float64: //  * double: f64
				field = SchemaField{Name: name, Type: Double, Nullable: nullable, Metadata: emptyMetaData}
			case reflect.Bool: //  * boolean: bool
				field = SchemaField{Name: name, Type: Boolean, Nullable: nullable, Metadata: emptyMetaData}

			//TODO Handle time.Time timestamps and dates[possibly by providing a deltatype tag parameter]
			case reflect.Struct:
				fieldPointer := reflect.New(structField.Type)
				fieldInterface := fieldPointer.Interface()
				recursiveStruct := GetSchema(fieldInterface)
				field = SchemaField{Name: name, Fields: recursiveStruct.Fields, Type: Struct, Metadata: emptyMetaData}

			default:
				fieldPointer := reflect.New(structField.Type)
				fieldInterface := fieldPointer.Interface()
				schemaDataType := getFieldDataType(fieldInterface)
				field = SchemaField{Name: name, Type: schemaDataType, Nullable: nullable, Metadata: emptyMetaData}

			}
		}
		fields = append(fields, field)

	}
	return SchemaTypeStruct{Fields: fields}
}

func getFieldDataType(t any) SchemaDataType {
	switch t.(type) {
	case string:
		return String
	case *[]byte:
		return Binary
	case []byte:
		return Binary
	case time.Time:
		return Timestamp
	case int64:
		return Long
	case int:
		return Integer
	default:
		return String
	}
}
