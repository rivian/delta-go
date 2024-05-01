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

// Package delta contains the resources required to interact with a Delta table.
package delta

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrParseSchema is returned when parsing the schema from JSON fails
	ErrParseSchema error = errors.New("unable to parse schema")
)

// GUID is a type alias for a string expected to match a GUID/UUID format.
type GUID string

// Schema represents the schema of the Delta table.
type Schema = SchemaTypeStruct

// SchemaDataType is one of: 	SchemaDataTypeName | SchemaTypeArray | SchemaTypeMap | SchemaTypeStruct
// We can't use a union constraint because the type is recursive
type SchemaDataType interface{}

// SchemaTypeStruct represents a struct in the schema
type SchemaTypeStruct struct {
	Type   SchemaDataTypeName `json:"type"` // Has to be "struct"
	Fields []SchemaField      `json:"fields"`
}

// SchemaField describes a specific field of the Delta table schema.
type SchemaField struct {
	// Name of this (possibly nested) column
	Name string         `json:"name"`
	Type SchemaDataType `json:"type"`
	// Boolean denoting whether this field can be null
	Nullable bool `json:"nullable"`
	// A JSON map containing information about this column. Keys prefixed with Delta are reserved
	// for the implementation.
	Metadata map[string]any `json:"metadata"`
}

// SchemaTypeArray represents an array field
type SchemaTypeArray struct {
	Type         SchemaDataTypeName `json:"type"` // Has to be "array"
	ElementType  SchemaDataType     `json:"elementType"`
	ContainsNull bool               `json:"containsNull"`
}

// SchemaTypeMap represents a map field
type SchemaTypeMap struct {
	Type              SchemaDataTypeName `json:"type"` // Has to be "map"
	KeyType           SchemaDataType     `json:"keyType"`
	ValueType         SchemaDataType     `json:"valueType"`
	ValueContainsNull bool               `json:"valueContainsNull"`
}

// SchemaDataTypeName contains the string .
type SchemaDataTypeName string

const (
	// String is the schema data type representing a string.
	String SchemaDataTypeName = "string" //  * string: utf8
	// Long is the schema data type representing a long.
	Long SchemaDataTypeName = "long" //  * long  // undocumented, i64?
	// Integer is the schema data type representing an integer.
	Integer SchemaDataTypeName = "integer" //  * integer: i32
	// Short is the schema data type representing a short.
	Short SchemaDataTypeName = "short" //  * short: i16
	// Byte is the schema data type representing a byte.
	Byte SchemaDataTypeName = "byte" //  * byte: i8
	// Float is the schema data type representing a float.
	Float SchemaDataTypeName = "float" //  * float: f32
	// Double is the schema data type representing a double.
	Double SchemaDataTypeName = "double" //  * double: f64
	// Boolean is the schema data type representing a boolean.
	Boolean SchemaDataTypeName = "boolean" //  * boolean: bool
	// Binary is the schema data type representing a binary.
	Binary SchemaDataTypeName = "binary" //  * binary: a sequence of binary data
	// Date is the schema data type representing a date.
	Date SchemaDataTypeName = "date" //  * date: A calendar date, represented as a year-month-day triple without a timezone
	// Timestamp is the schema data type representing a timestamp.
	Timestamp SchemaDataTypeName = "timestamp" //  * timestamp: Microsecond precision timestamp without a timezone
	// Struct is the schema data type representing a struct.
	Struct SchemaDataTypeName = "struct" //  * struct:
	// Array is the schema data type representing an array.
	Array SchemaDataTypeName = "array" //  * array:
	// Map is the schema data type representing a map.
	Map SchemaDataTypeName = "map" //  * map:
	// Unknown is the schema data type representing an unknown.
	Unknown SchemaDataTypeName = "unknown"
)

// JSON marshals a struct type field in a schema into a JSON object.
func (s *SchemaTypeStruct) JSON() []byte {
	containerStruct := SchemaTypeStruct{
		Type:   Struct,
		Fields: s.Fields,
	}
	b, _ := json.Marshal(containerStruct)
	return b
}

// UnmarshalJSON unmarshals a JSON object into a schema field.
func (s *SchemaField) UnmarshalJSON(b []byte) error {
	data := make(map[string]interface{})
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}
	if err := s.unmarshalSchemaField(data); err != nil {
		return err
	}
	return nil
}

func (s *SchemaField) unmarshalSchemaField(data map[string]interface{}) error {
	name, ok := data["name"]
	if !ok || reflect.TypeOf(name) == nil {
		return errors.Join(ErrParseSchema, errors.New("missing name"))
	}
	switch reflect.TypeOf(name).Kind() {
	case reflect.String:
		s.Name = name.(string)
		if len(s.Name) == 0 {
			return errors.Join(ErrParseSchema, errors.New("name is empty"))
		}
	default:
		return errors.Join(ErrParseSchema, errors.New("name must be a string"))
	}

	t, ok := data["type"]
	if !ok || reflect.TypeOf(t) == nil {
		return errors.Join(ErrParseSchema, errors.New("missing type"))
	}
	schemaType, err := unmarshalSchemaType(t)
	if err != nil {
		return err
	}
	s.Type = schemaType

	nullable, ok := data["nullable"]
	if !ok || reflect.TypeOf(nullable) == nil {
		return errors.Join(ErrParseSchema, errors.New("missing nullable"))
	}
	switch reflect.TypeOf(nullable).Kind() {
	case reflect.Bool:
		s.Nullable = nullable.(bool)
	default:
		return errors.Join(ErrParseSchema, errors.New("nullable must be a bool"))
	}

	metadata, ok := data["metadata"]
	if !ok || reflect.TypeOf(metadata) == nil {
		return errors.Join(ErrParseSchema, errors.New("missing metadata"))
	}
	switch reflect.TypeOf(metadata).Kind() {
	case reflect.Map:
		if reflect.TypeOf(metadata).Key().Kind() != reflect.String {
			return errors.Join(ErrParseSchema, errors.New("metadata map key type must be string"))
		}
		s.Metadata = metadata.(map[string]interface{})
	default:
		return errors.Join(ErrParseSchema, errors.New("metadata must be a map"))
	}

	return nil
}

func unmarshalSchemaType(v interface{}) (SchemaDataType, error) {
	if v == nil {
		return nil, errors.Join(ErrParseSchema, errors.New("nil type"))
	}
	switch reflect.TypeOf(v).Kind() {
	case reflect.String:
		t := SchemaDataTypeName(v.(string))
		switch t {
		case String, Long, Integer, Short, Byte, Float, Double, Boolean, Binary, Date, Timestamp, Struct, Array, Map:
			return t, nil
		}
		return nil, errors.Join(ErrParseSchema, fmt.Errorf("unknown type %s", t))
	case reflect.Map:
		m := v.(map[string]interface{})
		t, ok := m["type"]
		if !ok {
			return nil, errors.Join(ErrParseSchema, errors.New("missing type"))
		}
		switch typedType := t.(type) {
		case string:
			switch typedType {
			case string(Array):
				a := SchemaTypeArray{Type: Array}

				containsNull, ok := m["containsNull"]
				if !ok {
					return nil, errors.Join(ErrParseSchema, errors.New("missing containsNull"))
				}
				elementType, ok := m["elementType"]
				if !ok {
					return nil, errors.Join(ErrParseSchema, errors.New("missing elementType"))
				}

				switch typedContainsNull := containsNull.(type) {
				case bool:
					a.ContainsNull = typedContainsNull
				case string:
					c, err := strconv.ParseBool(typedContainsNull)
					if err != nil {
						return nil, errors.Join(ErrParseSchema, errors.New("invalid containsNull"))
					}
					a.ContainsNull = c
				default:
					return nil, errors.Join(ErrParseSchema, errors.New("invalid containsNull"))
				}

				elementSchemaType, err := unmarshalSchemaType(elementType)
				if err != nil {
					return nil, err
				}
				a.ElementType = elementSchemaType

				return a, nil
			case string(Struct):
				s := SchemaTypeStruct{Type: Struct}

				fields, ok := m["fields"]
				if !ok {
					return nil, errors.Join(ErrParseSchema, errors.New("missing fields"))
				}
				switch reflect.TypeOf(fields).Kind() {
				case reflect.Slice:
					f := reflect.ValueOf(fields)
					s.Fields = make([]SchemaField, f.Len())
					for i := 0; i < f.Len(); i++ {
						entry := f.Index(i).Interface()
						switch reflect.TypeOf(entry).Kind() {
						case reflect.Map:
							if reflect.TypeOf(entry).Key().Kind() != reflect.String {
								return nil, errors.Join(ErrParseSchema, fmt.Errorf("invalid field definition %v", entry))
							}
							err := s.Fields[i].unmarshalSchemaField(entry.(map[string]interface{}))
							if err != nil {
								return nil, err
							}
						default:
							return nil, errors.Join(ErrParseSchema, fmt.Errorf("invalid field %v", entry))
						}
					}
				default:
					return nil, errors.Join(ErrParseSchema, fmt.Errorf("fields is not a slice %v", fields))
				}

				return s, nil
			case string(Map):
				s := SchemaTypeMap{Type: Map}

				valueContainsNull, ok := m["valueContainsNull"]
				if !ok {
					return nil, errors.Join(ErrParseSchema, errors.New("missing valueContainsNull"))
				}
				valueType, ok := m["valueType"]
				if !ok {
					return nil, errors.Join(ErrParseSchema, errors.New("missing valueType"))
				}
				keyType, ok := m["keyType"]
				if !ok {
					return nil, errors.Join(ErrParseSchema, errors.New("missing keyType"))
				}

				switch typedValueContainsNull := valueContainsNull.(type) {
				case bool:
					s.ValueContainsNull = typedValueContainsNull
				case string:
					c, err := strconv.ParseBool(typedValueContainsNull)
					if err != nil {
						return nil, errors.Join(ErrParseSchema, errors.New("invalid valueContainsNull"))
					}
					s.ValueContainsNull = c
				default:
					return nil, errors.Join(ErrParseSchema, errors.New("invalid valueContainsNull"))
				}

				valueSchemaType, err := unmarshalSchemaType(valueType)
				if err != nil {
					return nil, err
				}
				s.ValueType = valueSchemaType

				keySchemaType, err := unmarshalSchemaType(keyType)
				if err != nil {
					return nil, err
				}
				s.KeyType = keySchemaType

				return s, nil
			default:
				return nil, errors.Join(ErrParseSchema, fmt.Errorf("unknown type %s", typedType))
			}
		default:
			return nil, errors.Join(ErrParseSchema, fmt.Errorf("type is not a string %v", t))
		}
	default:
		return nil, errors.Join(ErrParseSchema, fmt.Errorf("invalid type %v", v))
	}
}

// GetSchema recursively walks over the given struct interface i and extracts SchemaTypeStruct StructFields using reflect
//
// This is not currently being used in production and results should be inspected before being used.
//
// TODO: Handle error cases where types are not compatible with spark types.
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
				field = SchemaField{Name: name, Type: SchemaTypeStruct{Type: Struct, Fields: recursiveStruct.Fields}, Nullable: nullable, Metadata: emptyMetaData}

			default:
				fieldPointer := reflect.New(structField.Type)
				fieldInterface := fieldPointer.Interface()
				schemaDataType := getFieldDataType(fieldInterface)
				field = SchemaField{Name: name, Type: schemaDataType, Nullable: nullable, Metadata: emptyMetaData}

			}
		}
		fields = append(fields, field)

	}
	return SchemaTypeStruct{Type: Struct, Fields: fields}
}

func getFieldDataType(t any) SchemaDataTypeName {
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
