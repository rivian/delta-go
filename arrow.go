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
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/decimal128"
	"github.com/apache/arrow/go/v13/arrow/float16"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/compress"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/pqarrow"
	"github.com/apache/arrow/go/v13/parquet/schema"
)

var (
	ErrorArrowConversion error = errors.New("error converting from arrow")
)

// Read helpers

func goStructFromArrowArrays(goStructs []reflect.Value, arrowArrays []arrow.Array, goNamePrefix string, goNameArrowIndexMap map[string]int) error {
	goType := goStructs[0].Type()
	for goType.Kind() == reflect.Pointer {
		goType = goType.Elem()
	}

	for row := 0; row < len(goStructs); row++ {
		structElem := goStructs[row]
		for structElem.Kind() == reflect.Pointer {
			structElem = structElem.Elem()
		}

		if goType.Kind() != reflect.Struct {
			return errors.Join(ErrorArrowConversion, fmt.Errorf("expected struct type but found %v", goType.Name()))
		}

		for i := 0; i < goType.NumField(); i++ {
			goFieldName := goNamePrefix + "." + goType.Field(i).Name
			arrowIndex, ok := goNameArrowIndexMap[goFieldName]
			if ok {
				arrowField := arrowArrays[arrowIndex]
				if arrowField.IsNull(row) {
					continue
				}
				goField := structElem.FieldByName(goType.Field(i).Name)
				elem := traversePointersAndGetValue(goField, false)
				err := goValueFromArrowArray(elem, arrowField, row, goFieldName, goNameArrowIndexMap)
				if err != nil {
					return err
				}
			}
		}
	}
	// return goStruct, nil
	return nil
}

func goValueFromArrowArray(goValue reflect.Value, arrowArray arrow.Array, arrayOffset int, goNamePrefix string, goNameArrowIndexMap map[string]int) error {
	goType := goValue.Type()
	for goType.Kind() == reflect.Pointer {
		goType = goType.Elem()
	}

	if arrowArray.IsNull(arrayOffset) {
		return nil
	}

	switch goType.Kind() {
	case reflect.Bool:
		switch typedArrowArray := arrowArray.(type) {
		case *array.Boolean:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem := traversePointersAndGetValue(goValue, true)
			elem.SetBool(arrowValue)
			return nil
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to bool", typedArrowArray.DataType().Name()))
		}
	case reflect.Float32, reflect.Float64:
		switch typedArrowArray := arrowArray.(type) {
		case *array.Float16:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem := traversePointersAndGetValue(goValue, true)
			elem.SetFloat(float64(arrowValue.Float32()))
			return nil
		case *array.Float32:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem := traversePointersAndGetValue(goValue, true)
			elem.SetFloat(float64(arrowValue))
			return nil
		case *array.Float64:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem := traversePointersAndGetValue(goValue, true)
			elem.SetFloat(arrowValue)
			return nil
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to float", typedArrowArray.DataType().Name()))
		}
	case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int8:
		switch typedArrowArray := arrowArray.(type) {
		case *array.Int16:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem := traversePointersAndGetValue(goValue, true)
			elem.SetInt(int64(arrowValue))
			return nil
		case *array.Int32:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem := traversePointersAndGetValue(goValue, true)
			elem.SetInt(int64(arrowValue))
			return nil
		case *array.Int64:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem := traversePointersAndGetValue(goValue, true)
			elem.SetInt(arrowValue)
			return nil
		case *array.Int8:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem := traversePointersAndGetValue(goValue, true)
			elem.SetInt(int64(arrowValue))
			return nil
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to int", typedArrowArray.DataType().Name()))
		}
	case reflect.String:
		switch typedArrowArray := arrowArray.(type) {
		case *array.String:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem := traversePointersAndGetValue(goValue, true)
			elem.SetString(arrowValue)
			return nil
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to string", typedArrowArray.DataType().Name()))
		}
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint8:
		switch typedArrowArray := arrowArray.(type) {
		case *array.Uint16:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem := traversePointersAndGetValue(goValue, true)
			elem.SetUint(uint64(arrowValue))
			return nil
		case *array.Uint32:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem := traversePointersAndGetValue(goValue, true)
			elem.SetUint(uint64(arrowValue))
			return nil
		case *array.Uint64:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem := traversePointersAndGetValue(goValue, true)
			elem.SetUint(arrowValue)
			return nil
		case *array.Uint8:
			arrowValue := typedArrowArray.Value(arrayOffset)
			elem := traversePointersAndGetValue(goValue, true)
			elem.SetUint(uint64(arrowValue))
			return nil
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to uint", typedArrowArray.DataType().Name()))
		}
	case reflect.Array, reflect.Slice:
		var start, end int64
		var values arrow.Array
		switch typedArrowArray := arrowArray.(type) {
		case *array.List:
			start, end = typedArrowArray.ValueOffsets(arrayOffset)
			values = typedArrowArray.ListValues()
		case *array.FixedSizeList:
			start, end = typedArrowArray.ValueOffsets(arrayOffset)
			values = typedArrowArray.ListValues()
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to array/slice", typedArrowArray.DataType().Name()))
		}
		listElem := traversePointersAndGetValue(goValue, true)
		for i := 0; i < int(end-start); i++ {
			entry := newReflectValueByType(goType.Elem())
			goValueFromArrowArray(entry, values, i+int(start), goNamePrefix, goNameArrowIndexMap)
			listElem.Set(reflect.Append(listElem, entry.Elem()))
		}
	case reflect.Map:
		switch typedArrowArray := arrowArray.(type) {
		case *array.Map:
			mapElem := traversePointersAndGetValue(goValue, true)
			// mapElemType := reflect.PointerTo(goType.Elem())
			keys := typedArrowArray.Keys()
			items := typedArrowArray.Items()
			var start, end int64
			start, end = typedArrowArray.ValueOffsets(arrayOffset)
			for i := 0; i < int(end-start); i++ {
				entry := newReflectValueByType(goType.Elem())
				goValueFromArrowArray(entry, items, i+int(start), goNamePrefix, goNameArrowIndexMap)
				key := newReflectValueByType(goType.Key())
				goValueFromArrowArray(key, keys, i+int(start), goNamePrefix, goNameArrowIndexMap)
				mapElem.SetMapIndex(key.Elem(), entry.Elem())
			}
		default:
			return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert %s to map", typedArrowArray.DataType().Name()))
		}
	case reflect.Struct:
		switch typedArrowArray := arrowArray.(type) {
		case *array.Struct:
			structElem := traversePointersAndGetValue(goValue, true)

			for i := 0; i < goType.NumField(); i++ {
				goFieldName := goNamePrefix + "." + goType.Field(i).Name
				arrowIndex, ok := goNameArrowIndexMap[goFieldName]
				if ok {
					arrowField := typedArrowArray.Field(arrowIndex)
					goField := structElem.FieldByName(goType.Field(i).Name)
					err := goValueFromArrowArray(goField, arrowField, arrayOffset, goFieldName, goNameArrowIndexMap)
					if err != nil {
						return err
					}
				}
			}
		}
	default:
		return errors.Join(ErrorArrowConversion, fmt.Errorf("unable to convert reflect type %s", goType.Kind().String()))
	}

	return nil
}

func newReflectValueByType(elemType reflect.Type) reflect.Value {
	var entry reflect.Value
	switch elemType.Kind() {
	case reflect.Map:
		entry = reflect.MakeMap(elemType)
	case reflect.Slice:
		entry = reflect.MakeSlice(elemType, 0, 10)
	case reflect.Pointer:
		entry = reflect.New(elemType).Elem()
	default:
		entry = reflect.New(elemType)
	}
	return entry
}

func traversePointersAndGetValue(goValue reflect.Value, setNil bool) reflect.Value {
	elem := goValue
	for elem.Type().Kind() == reflect.Pointer {
		if !elem.CanSet() {
			elem = elem.Elem()
			continue
		}
		elemType := elem.Type().Elem()
		switch elemType.Kind() {
		case reflect.Map:
			newMap := reflect.MakeMap(elemType)
			newMapPtr := reflect.New(elemType)
			newMapPtr.Elem().Set(newMap)
			elem.Set(newMapPtr)
		case reflect.Slice:
			newSlice := reflect.MakeSlice(elemType, 0, 10)
			newSlicePtr := reflect.New(elemType)
			newSlicePtr.Elem().Set(newSlice)
			elem.Set(newSlicePtr)
		default:
			newElem := newReflectValueByType(elemType)
			elem.Set(newElem)
		}
		if !elem.Elem().IsValid() {
			break
		}
		elem = elem.Elem()
	}
	if elem.Type().Kind() == reflect.Map && elem.IsNil() {
		newMap := newReflectValueByType(elem.Type())
		elem.Set(newMap)
	}
	if elem.Type().Kind() == reflect.Slice && elem.IsNil() {
		newSlice := newReflectValueByType(elem.Type())
		elem.Set(newSlice)
	}
	if elem.Type().Kind() == reflect.Pointer && elem.Elem().IsValid() {
		elem = elem.Elem()
	}
	return elem
}

func readAndProcessStructsFromParquet[T any](parquetBytes []byte, process func(*T) error) error {
	bytesReader := bytes.NewReader(parquetBytes)
	parquetReader, err := file.NewParquetReader(bytesReader)
	if err != nil {
		return err
	}

	defaultValue := new(T)

	parquetSchema := parquetReader.MetaData().Schema

	arrowSchema, err := pqarrow.FromParquet(parquetSchema, nil, nil)
	if err != nil {
		return err
	}
	arrowFieldList := arrowSchema.Fields()

	// Get mappings between struct member names and parquet/arrow names so we don't have to look them up repeatedly
	// during record assignments
	structFieldNameToArrowIndexMappings := make(map[string]int, 100)
	defaultType := reflect.TypeOf(defaultValue)
	err = getStructFieldNameToArrowIndexMappings(defaultType, "Root", arrowFieldList, structFieldNameToArrowIndexMappings)
	if err != nil {
		return err
	}
	fileReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 1, Parallel: false}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	readCols := []int{}
	for i := 0; i < fileReader.ParquetReader().MetaData().Schema.NumColumns(); i++ {
		readCols = append(readCols, i)
	}

	// Read a row group at a time
	for i := 0; i < fileReader.ParquetReader().NumRowGroups(); i++ {
		tbl, err := fileReader.ReadRowGroups(context.TODO(), readCols, []int{i})
		if err != nil {
			return err
		}
		defer tbl.Release()

		tableReader := array.NewTableReader(tbl, 0)
		defer tableReader.Release()

		for tableReader.Next() {
			// the record contains a batch of rows
			record := tableReader.Record()

			entries := make([]*T, record.NumRows())
			entryValues := make([]reflect.Value, record.NumRows())
			for j := int64(0); j < record.NumRows(); j++ {
				t := new(T)
				entries[j] = t
				entryValues[j] = reflect.ValueOf(t)
			}

			goStructFromArrowArrays(entryValues, record.Columns(), "Root", structFieldNameToArrowIndexMappings)
			for j := int64(0); j < record.NumRows(); j++ {
				err = process(entries[j])
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Write helpers

func writeStructsToParquetBytes[T any](values []T) ([]byte, error) {
	buf := new(bytes.Buffer)
	defaultValue := new(T)
	parquetSchema, err := schema.NewSchemaFromStruct(defaultValue)
	if err != nil {
		return nil, err
	}
	arrowSchema, err := pqarrow.FromParquet(parquetSchema, nil, nil)
	if err != nil {
		return nil, err
	}
	arrowFieldList := arrowSchema.Fields()

	// Get mappings between struct member names and parquet/arrow names so we don't have to look them up repeatedly
	// during record assignments
	structFieldNameToArrowIndexMappings := make(map[string]int, 100)
	valueType := reflect.TypeOf(defaultValue)
	err = getStructFieldNameToArrowIndexMappings(valueType, "Root", arrowFieldList, structFieldNameToArrowIndexMappings)
	if err != nil {
		return nil, err
	}

	recordBuilder := array.NewStructBuilder(memory.DefaultAllocator, arrow.StructOf(arrowFieldList...))
	recordBuilder.Resize(len(values))

	for _, record := range values {
		appendGoValueToArrowBuilder(reflect.ValueOf(record), recordBuilder, "Root", structFieldNameToArrowIndexMappings)
	}

	cols := make([]arrow.Column, 0, len(arrowFieldList))
	for idx, field := range arrowFieldList {
		arr := recordBuilder.FieldBuilder(idx).NewArray()
		defer arr.Release()
		chunked := arrow.NewChunked(field.Type, []arrow.Array{arr})
		defer chunked.Release()
		col := arrow.NewColumn(field, chunked)
		defer col.Release()
		cols = append(cols, *col)
	}

	tbl := array.NewTable(arrowSchema, cols, int64(len(values)))
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	err = pqarrow.WriteTable(tbl, buf, tbl.NumRows(), props, pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func appendGoValueToArrowBuilder(goValue reflect.Value, builder array.Builder, goNamePrefix string, goNameArrowIndexMap map[string]int) error {
	for goValue.Kind() == reflect.Ptr {
		if goValue.IsNil() {
			builder.AppendNull()
			return nil
		}
		goValue = goValue.Elem()
	}

	if !goValue.IsValid() {
		builder.AppendNull()
		return nil
	}

	if (goValue.Kind() == reflect.Map || goValue.Kind() == reflect.Slice) && goValue.IsNil() {
		builder.AppendNull()
		return nil
	}

	switch builder.Type().ID() {
	// Non-nested types
	case arrow.INT8:
		builder.(*array.Int8Builder).Append(int8(goValue.Int()))
	case arrow.INT16:
		builder.(*array.Int16Builder).Append(int16(goValue.Int()))
	case arrow.INT32:
		builder.(*array.Int32Builder).Append(int32(goValue.Int()))
	case arrow.INT64:
		builder.(*array.Int64Builder).Append(goValue.Int())
	case arrow.UINT8:
		builder.(*array.Uint8Builder).Append(uint8(goValue.Uint()))
	case arrow.UINT16:
		builder.(*array.Uint16Builder).Append(uint16(goValue.Uint()))
	case arrow.UINT32:
		builder.(*array.Uint32Builder).Append(uint32(goValue.Uint()))
	case arrow.UINT64:
		builder.(*array.Uint64Builder).Append(goValue.Uint())
	case arrow.BINARY:
		builder.(*array.BinaryBuilder).Append(goValue.Bytes())
	case arrow.BOOL:
		builder.(*array.BooleanBuilder).Append(goValue.Bool())
	case arrow.DATE32:
		// TODO
		builder.(*array.Date32Builder).Append(arrow.Date32(goValue.Int()))
	case arrow.DATE64:
		// TODO
		builder.(*array.Date32Builder).Append(arrow.Date32(goValue.Int()))
	case arrow.DECIMAL:
		switch goValue.Kind() {
		case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
			builder.(*array.Decimal128Builder).Append(decimal128.FromI64(goValue.Int()))
		case reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Uint:
			builder.(*array.Decimal128Builder).Append(decimal128.FromU64(goValue.Uint()))
			// TODO big int
		}
	case arrow.DURATION:
		builder.(*array.DurationBuilder).Append(arrow.Duration(goValue.Int()))
	case arrow.FIXED_SIZE_BINARY:
		builder.(*array.FixedSizeBinaryBuilder).Append(goValue.Bytes())
	case arrow.FLOAT16:
		builder.(*array.Float16Builder).Append(float16.New(float32(goValue.Float())))
	case arrow.FLOAT32:
		builder.(*array.Float32Builder).Append(float32(goValue.Float()))
	case arrow.FLOAT64:
		builder.(*array.Float64Builder).Append(goValue.Float())
	case arrow.INTERVAL_DAY_TIME:
		return errors.Join(ErrNotImplemented, ErrorGeneratingCheckpoint, errors.New("unsupported arrow type INTERVAL_DAY_TIME"))
	case arrow.INTERVAL_MONTHS:
		return errors.Join(ErrNotImplemented, ErrorGeneratingCheckpoint, errors.New("unsupported arrow type INTERVAL_MONTHS"))
	case arrow.INTERVAL_MONTH_DAY_NANO:
		return errors.Join(ErrNotImplemented, ErrorGeneratingCheckpoint, errors.New("unsupported arrow type INTERVAL_MONTH_DAY_NANO"))
	case arrow.NULL:
		builder.AppendNull()
	case arrow.RUN_END_ENCODED:
		return errors.Join(ErrNotImplemented, ErrorGeneratingCheckpoint, errors.New("unsupported arrow type RUN_END_ENCODED"))
	case arrow.STRING:
		builder.(*array.StringBuilder).Append(goValue.String())
	case arrow.TIME32:
		// TODO check this
		builder.(*array.Time32Builder).Append(arrow.Time32(goValue.Int()))
	case arrow.TIME64:
		// TODO check this
		builder.(*array.Time64Builder).Append(arrow.Time64(goValue.Int()))
	case arrow.TIMESTAMP:
		// TODO check this
		builder.(*array.TimestampBuilder).Append(arrow.Timestamp(goValue.Int()))
	// Nested types
	case arrow.DICTIONARY:
		return errors.Join(ErrNotImplemented, ErrorGeneratingCheckpoint, errors.New("unsupported arrow type DICTIONARY"))
	case arrow.FIXED_SIZE_LIST:
		listBuilder := builder.(*array.FixedSizeListBuilder)
		if goValue.Len() == 0 {
			listBuilder.AppendEmptyValue()
			break
		}
		listBuilder.ValueBuilder().Reserve(goValue.Len())
		for i := 0; i < goValue.Len(); i++ {
			listBuilder.Append(true)
			err := appendGoValueToArrowBuilder(goValue.Index(i), listBuilder.ValueBuilder(), goNamePrefix, goNameArrowIndexMap)
			if err != nil {
				return err
			}
		}
	case arrow.LIST:
		listBuilder := builder.(*array.ListBuilder)
		if goValue.Len() == 0 {
			listBuilder.AppendEmptyValue()
			break
		}
		listBuilder.ValueBuilder().Reserve(goValue.Len())
		listBuilder.Append(true)
		for i := 0; i < goValue.Len(); i++ {
			err := appendGoValueToArrowBuilder(goValue.Index(i), listBuilder.ValueBuilder(), goNamePrefix, goNameArrowIndexMap)
			if err != nil {
				return err
			}
		}
	case arrow.MAP:
		mapBuilder := builder.(*array.MapBuilder)
		if goValue.Len() == 0 {
			mapBuilder.AppendEmptyValue()
			break
		}
		mapBuilder.KeyBuilder().Reserve(goValue.Len())
		mapBuilder.ItemBuilder().Reserve(goValue.Len())
		mapBuilder.Append(true)
		for _, key := range goValue.MapKeys() {
			err := appendGoValueToArrowBuilder(key, mapBuilder.KeyBuilder(), "", goNameArrowIndexMap)
			if err != nil {
				return err
			}
			err = appendGoValueToArrowBuilder(goValue.MapIndex(key), mapBuilder.ItemBuilder(), goNamePrefix, goNameArrowIndexMap)
			if err != nil {
				return err
			}
		}

	case arrow.STRUCT:
		structBuilder := builder.(*array.StructBuilder)
		structBuilder.Append(true)
		for i := 0; i < goValue.NumField(); i++ {
			fieldName := goNamePrefix + "." + goValue.Type().Field(i).Name
			arrowIndex, ok := goNameArrowIndexMap[fieldName]
			if ok {
				err := appendGoValueToArrowBuilder(goValue.Field(i), structBuilder.FieldBuilder(arrowIndex), fieldName, goNameArrowIndexMap)
				if err != nil {
					return err
				}
			}
			// If not in the goNameArrowIndexMap, we don't want to try to append it
		}
	default:
		return errors.Join(ErrNotImplemented, ErrorGeneratingCheckpoint, fmt.Errorf("unsupported arrow type %s", builder.Type().Name()))
	}
	return nil
}

// Map go field names to parquet field names, and also to the arrow field index
func getStructFieldNameToArrowIndexMappings(goType reflect.Type, goNamePrefix string, arrowFields []arrow.Field, goNameArrowIndexMap map[string]int) error {
	for goType.Kind() == reflect.Pointer {
		goType = goType.Elem()
	}

	switch goType.Kind() {
	case reflect.Struct:
		for i := 0; i < goType.NumField(); i++ {
			var parquetName string
			field := goType.Field(i)
			tag := field.Tag
			if ptags, ok := tag.Lookup("parquet"); ok {
				if ptags != "-" {
				findNameTag:
					for _, tag := range strings.Split(strings.Replace(ptags, "\t", "", -1), ",") {
						tag = strings.TrimSpace(tag)
						kv := strings.SplitN(tag, "=", 2)
						key := strings.TrimSpace(strings.ToLower(kv[0]))
						value := strings.TrimSpace(kv[1])

						switch key {
						case "name":
							parquetName = value
							break findNameTag
						default:
							// nop
						}
					}
				}
			}
			if len(parquetName) > 0 {
				fieldName := goNamePrefix + "." + field.Name
				var arrowField arrow.Field
				found := false
				for arrowIndex := 0; arrowIndex < len(arrowFields); arrowIndex++ {
					if arrowFields[arrowIndex].Name == parquetName {
						goNameArrowIndexMap[fieldName] = arrowIndex
						arrowField = arrowFields[arrowIndex]
						found = true
						break
					}
				}
				if !found {
					return errors.Join(ErrorGeneratingCheckpoint, fmt.Errorf("unexpected schema conversion error: could not find %s in arrow fields", parquetName))
				}

				arrowStructMemberFields := arrowFieldsFromField(arrowField)
				if arrowStructMemberFields != nil {
					err := getStructFieldNameToArrowIndexMappings(field.Type, fieldName, arrowStructMemberFields, goNameArrowIndexMap)
					if err != nil {
						return err
					}
				}
			}
		}
	case reflect.Array, reflect.Slice, reflect.Map:
		// map: incoming is entries.
		field := goType.Elem()
		arrowField := arrowFields[0]
		arrowStructMemberFields := arrowFieldsFromField(arrowField)
		// map: fields are "key", "value". value fields are "anInt", "aString"
		if goType.Kind() == reflect.Map {
			// We need to get the map value
			arrowStructMemberFields = arrowFieldsFromField(arrowStructMemberFields[1])
		}
		if arrowStructMemberFields != nil {
			err := getStructFieldNameToArrowIndexMappings(field, goNamePrefix, arrowStructMemberFields, goNameArrowIndexMap)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func arrowFieldsFromField(arrowField arrow.Field) []arrow.Field {
	var arrowMemberFields []arrow.Field
	switch arrowField.Type.ID() {
	case arrow.STRUCT:
		arrowMemberFields = arrowField.Type.(*arrow.StructType).Fields()
	case arrow.LIST:
		arrowMemberFields = arrowField.Type.(*arrow.ListType).Fields()
	case arrow.MAP:
		arrowMemberFields = arrowField.Type.(*arrow.MapType).Fields()
	case arrow.FIXED_SIZE_LIST:
		arrowMemberFields = arrowField.Type.(*arrow.FixedSizeListType).Fields()
	}
	return arrowMemberFields
}
