package delta

import (
	"fmt"
	"reflect"
	"testing"
)

func ptr[T any](v T) *T {
	return &v
}
func TestRoundTripSimpleTypes(t *testing.T) {
	type SimpleTypes struct {
		AnInt    int     `parquet:"name=anInt"`
		AnInt8   int8    `parquet:"name=anInt8"`
		AnInt16  int16   `parquet:"name=anInt16"`
		AnInt32  int32   `parquet:"name=anInt32"`
		AnInt64  int64   `parquet:"name=anInt64"`
		AFloat32 float32 `parquet:"name=aFloat32"`
		AFloat64 float32 `parquet:"name=aFloat64"`
		AString  string  `parquet:"name=aString, converted=UTF8"`
		ABool    bool    `parquet:"name=aBool"`
		AUint    uint    `parquet:"name=aUint"`
		AUint8   uint8   `parquet:"name=aUint8"`
		AUint16  uint16  `parquet:"name=aUint16"`
		AUint32  uint32  `parquet:"name=aUint32"`
		AUint64  uint64  `parquet:"name=aUint64"`
	}

	simpleTypes := []SimpleTypes{
		{AnInt: 1, AnInt8: 2, AnInt16: 3, AnInt32: -50, AnInt64: 8223372036854775807, AFloat32: 3.1415, AFloat64: -1.1234, AString: "Good morning!", ABool: true, AUint: 12, AUint8: 7, AUint16: 5, AUint32: 32, AUint64: 6543},
		{AnInt: -42, AnInt8: 12, AnInt16: -34, AnInt32: 12345, AnInt64: -100000, AFloat32: 0, AFloat64: 1, AString: "", ABool: false, AUint: 912, AUint8: 4, AUint16: 55, AUint32: 23, AUint64: 3456},
	}
	expected := make([]any, len(simpleTypes))
	for i, s := range simpleTypes {
		expected[i] = s
	}

	bytes, err := writeStructsToParquetBytes(simpleTypes)
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	processSimpleStruct := func(result *SimpleTypes) error {
		if !reflect.DeepEqual(*result, expected[i]) {
			return fmt.Errorf("expected %v got %v", expected[i], *result)
		}
		i++
		return nil
	}

	err = readAndProcessStructsFromParquet(bytes, processSimpleStruct)
	if err != nil {
		t.Error(err)
	}

	type PointerTypes struct {
		AnInt    *int     `parquet:"name=anInt"`
		AnInt8   *int8    `parquet:"name=anInt8"`
		AnInt16  *int16   `parquet:"name=anInt16"`
		AnInt32  *int32   `parquet:"name=anInt32"`
		AnInt64  *int64   `parquet:"name=anInt64"`
		AFloat32 *float32 `parquet:"name=aFloat32"`
		AFloat64 *float32 `parquet:"name=aFloat64"`
		AString  *string  `parquet:"name=aString, converted=UTF8"`
		ABool    *bool    `parquet:"name=aBool"`
		AUint    *uint    `parquet:"name=aUint"`
		AUint8   *uint8   `parquet:"name=aUint8"`
		AUint16  *uint16  `parquet:"name=aUint16"`
		AUint32  *uint32  `parquet:"name=aUint32"`
		AUint64  *uint64  `parquet:"name=aUint64"`
	}
	simpleToPointer := func(s *SimpleTypes) PointerTypes {
		return PointerTypes{
			AnInt:    &s.AnInt,
			AnInt8:   &s.AnInt8,
			AnInt16:  &s.AnInt16,
			AnInt32:  &s.AnInt32,
			AnInt64:  &s.AnInt64,
			AFloat32: &s.AFloat32,
			AFloat64: &s.AFloat64,
			AString:  &s.AString,
			ABool:    &s.ABool,
			AUint:    &s.AUint,
			AUint8:   &s.AUint8,
			AUint16:  &s.AUint16,
			AUint32:  &s.AUint32,
			AUint64:  &s.AUint64,
		}
	}

	pointerTypes := make([]PointerTypes, len(simpleTypes))
	for i, s := range simpleTypes {
		pointerTypes[i] = simpleToPointer(&s)
		expected[i] = pointerTypes[i]
	}

	i = 0
	processPointerStruct := func(result *PointerTypes) error {
		if !reflect.DeepEqual(*result, expected[i]) {
			return fmt.Errorf("expected %v got %v", expected[i], *result)
		}
		i++
		return nil
	}
	bytes, err = writeStructsToParquetBytes(pointerTypes)
	if err != nil {
		t.Fatal(err)
	}
	err = readAndProcessStructsFromParquet(bytes, processPointerStruct)
	if err != nil {
		t.Error(err)
	}
}

func TestRoundTripNestedTypes(t *testing.T) {
	// Note that round tripping a nil map or slice (not a pointer) will fail on the compare,
	// because arrow will read it as an empty non-nil map or slice
	type NestedTypes2 struct {
		AnInt   int32  `parquet:"name=anInt"`
		AString string `parquet:"name=aString, converted=UTF8"`
	}
	type NestedTypes1 struct {
		AMap          map[int64]NestedTypes2 `parquet:"name=aMap"`
		ANestedTypes2 NestedTypes2           `parquet:"name=aNestedTypes2"`
	}
	type NestedTypes struct {
		AMap          map[string]string  `parquet:"name=aMap, keyconverted=UTF8, valueconverted=UTF8"`
		AList         []int32            `parquet:"name=aList"`
		AMapPtr       *map[string]string `parquet:"name=aMapPtr, keyconverted=UTF8, valueconverted=UTF8"`
		AListPtr      *[]int32           `parquet:"name=aListPtr"`
		ANestedTypes1 NestedTypes1       `parquet:"name=aNestedTypes1"`
		ANestedTypes2 *NestedTypes2      `parquet:"name=aNestedTypes2"`
		AListNested   []NestedTypes2     `parquet:"name=aListNested"`
	}

	nestedTypes2 := []NestedTypes2{
		{AnInt: 1, AString: "one"},
		{AnInt: 2, AString: "two"},
		{AnInt: 3, AString: "three"},
		{AnInt: -4, AString: "negative four"},
		{AnInt: 5, AString: ""},
	}

	nestedTypes1 := []NestedTypes1{
		// {ANestedTypes2: nestedTypes2[1]},
		// {ANestedTypes2: nestedTypes2[4]},
		// {ANestedTypes2: nestedTypes2[2]},
		{AMap: map[int64]NestedTypes2{1234: nestedTypes2[0], 567: nestedTypes2[1]}, ANestedTypes2: nestedTypes2[1]},
		{AMap: map[int64]NestedTypes2{89: nestedTypes2[3], 0: nestedTypes2[2]}, ANestedTypes2: nestedTypes2[4]},
		{AMap: map[int64]NestedTypes2{}, ANestedTypes2: nestedTypes2[2]},
	}

	nestedTypes := []NestedTypes{
		{
			AMap: map[string]string{
				"hello": "bye",
				"blue":  "green",
				"empty": "",
			},
			AList:         []int32{1, 2, 3, 4},
			AMapPtr:       ptr(map[string]string{"ok": "okay", "pencil": "mechanical"}),
			AListPtr:      nil,
			ANestedTypes1: nestedTypes1[2],
			ANestedTypes2: nil,
			AListNested:   []NestedTypes2{nestedTypes2[0], nestedTypes2[3]},
		},
		{
			AMap: map[string]string{
				"apple":  "banana",
				"coffee": "tea",
			},
			AList:         []int32{1, 1, 1},
			AMapPtr:       nil,
			AListPtr:      ptr([]int32{987, 654}),
			ANestedTypes1: nestedTypes1[1],
			ANestedTypes2: ptr(nestedTypes2[0]),
			AListNested:   []NestedTypes2{},
		},
		{
			AMap:          map[string]string{},
			AList:         []int32{},
			ANestedTypes1: nestedTypes1[2],
			AListNested:   []NestedTypes2{nestedTypes2[0]},
		},
	}
	expected := make([]any, len(nestedTypes))
	for i := range nestedTypes {
		expected[i] = nestedTypes[i]
	}
	i := 0
	processNestedStruct := func(result *NestedTypes) error {
		if !reflect.DeepEqual(*result, expected[i]) {
			return fmt.Errorf("expected %v got %v at index %d", expected[i], *result, i)
		}
		i++
		return nil
	}
	bytes, err := writeStructsToParquetBytes(nestedTypes)
	if err != nil {
		t.Fatal(err)
	}
	err = readAndProcessStructsFromParquet(bytes, processNestedStruct)
	if err != nil {
		t.Error(err)
	}
}
