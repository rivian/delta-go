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
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/iancoleman/strcase"
	"golang.org/x/exp/constraints"
)

var (
	ErrorActionJSONFormat error = errors.New("invalid format for action JSON")
	ErrorActionUnknown    error = errors.New("unknown action")
	ErrorActionConversion error = errors.New("error converting action")
)

// Delta log action that describes a parquet data file that is part of the table.
type Action interface {
	// Add | CommitInfo
}

type CommitInfo map[string]interface{}

// An Add action is typed to allow the stats_parsed and partitionValues_parsed fields to be written to checkpoints with
// the correct schema without using reflection.
// The Add variant is for a non-partitioned table; the PartitionValuesParsed field will be omitted.
// When writing a partitioned table checkpoint, the AddPartitioned variant below is used instead.
type Add[RowType any] struct {
	// A relative path, from the root of the table, to a file that should be added to the table
	Path *string `json:"path" parquet:"name=path, type=BYTE_ARRAY, convertedtype=UTF8"`
	// A map from partition column to value for this file
	// This field is required even without a partition.
	PartitionValues *map[string]string `json:"partitionValues" parquet:"name=partitionValues, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	// The size of this file in bytes
	Size *DeltaDataTypeLong `json:"size" parquet:"name=size, type=INT64"`
	// The time this file was created, as milliseconds since the epoch
	ModificationTime *DeltaDataTypeTimestamp `json:"modificationTime" parquet:"name=modificationTime, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MILLIS"`
	// When false the file must already be present in the table or the records in the added file
	// must be contained in one or more remove actions in the same version
	//
	// streaming queries that are tailing the transaction log can use this flag to skip actions
	// that would not affect the final results.
	DataChange *bool `json:"dataChange" parquet:"name=dataChange, type=BOOLEAN"`
	// Map containing metadata about this file
	Tags *map[string]string `json:"tags,omitempty" parquet:"name=tags, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	// Contains statistics (e.g., count, min/max values for columns) about the data in this file
	Stats *string `json:"stats" parquet:"name=stats, type=BYTE_ARRAY, convertedtype=UTF8"`
	// Contains statistics (e.g., count, min/max values for columns) about the data in this file in
	// raw parquet format. This field needs to be written when statistics are available and the
	// table property: delta.checkpoint.writeStatsAsStruct is set to true.
	//
	// This field is only available in add action records read from / written to checkpoints
	StatsParsed *GenericStats[RowType] `json:"-" parquet:"name=stats_parsed"`
}

// An Add action is typed to allow the stats_parsed and partitionValues_parsed fields to be written to checkpoints with
// the correct schema without using reflection.
// The AddPartitioned variant is for a partitioned table; the PartitionValuesParsed field will consist of the partition struct.
type AddPartitioned[RowType any, PartitionType any] struct {
	// A relative path, from the root of the table, to a file that should be added to the table
	Path *string `json:"path" parquet:"name=path, type=BYTE_ARRAY, convertedtype=UTF8"`
	// A map from partition column to value for this file
	PartitionValues *map[string]string `json:"partitionValues" parquet:"name=partitionValues, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	// The size of this file in bytes
	Size *DeltaDataTypeLong `json:"size" parquet:"name=size, type=INT64"`
	// The time this file was created, as milliseconds since the epoch
	ModificationTime *DeltaDataTypeTimestamp `json:"modificationTime" parquet:"name=modificationTime, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MILLIS"`
	// When false the file must already be present in the table or the records in the added file
	// must be contained in one or more remove actions in the same version
	//
	// streaming queries that are tailing the transaction log can use this flag to skip actions
	// that would not affect the final results.
	DataChange *bool `json:"dataChange" parquet:"name=dataChange, type=BOOLEAN"`
	// Map containing metadata about this file
	Tags *map[string]string `json:"tags,omitempty" parquet:"name=tags, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	// Contains statistics (e.g., count, min/max values for columns) about the data in this file
	Stats *string `json:"stats" parquet:"name=stats, type=BYTE_ARRAY, convertedtype=UTF8"`

	// TODO - parsed fields dropped after the parquet library switch.
	// This requires dropping writer version to 2.
	// We likely need to re-implement using reflection.

	// Partition values stored in raw parquet struct format. In this struct, the column names
	// correspond to the partition columns and the values are stored in their corresponding data
	// type. This is a required field when the table is partitioned and the table property
	// delta.checkpoint.writeStatsAsStruct is set to true. If the table is not partitioned, this
	// column can be omitted.
	//
	// This field is only available in add action records read from / written to checkpoints
	// PartitionValuesParsed PartitionType `json:"-" parquet:"name=partitionValues_parsed"`
	// Contains statistics (e.g., count, min/max values for columns) about the data in this file in
	// raw parquet format. This field needs to be written when statistics are available and the
	// table property: delta.checkpoint.writeStatsAsStruct is set to true.
	//
	// This field is only available in add action records read from / written to checkpoints
	// StatsParsed GenericStats[RowType] `json:"-" parquet:"name=stats_parsed"`
}

// TODO partition version, and parsed fields
func NewAddFromValue[RowType any](value reflect.Value) (*Add[RowType], error) {
	add := new(Add[RowType])
	var err error
	add.DataChange, err = boolFromValue(value.FieldByName("DataChange"))
	if err != nil {
		return nil, errors.Join(err, errors.New("unable to read DataChange for Add"))
	}
	if add.DataChange == nil {
		addDataChange := false
		add.DataChange = &addDataChange
	}
	if err != nil {
		return nil, errors.Join(err, errors.New("unable to read DataChange for Add"))
	}
	add.ModificationTime, err = deltaDataIntTypeFromValue[DeltaDataTypeTimestamp](value.FieldByName("ModificationTime"))
	if err != nil {
		return nil, errors.Join(err, errors.New("unable to read ModificationTime for Add"))
	}
	add.Path, err = stringFromValue(value.FieldByName("Path"))
	if err != nil {
		return nil, errors.Join(err, errors.New("unable to read Path for Add"))
	}

	add.Size, err = deltaDataIntTypeFromValue[DeltaDataTypeLong](value.FieldByName("Size"))
	if err != nil {
		return nil, errors.Join(err, errors.New("unable to read Size for Add"))
	}

	add.Stats, err = stringFromValue(value.FieldByName("Stats"))
	if err != nil {
		return nil, errors.Join(err, errors.New("unable to read Stats for Add"))
	}

	// TODO type checking map key/values
	partitionValues := value.FieldByName("PartitionValues")
	if partitionValues.IsValid() {
		partitionValuesMap := make(map[string]string, len(partitionValues.MapKeys()))
		add.PartitionValues = &partitionValuesMap
		for _, e := range partitionValues.MapKeys() {
			val := partitionValues.MapIndex(e)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			(*add.PartitionValues)[e.String()] = val.String()
		}
	} else {
		partitionValuesMap := make(map[string]string, 0)
		add.PartitionValues = &partitionValuesMap
	}

	tags := value.FieldByName("Tags")
	if tags.IsValid() {
		tagsMap := make(map[string]string, len(tags.MapKeys()))
		add.Tags = &tagsMap
		for _, e := range tags.MapKeys() {
			val := tags.MapIndex(e)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			(*add.Tags)[e.String()] = val.String()
		}
	} else {
		tagsMap := make(map[string]string, 0)
		add.Tags = &tagsMap
	}
	return add, nil
}

// Some reflect helper functions
func boolFromValue(value reflect.Value) (*bool, error) {
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return nil, nil
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.Bool {
		boolValue := value.Bool()
		return &boolValue, nil
	}
	return nil, ErrorActionConversion
}

func deltaDataIntTypeFromValue[DeltaIntType DeltaDataTypeInt | DeltaDataTypeLong | DeltaDataTypeTimestamp | DeltaDataTypeVersion | DeltaDataTypeDate](value reflect.Value) (*DeltaIntType, error) {
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return nil, nil
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.Int8 || value.Kind() == reflect.Int16 || value.Kind() == reflect.Int || value.Kind() == reflect.Int32 || value.Kind() == reflect.Int64 {
		typedValue := DeltaIntType(value.Int())
		return &typedValue, nil
	}

	return nil, ErrorActionConversion
}

func stringFromValue(value reflect.Value) (*string, error) {
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return nil, nil
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.String {
		stringValue := value.String()
		return &stringValue, nil
	}
	return nil, ErrorActionConversion
}

// / Convenience function to copy data from an Add to an AddPartitioned
func (addPartitioned *AddPartitioned[RowType, PartitionType]) fromAdd(add *Add[RowType]) {
	addPartitioned.DataChange = add.DataChange
	addPartitioned.ModificationTime = add.ModificationTime
	addPartitioned.PartitionValues = add.PartitionValues
	addPartitioned.Path = add.Path
	addPartitioned.Size = add.Size
	addPartitioned.Stats = add.Stats
	addPartitioned.Tags = add.Tags
}

// / Represents a tombstone (deleted file) in the Delta log.
// / This is a top-level action in Delta log entries.
type Remove struct {
	/// The path of the file that is removed from the table.
	Path *string `json:"path" parquet:"name=path, type=BYTE_ARRAY, convertedtype=UTF8"`
	/// The timestamp when the remove was added to table state.
	DeletionTimestamp *DeltaDataTypeTimestamp `json:"deletionTimestamp" parquet:"name=deletionTimestamp, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MILLIS"`
	/// Whether data is changed by the remove. A table optimize will report this as false for
	/// example, since it adds and removes files by combining many files into one.
	DataChange *bool `json:"dataChange" parquet:"name=dataChange, type=BOOLEAN"`
	/// When true the fields partitionValues, size, and tags are present
	///
	/// NOTE: Although it's defined as required in scala delta implementation, but some writes
	/// it's still nullable so we keep it as Option<> for compatibly.
	ExtendedFileMetadata *bool `json:"extendedFileMetadata" parquet:"name=extendedFileMetadata, type=BOOLEAN"`
	/// A map from partition column to value for this file.
	PartitionValues *map[string]string `json:"partitionValues" parquet:"name=partitionValues, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	/// Size of this file in bytes
	Size *DeltaDataTypeLong `json:"size" parquet:"name=size, type=INT64"`
	/// Map containing metadata about this file
	Tags *map[string]string `json:"tags"`
}

func NewRemoveFromValue(value reflect.Value) (*Remove, error) {
	remove := new(Remove)
	var err error
	remove.DataChange, err = boolFromValue(value.FieldByName("DataChange"))
	if err != nil {
		return nil, errors.Join(err, errors.New("unable to read DataChange for Remove"))
	}
	if remove.DataChange == nil {
		removeDataChange := false
		remove.DataChange = &removeDataChange
	}
	remove.DeletionTimestamp, err = deltaDataIntTypeFromValue[DeltaDataTypeTimestamp](value.FieldByName("DeletionTimestamp"))
	if err != nil {
		return nil, errors.Join(err, errors.New("unable to read DeletionTimestamp in Remove"))
	}
	remove.ExtendedFileMetadata, err = boolFromValue(value.FieldByName("ExtendedFileMetadata"))
	if err != nil {
		return nil, errors.Join(err, errors.New("unable to read ExtendedFileMetadata for Remove"))
	}
	if remove.ExtendedFileMetadata == nil {
		removeExtendedFileMetadata := false
		remove.ExtendedFileMetadata = &removeExtendedFileMetadata
	}
	// TODO type checking map key/values
	partitionValues := value.FieldByName("PartitionValues")
	if partitionValues.IsValid() {
		partitionValuesMap := make(map[string]string, len(partitionValues.MapKeys()))
		remove.PartitionValues = &partitionValuesMap
		for _, e := range partitionValues.MapKeys() {
			val := partitionValues.MapIndex(e)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			(*remove.PartitionValues)[e.String()] = val.String()
		}
	} else {
		partitionValuesMap := make(map[string]string, 0)
		remove.PartitionValues = &partitionValuesMap
	}
	remove.Path, err = stringFromValue(value.FieldByName("Path"))
	if err != nil {
		return nil, errors.Join(err, errors.New("unable to read Path in Remove"))
	}

	remove.Size, err = deltaDataIntTypeFromValue[DeltaDataTypeLong](value.FieldByName("Size"))
	if err != nil {
		return nil, errors.Join(err, errors.New("unable to read Size in Remove"))
	}
	tags := value.FieldByName("Tags")
	if tags.IsValid() {
		tagsMap := make(map[string]string, len(tags.MapKeys()))
		remove.Tags = &tagsMap
		for _, e := range tags.MapKeys() {
			val := tags.MapIndex(e)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			(*remove.Tags)[e.String()] = val.String()
		}
	} else {
		tagsMap := make(map[string]string, 0)
		remove.Tags = &tagsMap
	}
	return remove, nil
}

// Describes the data format of files in the table.
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#format-specification
type Format struct {
	/// Name of the encoding for files in this table.
	// Default: "parquet"
	Provider *string `json:"provider" parquet:"name=provider, type=BYTE_ARRAY, convertedtype=UTF8"`
	/// A map containing configuration options for the format.
	// Default: {}
	Options *map[string]string `json:"options" parquet:"name=options, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

func NewFormatFromValue(value reflect.Value) (*Format, error) {
	format := new(Format)

	var err error
	format.Provider, err = stringFromValue(value.FieldByName("Provider"))
	if err != nil {
		return nil, err
	}
	options := value.FieldByName("Options")
	if options.IsValid() {
		optionsMap := make(map[string]string, len(options.MapKeys()))
		format.Options = &optionsMap
		for _, e := range options.MapKeys() {
			val := options.MapIndex(e)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			(*format.Options)[e.String()] = val.String()
		}
	} else {
		optionsMap := make(map[string]string, 0)
		format.Options = &optionsMap
	}

	return format, nil
}

// NewFormat provides the correct default format options as of Delta Lake 0.3.0
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#format-specification
// As of Delta Lake 0.3.0, user-facing APIs only allow the creation of tables where format = 'parquet' and options = {}.
func (format *Format) Default() Format {
	provider := "parquet"
	format.Provider = &provider
	optionsMap := make(map[string]string)
	format.Options = &optionsMap
	return *format
}

// / Action that describes the metadata of the table.
// / This is a top-level action in Delta log entries.
type MetaData struct {
	/// Unique identifier for this table
	Id uuid.UUID `json:"id"`
	/// Parquet library cannot import to UUID
	IdAsString *string `json:"-" parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8"`
	/// User-provided identifier for this table
	Name *string `json:"name" parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	/// User-provided description for this table
	Description *string `json:"description" parquet:"name=description, type=BYTE_ARRAY, convertedtype=UTF8"`
	/// Specification of the encoding for the files stored in the table
	Format *Format `json:"format" parquet:"name=format"`
	/// Schema of the table
	SchemaString *string `json:"schemaString" parquet:"name=schemaString, type=BYTE_ARRAY, convertedtype=UTF8"`
	/// An array containing the names of columns by which the data should be partitioned
	PartitionColumns *[]string `json:"partitionColumns" parquet:"name=partitionColumns, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	/// A map containing configuration options for the table
	Configuration *map[string]string `json:"configuration" parquet:"name=configuration, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	/// The time when this metadata action is created, in milliseconds since the Unix epoch
	CreatedTime *DeltaDataTypeTimestamp `json:"createdTime" parquet:"name=createdTime, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MILLIS"`
}

func NewMetadataFromValue(value reflect.Value) (*MetaData, error) {
	metadata := new(MetaData)
	var err error
	metadata.IdAsString, err = stringFromValue(value.FieldByName("Id"))
	if err != nil {
		return nil, err
	}
	if metadata.IdAsString != nil {
		id, err := uuid.Parse(*metadata.IdAsString)
		if err != nil {
			return nil, err
		}
		metadata.Id = id

	}

	metadata.Name, err = stringFromValue(value.FieldByName("Name"))
	if err != nil {
		return nil, err
	}
	metadata.Description, err = stringFromValue(value.FieldByName("Description"))
	if err != nil {
		return nil, err
	}
	metadata.Format, err = NewFormatFromValue(value.FieldByName("Format").Elem())
	if err != nil {
		return nil, err
	}
	metadata.SchemaString, err = stringFromValue(value.FieldByName("SchemaString"))
	if err != nil {
		return nil, err
	}

	partitionColumns := value.FieldByName("PartitionColumns")
	if partitionColumns.IsValid() {
		partitionColumnsMap := make([]string, partitionColumns.Len())
		metadata.PartitionColumns = &partitionColumnsMap
		for i := 0; i < partitionColumns.Len(); i++ {
			val := partitionColumns.Index(i)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			(*metadata.PartitionColumns)[i] = val.String()
		}
	} else {
		partitionColumnsMap := make([]string, 0)
		metadata.PartitionColumns = &partitionColumnsMap
	}

	configuration := value.FieldByName("Configuration")
	if configuration.IsValid() {
		configurationMap := make(map[string]string, len(configuration.MapKeys()))
		metadata.Configuration = &configurationMap
		for _, e := range configuration.MapKeys() {
			val := configuration.MapIndex(e)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			(*metadata.Configuration)[e.String()] = val.String()
		}
	} else {
		configurationMap := make(map[string]string, 0)
		metadata.Configuration = &configurationMap
	}

	metadata.CreatedTime, err = deltaDataIntTypeFromValue[DeltaDataTypeTimestamp](value.FieldByName("CreatedTime"))
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

// MetaData.ToDeltaTableMetaData() converts a MetaData to DeltaTableMetaData
// Internally, it converts the schema from a string.
func (md *MetaData) ToDeltaTableMetaData() (DeltaTableMetaData, error) {
	var err error
	schema, err := md.GetSchema()

	// Manually convert IdAsString to UUID if required
	id := md.Id
	if md.Id == uuid.Nil && md.IdAsString != nil && len(*md.IdAsString) > 0 {
		id, err = uuid.Parse(*md.IdAsString)
		if err != nil {
			return DeltaTableMetaData{}, err
		}
	}

	var partitionColumns []string
	if md.PartitionColumns != nil {
		partitionColumns = *md.PartitionColumns
	}
	var configuration map[string]string
	if md.Configuration != nil {
		configuration = *md.Configuration
	}

	dtmd := DeltaTableMetaData{
		Id:               id,
		Schema:           schema,
		PartitionColumns: partitionColumns,
		Configuration:    configuration,
	}
	if md.Name != nil {
		dtmd.Name = *md.Name
	}
	if md.Description != nil {
		dtmd.Description = *md.Description
	}
	if md.Format != nil {
		dtmd.Format = *md.Format
	}
	if md.CreatedTime != nil {
		dtmd.CreatedTime = time.UnixMilli(int64(*md.CreatedTime))
	}
	return dtmd, err
}

// / Action used by streaming systems to track progress using application-specific versions to
// / enable idempotency.
type Txn struct {
	/// A unique identifier for the application performing the transaction.
	AppId *string `json:"appId" parquet:"name=appId, type=BYTE_ARRAY, convertedtype=UTF8"`
	/// An application-specific numeric identifier for this transaction.
	Version *DeltaDataTypeVersion `json:"version" parquet:"name=version, type=INT64"`
	/// The time when this transaction action was created in milliseconds since the Unix epoch.
	LastUpdated *DeltaDataTypeTimestamp `json:"-" parquet:"name=lastUpdated, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MILLIS"`
}

func NewTxnFromValue(value reflect.Value) (*Txn, error) {
	txn := new(Txn)
	var err error

	txn.AppId, err = stringFromValue(value.FieldByName("AppId"))
	if err != nil {
		return nil, err
	}
	txn.Version, err = deltaDataIntTypeFromValue[DeltaDataTypeVersion](value.FieldByName("Version"))
	if err != nil {
		return nil, err
	}
	txn.LastUpdated, err = deltaDataIntTypeFromValue[DeltaDataTypeTimestamp](value.FieldByName("LastUpdated"))
	if err != nil {
		return nil, err
	}
	return txn, nil
}

// / Action used to increase the version of the Delta protocol required to read or write to the
// / table.
type Protocol struct {
	/// Minimum version of the Delta read protocol a client must implement to correctly read the
	/// table.
	MinReaderVersion *DeltaDataTypeInt `json:"minReaderVersion" parquet:"name=minReaderVersion, type=INT32"`
	/// Minimum version of the Delta write protocol a client must implement to correctly read the
	/// table.
	MinWriterVersion *DeltaDataTypeInt `json:"minWriterVersion" parquet:"name=minWriterVersion, type=INT32"`
}

// / Default protocol versions are currently 1, 1 as we do not support any more advanced features yet
func (protocol *Protocol) Default() Protocol {
	minReaderVersion := DeltaDataTypeInt(1)
	minWriterVersion := DeltaDataTypeInt(1)
	protocol.MinReaderVersion = &minReaderVersion
	protocol.MinWriterVersion = &minWriterVersion
	return *protocol
}

func NewProtocolFromValue(value reflect.Value) (*Protocol, error) {
	protocol := new(Protocol)
	var err error
	protocol.MinReaderVersion, err = deltaDataIntTypeFromValue[DeltaDataTypeInt](value.FieldByName("MinReaderVersion"))
	if err != nil {
		return nil, err
	}
	protocol.MinWriterVersion, err = deltaDataIntTypeFromValue[DeltaDataTypeInt](value.FieldByName("MinWriterVersion"))
	if err != nil {
		return nil, err
	}
	return protocol, nil
}

type Cdc struct {
	/// A relative path, from the root of the table, or an
	/// absolute path to a CDC file
	Path *string `json:"path" parquet:"name=path, type=BYTE_ARRAY, convertedtype=UTF8"`
	/// The size of this file in bytes
	Size *DeltaDataTypeLong `json:"size" parquet:"name=size, type=INT64"`
	/// A map from partition column to value for this file
	PartitionValues *map[string]string `json:"partitionValues"`
	/// Should always be set to false because they do not change the underlying data of the table
	DataChange *bool `json:"dataChange" parquet:"name=dataChange, type=BOOLEAN"`
	/// Map containing metadata about this file
	Tags *map[string]string `json:"tags,omitempty"`
}

func logEntryFromAction[RowType any, PartitionType any](action Action) ([]byte, error) {
	var log []byte
	m := make(map[string]any)

	var err error
	switch action.(type) {
	//TODO: Add errors for missing or null values that are not allowed by the delta protocol
	//https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions
	case Remove, CommitInfo, MetaData, Protocol, Txn:
		// wrap the action data in a camelCase of the action type
		key := strcase.ToLowerCamel(reflect.TypeOf(action).Name())
		m[key] = action
		log, err = json.Marshal(m)
	case *Remove, *CommitInfo, *MetaData, *Protocol, *Txn:
		key := strcase.ToLowerCamel(reflect.ValueOf(action).Elem().Type().Name())
		m[key] = action
		log, err = json.Marshal(m)
	case AddPartitioned[RowType, PartitionType], *AddPartitioned[RowType, PartitionType]:
		key := string(AddActionKey)
		m[key] = action
		log, err = json.Marshal(m)
	default:
		log, err = json.Marshal(action)
	}
	if err != nil {
		return log, err
	}

	return log, nil
}

func LogEntryFromActions[RowType any, PartitionType any](actions []Action) ([]byte, error) {
	var jsons [][]byte

	for _, action := range actions {
		j, err := logEntryFromAction[RowType, PartitionType](action)
		jsons = append(jsons, j)
		if err != nil {
			return bytes.Join(jsons, []byte("\n")), err
		}
	}

	return bytes.Join(jsons, []byte("\n")), nil
}

type ActionKey string

const (
	AddActionKey         ActionKey = "add"
	RemoveActionKey      ActionKey = "remove"
	CommitInfoActionKey  ActionKey = "commitInfo"
	ProtocolActionKey    ActionKey = "protocol"
	MetaDataActionKey    ActionKey = "metaData"
	FormatActionKey      ActionKey = "format"
	TransactionActionKey ActionKey = "txn"
	CDCActionKey         ActionKey = "cdc"
)

// / Retrieve an action from a log entry
func actionFromLogEntry[RowType any, PartitionType any](unstructuredResult map[string]json.RawMessage) (Action, error) {
	if len(unstructuredResult) != 1 {
		return nil, errors.Join(ErrorActionJSONFormat, errors.New("log entry JSON must have one value"))
	}

	var action Action
	var marshalledAction json.RawMessage
	var actionFound bool

	if marshalledAction, actionFound = unstructuredResult[string(AddActionKey)]; actionFound {
		action = new(AddPartitioned[RowType, PartitionType])
	} else if marshalledAction, actionFound = unstructuredResult[string(RemoveActionKey)]; actionFound {
		action = new(Remove)
	} else if marshalledAction, actionFound = unstructuredResult[string(CommitInfoActionKey)]; actionFound {
		action = new(CommitInfo)
	} else if marshalledAction, actionFound = unstructuredResult[string(ProtocolActionKey)]; actionFound {
		action = new(Protocol)
	} else if marshalledAction, actionFound = unstructuredResult[string(MetaDataActionKey)]; actionFound {
		action = new(MetaData)
	} else if marshalledAction, actionFound = unstructuredResult[string(FormatActionKey)]; actionFound {
		action = new(Format)
	} else if marshalledAction, actionFound = unstructuredResult[string(TransactionActionKey)]; actionFound {
		action = new(Txn)
	} else if _, actionFound = unstructuredResult[string(CDCActionKey)]; actionFound {
		return nil, ErrorCDCNotSupported
	} else {
		return nil, ErrorActionUnknown
	}

	err := json.Unmarshal(marshalledAction, action)
	if err != nil {
		return nil, errors.Join(ErrorActionJSONFormat, err)
	}

	return action, nil
}

// / Retrieve all actions in this log
func ActionsFromLogEntries[RowType any, PartitionType any](logEntries []byte) ([]Action, error) {
	lines := bytes.Split(logEntries, []byte("\n"))
	actions := make([]Action, 0, len(lines))
	for _, currentLine := range lines {
		if len(currentLine) == 0 {
			continue
		}
		var unstructuredResult map[string]json.RawMessage
		err := json.Unmarshal(currentLine, &unstructuredResult)
		if err != nil {
			return nil, errors.Join(ErrorActionJSONFormat, err)
		}
		action, err := actionFromLogEntry[RowType, PartitionType](unstructuredResult)
		if err != nil {
			return nil, err
		}
		actions = append(actions, action)
	}
	return actions, nil
}

// Returns the table schema from the embedded schema string contained within the metadata
// action.
func (m *MetaData) GetSchema() (Schema, error) {
	var schema Schema
	err := json.Unmarshal([]byte(*m.SchemaString), &schema)
	return schema, err
}

// /// Operation performed when creating a new log entry with one or more actions.
// /// This is a key element of the `CommitInfo` action.
// #[allow(clippy::large_enum_variant)]
// #[derive(Serialize, Deserialize, Debug, Clone)]
// #[serde(rename_all = "camelCase")]
type DeltaOperation interface {
	GetCommitInfo() CommitInfo
}

// / Represents a Delta `Create` operation.
// / Would usually only create the table, if also data is written,
// / a `Write` operations is more appropriate
type Create struct {
	/// The save mode used during the create.
	Mode SaveMode `json:"mode"`
	/// The storage location of the new table
	Location string `json:"location"`
	/// The min reader and writer protocol versions of the table
	Protocol Protocol
	/// Metadata associated with the new table
	MetaData DeltaTableMetaData
}

func (op Create) GetCommitInfo() CommitInfo {
	commitInfo := make(CommitInfo)

	operation := "delta-go.Create"
	commitInfo["operation"] = operation
	commitInfo["operationParameters"] = op

	return commitInfo
}

// / Represents a Delta `Write` operation.
// / Write operations will typically only include `Add` actions.
type Write struct {
	/// The save mode used during the write.
	Mode SaveMode `json:"mode"`
	/// The columns the write is partitioned by.
	PartitionBy []string `json:"partitionBy"`
	/// The predicate used during the write.
	Predicate []string `json:"predicate"`
}

func (op Write) GetCommitInfo() CommitInfo {
	commitInfo := make(CommitInfo)

	operation := "delta-go.Write"
	commitInfo["operation"] = operation

	// convert PartitionBy to JSON string, return "[]" if empty
	partitionByJSON, _ := json.Marshal(op.PartitionBy)
	if len(op.PartitionBy) == 0 {
		partitionByJSON = []byte("[]")
	}

	// convert PartitionBy to JSON string, return "[]" if empty
	predicateJSON, _ := json.Marshal(op.Predicate)
	if len(op.Predicate) == 0 {
		predicateJSON = []byte("[]")
	}

	// add operation parameters to map
	operationParams := make(map[string]interface{})
	operationParams["mode"] = op.Mode
	operationParams["partitionBy"] = string(partitionByJSON)
	operationParams["predicate"] = string(predicateJSON)

	// add operation parameters map to commitInfo
	commitInfo["operationParameters"] = operationParams

	return commitInfo
}

// / Represents a Delta `StreamingUpdate` operation.
type StreamingUpdate struct {
	/// The output mode the streaming writer is using.
	OutputMode OutputMode
	/// The query id of the streaming writer.
	QueryId string
	/// The epoch id of the written micro-batch.
	EpochId int64
}

// / The SaveMode used when performing a DeltaOperation
type SaveMode string

const (
	/// Files will be appended to the target location.
	Append SaveMode = "Append"
	/// The target location will be overwritten.
	Overwrite SaveMode = "Overwrite"
	/// If files exist for the target, the operation must fail.
	ErrorIfExists SaveMode = "ErrorIfExists"
	/// If files exist for the target, the operation must not proceed or change any data.
	Ignore SaveMode = "Ignore"
)

// / The OutputMode used in streaming operations.
type OutputMode string

const (
	/// Only new rows will be written when new data is available.
	AppendOutputMode OutputMode = "Append"
	/// The full output (all rows) will be written whenever new data is available.
	Complete OutputMode = "Complete"
	/// Only rows with updates will be written when new or changed data is available.
	Update OutputMode = "Update"
)

type Stats struct {
	NumRecords  int64            `json:"numRecords" parquet:"name=numRecords, type=INT64"`
	TightBounds bool             `json:"tightBounds" parquet:"name=tightBounds, type=BOOLEAN"`
	MinValues   map[string]any   `json:"minValues" parquet:"name=minValues, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8"`
	MaxValues   map[string]any   `json:"maxValues" parquet:"name=maxValues, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8"`
	NullCount   map[string]int64 `json:"nullCount" parquet:"name=nullCount, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT64"`
}

type GenericStats[RowType any] struct {
	NumRecords  int64            `json:"numRecords" parquet:"name=numRecords, type=INT64"`
	TightBounds bool             `json:"tightBounds" parquet:"name=tightBounds, type=BOOLEAN"`
	MinValues   RowType          `json:"minValues" parquet:"name=minValues"`
	MaxValues   RowType          `json:"maxValues" parquet:"name=maxValues"`
	NullCount   map[string]int64 `json:"nullCount" parquet:"name=nullCount, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT64"`
}

func (s *Stats) Json() []byte {
	b, _ := json.Marshal(s)
	return b
}

func StatsFromJson(b []byte) (*Stats, error) {
	s := new(Stats)
	var err error
	if len(b) > 0 {
		err = json.Unmarshal(b, s)
	}
	return s, err
}

// / Convert untyped Stats object to a GenericStats object
func statsAsGenericStats[RowType any](s *Stats) (*GenericStats[RowType], error) {
	genericStats := new(GenericStats[RowType])
	genericStats.NumRecords = s.NumRecords
	genericStats.TightBounds = s.TightBounds
	genericStats.NullCount = s.NullCount

	// Convert min and max values via JSON rather than setting up the reflection manually
	b, err := json.Marshal(s.MinValues)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &genericStats.MinValues)
	if err != nil {
		return nil, err
	}

	b, err = json.Marshal(s.MaxValues)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &genericStats.MaxValues)
	if err != nil {
		return nil, err
	}

	return genericStats, nil
}

// UpdateStats computes Stats.NullCount, Stats.MinValues, Stats.MaxValues for a given k,v struct property
// the struct property is passed in as a pointer to ensure that it can be evaluated as nil[NULL]
// TODO Handle struct types
func UpdateStats[T constraints.Ordered](s *Stats, k string, vpt *T) {

	var v T
	if vpt == nil {
		if s.NullCount == nil {
			s.NullCount = make(map[string]int64)
		}
		s.NullCount[k]++
		//Value is nil, skip applying MinValues, MaxValues
		return
	} else {
		v = *vpt
	}
	if priorMin, hasPriorMin := s.MinValues[k]; hasPriorMin {
		if v < priorMin.(T) {
			s.MinValues[k] = v
		}
	} else {
		if s.MinValues == nil {
			s.MinValues = make(map[string]any)
		}
		s.MinValues[k] = v
	}

	if priorMax, hasPriorMax := s.MaxValues[k]; hasPriorMax {
		if v > priorMax.(T) {
			s.MaxValues[k] = v
		}
	} else {
		if s.MaxValues == nil {
			s.MaxValues = make(map[string]any)

		}
		s.MaxValues[k] = v

	}

}
