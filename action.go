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
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/iancoleman/strcase"
	"golang.org/x/exp/constraints"
)

// Delta log action that describes a parquet data file that is part of the table.
type Action interface {
	// Add | CommitInfo
}

type CommitInfo map[string]interface{}

type Add struct {
	// A relative path, from the root of the table, to a file that should be added to the table
	Path string `json:"path"`
	// The size of this file in bytes
	Size DeltaDataTypeLong `json:"size"`
	// A map from partition column to value for this file
	PartitionValues map[string]string `json:"partitionValues"`
	// // Partition values stored in raw parquet struct format. In this struct, the column names
	// // correspond to the partition columns and the values are stored in their corresponding data
	// // type. This is a required field when the table is partitioned and the table property
	// // delta.checkpoint.writeStatsAsStruct is set to true. If the table is not partitioned, this
	// // column can be omitted.
	// //
	// // This field is only available in add action records read from checkpoints
	// #[cfg(feature = "parquet")]
	// #[serde(skip_serializing, skip_deserializing)]
	// PartitionValuesParsed: Option<parquet::record::Row>,
	// // Partition values stored in raw parquet struct format. In this struct, the column names
	// // correspond to the partition columns and the values are stored in their corresponding data
	// // type. This is a required field when the table is partitioned and the table property
	// // delta.checkpoint.writeStatsAsStruct is set to true. If the table is not partitioned, this
	// // column can be omitted.
	// //
	// // This field is only available in add action records read from checkpoints
	// #[cfg(feature = "parquet2")]
	// #[serde(skip_serializing, skip_deserializing)]
	// pub partition_values_parsed: Option<String>,
	// // The time this file was created, as milliseconds since the epoch
	ModificationTime DeltaDataTypeTimestamp `json:"modificationTime"`
	// When false the file must already be present in the table or the records in the added file
	// must be contained in one or more remove actions in the same version
	//
	// streaming queries that are tailing the transaction log can use this flag to skip actions
	// that would not affect the final results.
	DataChange bool `json:"dataChange"`
	// Contains statistics (e.g., count, min/max values for columns) about the data in this file
	Stats string `json:"stats"`
	// Contains statistics (e.g., count, min/max values for columns) about the data in this file in
	// raw parquet format. This field needs to be written when statistics are available and the
	// table property: delta.checkpoint.writeStatsAsStruct is set to true.
	//
	// // This field is only available in add action records read from checkpoints
	// #[cfg(feature = "parquet")]
	// #[serde(skip_serializing, skip_deserializing)]
	// pub stats_parsed: Option<parquet::record::Row>,
	// // Contains statistics (e.g., count, min/max values for columns) about the data in this file in
	// // raw parquet format. This field needs to be written when statistics are available and the
	// // table property: delta.checkpoint.writeStatsAsStruct is set to true.
	// //
	// // This field is only available in add action records read from checkpoints
	// #[cfg(feature = "parquet2")]
	// #[serde(skip_serializing, skip_deserializing)]
	// pub stats_parsed: Option<String>,
	// Map containing metadata about this file
	Tags map[string]string `json:"tags,omitempty"`
}

// / Represents a tombstone (deleted file) in the Delta log.
// / This is a top-level action in Delta log entries.
type Remove struct {
	/// The path of the file that is removed from the table.
	Path string `json:"path"`
	/// The timestamp when the remove was added to table state.
	DeletionTimestamp DeltaDataTypeTimestamp `json:"deletionTimestamp"`
	/// Whether data is changed by the remove. A table optimize will report this as false for
	/// example, since it adds and removes files by combining many files into one.
	DataChange bool `json:"dataChange"`
	/// When true the fields partitionValues, size, and tags are present
	///
	/// NOTE: Although it's defined as required in scala delta implementation, but some writes
	/// it's still nullable so we keep it as Option<> for compatibly.
	ExtendedFileMetadata bool `json:"extendedFileMetadata"`
	/// A map from partition column to value for this file.
	PartitionValues map[string]string `json:"partitionValues"`
	/// Size of this file in bytes
	Size DeltaDataTypeLong `json:"size"`
	/// Map containing metadata about this file
	Tags map[string]string `json:"tags"`
}

// Describes the data format of files in the table.
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#format-specification
type Format struct {
	/// Name of the encoding for files in this table.
	// Default: "parquet"
	Provider string `json:"provider"`
	/// A map containing configuration options for the format.
	// Default: {}
	Options map[string]string `json:"options"`
}

// NewFormat provides the correct default format options as of Delta Lake 0.3.0
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#format-specification
// As of Delta Lake 0.3.0, user-facing APIs only allow the creation of tables where format = 'parquet' and options = {}.
func (format *Format) Default() Format {
	format.Provider = "parquet"
	format.Options = make(map[string]string)
	return *format
}

// / Action that describes the metadata of the table.
// / This is a top-level action in Delta log entries.
type MetaData struct {
	/// Unique identifier for this table
	Id uuid.UUID `json:"id"`
	/// User-provided identifier for this table
	Name string `json:"name"`
	/// User-provided description for this table
	Description string `json:"description"`
	/// Specification of the encoding for the files stored in the table
	Format Format `json:"format"`
	/// Schema of the table
	SchemaString string `json:"schemaString"`
	/// An array containing the names of columns by which the data should be partitioned
	PartitionColumns []string `json:"partitionColumns"`
	/// The time when this metadata action is created, in milliseconds since the Unix epoch
	CreatedTime int64 `json:"createdTime"`
	/// A map containing configuration options for the table
	Configuration map[string]string `json:"configuration"`
}

// MetaData.ToDeltaTableMetaData() converts a MetaData to DeltaTableMetaData
// Internally, it converts the schema from a string.
func (md *MetaData) ToDeltaTableMetaData() (DeltaTableMetaData, error) {
	schema, err := md.GetSchema()
	dtmd := DeltaTableMetaData{
		Id:               md.Id,
		Name:             md.Name,
		Description:      md.Description,
		Format:           md.Format,
		Schema:           schema,
		PartitionColumns: md.PartitionColumns,
		CreatedTime:      time.UnixMilli(md.CreatedTime),
		Configuration:    md.Configuration,
	}
	return dtmd, err
}

func logEntryFromAction(action Action) ([]byte, error) {
	var log []byte
	m := make(map[string]any)

	var err error
	switch action.(type) {
	//TODO: Add errors for missing or null values that are not allowed by the delta protocol
	//https://github.com/delta-io/delta/blob/master/PROTOCOL.md#actions
	case Add, Remove, CommitInfo, MetaData, Protocol:
		// wrap the action data in a camelCase of the action type
		key := strcase.ToLowerCamel(reflect.TypeOf(action).Name())
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

func LogEntryFromActions(actions []Action) ([]byte, error) {
	var jsons [][]byte

	for _, action := range actions {
		j, err := logEntryFromAction(action)
		jsons = append(jsons, j)
		if err != nil {
			return bytes.Join(jsons, []byte("\n")), err
		}
	}

	return bytes.Join(jsons, []byte("\n")), nil
}

// Returns the table schema from the embedded schema string contained within the metadata
// action.
func (m *MetaData) GetSchema() (Schema, error) {
	var schema Schema
	err := json.Unmarshal([]byte(m.SchemaString), &schema)
	return schema, err
}

// / Action used by streaming systems to track progress using application-specific versions to
// / enable idempotency.
type Txn struct {
	/// A unique identifier for the application performing the transaction.
	AppId string
	/// An application-specific numeric identifier for this transaction.
	Version DeltaDataTypeVersion
	/// The time when this transaction action was created in milliseconds since the Unix epoch.
	LastUpdated DeltaDataTypeTimestamp
}

// / Action used to increase the version of the Delta protocol required to read or write to the
// / table.
type Protocol struct {
	/// Minimum version of the Delta read protocol a client must implement to correctly read the
	/// table.
	MinReaderVersion DeltaDataTypeInt `json:"minReaderVersion"`
	/// Minimum version of the Delta write protocol a client must implement to correctly read the
	/// table.
	MinWriterVersion DeltaDataTypeInt `json:"minWriterVersion"`
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

// Stats https://github.com/delta-io/delta/blob/master/PROTOCOL.md#per-file-statistics
//
//	"stats":"{
//		"numRecords":3,
//		"minValues": {\"letter\":\"a\",\"number\":1,\"a_float\":1.1},
//		"maxValues\":{\"letter\":\"c\",\"number\":3,\"a_float\":3.3},
//		"nullCount\":{\"letter\":0,\"number\":0,\"a_float\":0}
//		}"
type Stats struct {
	NumRecords  int64            `json:"numRecords"`
	TightBounds bool             `json:"tightBounds"`
	MinValues   map[string]any   `json:"minValues"`
	MaxValues   map[string]any   `json:"maxValues"`
	NullCount   map[string]int64 `json:"nullCount"`
}

func (s *Stats) Json() []byte {
	b, _ := json.Marshal(s)
	return b
}

// UpdateStats computes Stats.NullCount, Stats.MinValues, Stats.MaxValues for a given k,v struct property
// the struct property is passed in as a pointer to ensure that it can be evaluated as nil[NULL]
// TODO Handel struct types
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
