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
	ErrActionJSONFormat error = errors.New("invalid format for action JSON")
	ErrActionUnknown    error = errors.New("unknown action")
	ErrActionConversion error = errors.New("error reading action")
)

// Delta log action that describes a parquet data file that is part of the table.
type Action interface {
	// Add | CommitInfo
}

type CommitInfo map[string]interface{}

// An Add action is typed to allow the stats_parsed and partitionValues_parsed fields to be written to checkpoints with
// the correct schema without using reflection.
// The Add variant is for a non-partitioned table; the PartitionValuesParsed field will be omitted.
type Add struct {
	// A relative path, from the root of the table, to a file that should be added to the table
	Path string `json:"path" parquet:"name=path, repetition=OPTIONAL, converted=UTF8"`
	// A map from partition column to value for this file
	// This field is required even without a partition.
	PartitionValues map[string]string `json:"partitionValues" parquet:"name=partitionValues, repetition=OPTIONAL, keyconverted=UTF8, valueconverted=UTF8"`
	// The size of this file in bytes
	Size int64 `json:"size" parquet:"name=size, repetition=OPTIONAL"`
	// The time this file was created, as milliseconds since the epoch
	ModificationTime int64 `json:"modificationTime" parquet:"name=modificationTime, repetition=OPTIONAL"`
	// When false the file must already be present in the table or the records in the added file
	// must be contained in one or more remove actions in the same version
	//
	// streaming queries that are tailing the transaction log can use this flag to skip actions
	// that would not affect the final results.
	DataChange bool `json:"dataChange" parquet:"name=dataChange, repetition=OPTIONAL"`
	// Map containing metadata about this file
	Tags map[string]string `json:"tags,omitempty" parquet:"name=tags, repetition=OPTIONAL, keyconverted=UTF8, valueconverted=UTF8"`
	// Contains statistics (e.g., count, min/max values for columns) about the data in this file
	Stats string `json:"stats" parquet:"name=stats, repetition=OPTIONAL, converted=UTF8"`

	// TODO - parsed fields dropped after the parquet library switch.
	// This requires dropping writer version to 2.
	// We likely need to re-implement using reflection.
	//
	// Partition values stored in raw parquet struct format. In this struct, the column names
	// correspond to the partition columns and the values are stored in their corresponding data
	// type. This is a required field when the table is partitioned and the table property
	// delta.checkpoint.writeStatsAsStruct is set to true. If the table is not partitioned, this
	// column can be omitted.
	//
	// This field is only available in add action records read from / written to checkpoints
	// PartitionValuesParsed *PartitionType `json:"-" parquet:"name=partitionValues_parsed, repetition=OPTIONAL"`
	// Contains statistics (e.g., count, min/max values for columns) about the data in this file in
	// raw parquet format. This field needs to be written when statistics are available and the
	// table property: delta.checkpoint.writeStatsAsStruct is set to true.
	//
	// This field is only available in add action records read from / written to checkpoints
	// StatsParsed *GenericStats `json:"-" parquet:"name=stats_parsed, repetition=OPTIONAL"`
}

// / Represents a tombstone (deleted file) in the Delta log.
// / This is a top-level action in Delta log entries.
type Remove struct {
	/// The path of the file that is removed from the table.
	Path string `json:"path" parquet:"name=path, repetition=OPTIONAL, converted=UTF8"`
	/// The timestamp when the remove was added to table state.
	DeletionTimestamp *int64 `json:"deletionTimestamp" parquet:"name=deletionTimestamp, repetition=OPTIONAL"`
	/// Whether data is changed by the remove. A table optimize will report this as false for
	/// example, since it adds and removes files by combining many files into one.
	DataChange bool `json:"dataChange" parquet:"name=dataChange, repetition=OPTIONAL"`
	/// When true the fields partitionValues, size, and tags are present
	///
	/// NOTE: Although it's defined as required in scala delta implementation, but some writes
	/// it's still nullable so we keep it as Option<> for compatibly.
	ExtendedFileMetadata *bool `json:"extendedFileMetadata" parquet:"name=extendedFileMetadata, repetition=OPTIONAL"`
	/// A map from partition column to value for this file.
	PartitionValues *map[string]string `json:"partitionValues" parquet:"name=partitionValues, repetition=OPTIONAL, keyconverted=UTF8, valueconverted=UTF8"`
	/// Size of this file in bytes
	Size *int64 `json:"size" parquet:"name=size, repetition=OPTIONAL"`
	/// Map containing metadata about this file
	Tags *map[string]string `json:"tags" parquet:"-"`
}

// Describes the data format of files in the table.
// https://github.com/delta-io/delta/blob/master/PROTOCOL.md#format-specification
type Format struct {
	/// Name of the encoding for files in this table.
	// Default: "parquet"
	Provider string `json:"provider" parquet:"name=provider, repetition=OPTIONAL, converted=UTF8"`
	/// A map containing configuration options for the format.
	// Default: {}
	Options map[string]string `json:"options" parquet:"name=options, repetition=OPTIONAL, keyconverted=UTF8, valueconverted=UTF8"`
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
	Id uuid.UUID `json:"id" parquet:"-"`
	/// Parquet library cannot import to UUID
	IdAsString string `json:"-" parquet:"name=id, repetition=OPTIONAL, converted=UTF8"`
	/// User-provided identifier for this table
	Name *string `json:"name" parquet:"name=name, repetition=OPTIONAL, converted=UTF8"`
	/// User-provided description for this table
	Description *string `json:"description" parquet:"name=description, repetition=OPTIONAL, converted=UTF8"`
	/// Specification of the encoding for the files stored in the table
	Format Format `json:"format" parquet:"name=format, repetition=OPTIONAL"`
	/// Schema of the table
	SchemaString string `json:"schemaString" parquet:"name=schemaString, repetition=OPTIONAL, converted=UTF8"`
	/// An array containing the names of columns by which the data should be partitioned
	PartitionColumns []string `json:"partitionColumns" parquet:"name=partitionColumns, repetition=OPTIONAL, valueconverted=UTF8"`
	/// A map containing configuration options for the table
	Configuration map[string]string `json:"configuration" parquet:"name=configuration, repetition=OPTIONAL, keyconverted=UTF8, valueconverted=UTF8"`
	/// The time when this metadata action is created, in milliseconds since the Unix epoch
	CreatedTime *int64 `json:"createdTime" parquet:"name=createdTime, repetition=OPTIONAL"`
}

// MetaData.ToDeltaTableMetaData() converts a MetaData to DeltaTableMetaData
// Internally, it converts the schema from a string.
func (md *MetaData) ToDeltaTableMetaData() (DeltaTableMetaData, error) {
	var err error
	schema, err := md.GetSchema()

	// Manually convert IdAsString to UUID if required
	id := md.Id
	if md.Id == uuid.Nil && len(md.IdAsString) > 0 {
		id, err = uuid.Parse(md.IdAsString)
		if err != nil {
			return DeltaTableMetaData{}, errors.Join(err, errors.New("unable to parse UUID in metadata"))
		}
	}
	dtmd := DeltaTableMetaData{
		Id:               id,
		Schema:           schema,
		PartitionColumns: md.PartitionColumns,
		Configuration:    md.Configuration,
	}
	if md.Name != nil {
		dtmd.Name = *md.Name
	}
	if md.Description != nil {
		dtmd.Description = *md.Description
	}
	dtmd.Format = md.Format
	if md.CreatedTime != nil {
		dtmd.CreatedTime = time.UnixMilli(*md.CreatedTime)
	}
	return dtmd, err
}

// / Action used by streaming systems to track progress using application-specific versions to
// / enable idempotency.
type Txn struct {
	/// A unique identifier for the application performing the transaction.
	AppId string `json:"appId" parquet:"name=appId, repetition=OPTIONAL, converted=UTF8"`
	/// An application-specific numeric identifier for this transaction.
	Version int64 `json:"version" parquet:"name=version, repetition=OPTIONAL"`
	/// The time when this transaction action was created in milliseconds since the Unix epoch.
	LastUpdated *int64 `json:"-" parquet:"name=lastUpdated, repetition=OPTIONAL"`
}

// / Action used to increase the version of the Delta protocol required to read or write to the
// / table.
type Protocol struct {
	/// Minimum version of the Delta read protocol a client must implement to correctly read the
	/// table.
	MinReaderVersion int32 `json:"minReaderVersion" parquet:"name=minReaderVersion, repetition=OPTIONAL"`
	/// Minimum version of the Delta write protocol a client must implement to correctly read the
	/// table.
	MinWriterVersion int32 `json:"minWriterVersion" parquet:"name=minWriterVersion, repetition=OPTIONAL"`
}

// / Default protocol versions are currently 1, 1 as we do not support any more advanced features yet
func (protocol *Protocol) Default() Protocol {
	protocol.MinReaderVersion = 1
	protocol.MinWriterVersion = 1
	return *protocol
}

type Cdc struct {
	/// A relative path, from the root of the table, or an
	/// absolute path to a CDC file
	Path string `json:"path" parquet:"name=path, repetition=OPTIONAL, converted=UTF8"`
	/// The size of this file in bytes
	Size int64 `json:"size" parquet:"name=size, repetition=OPTIONAL"`
	/// A map from partition column to value for this file
	PartitionValues map[string]string `json:"partitionValues"`
	/// Should always be set to false because they do not change the underlying data of the table
	DataChange bool `json:"dataChange" parquet:"name=dataChange, repetition=OPTIONAL"`
	/// Map containing metadata about this file
	Tags *map[string]string `json:"tags,omitempty"`
}

func logEntryFromAction(action Action) ([]byte, error) {
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
	case Add, *Add:
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
func actionFromLogEntry(unstructuredResult map[string]json.RawMessage) (Action, error) {
	if len(unstructuredResult) != 1 {
		return nil, errors.Join(ErrActionJSONFormat, errors.New("log entry JSON must have one value"))
	}

	var action Action
	var marshalledAction json.RawMessage
	var actionFound bool

	if marshalledAction, actionFound = unstructuredResult[string(AddActionKey)]; actionFound {
		action = new(Add)
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
		return nil, ErrActionUnknown
	}

	err := json.Unmarshal(marshalledAction, action)
	if err != nil {
		return nil, errors.Join(ErrActionJSONFormat, err)
	}

	return action, nil
}

// / Retrieve all actions in this log
func ActionsFromLogEntries(logEntries []byte) ([]Action, error) {
	lines := bytes.Split(logEntries, []byte("\n"))
	actions := make([]Action, 0, len(lines))
	for _, currentLine := range lines {
		if len(currentLine) == 0 {
			continue
		}
		var unstructuredResult map[string]json.RawMessage
		err := json.Unmarshal(currentLine, &unstructuredResult)
		if err != nil {
			return nil, errors.Join(ErrActionJSONFormat, err)
		}
		action, err := actionFromLogEntry(unstructuredResult)
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
	err := json.Unmarshal([]byte(m.SchemaString), &schema)
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
	NumRecords  int64            `json:"numRecords" parquet:"name=numRecords, repetition=OPTIONAL"`
	TightBounds bool             `json:"tightBounds" parquet:"name=tightBounds, repetition=OPTIONAL"`
	MinValues   map[string]any   `json:"minValues" parquet:"name=minValues, repetition=OPTIONAL, keyconverted=UTF8"`
	MaxValues   map[string]any   `json:"maxValues" parquet:"name=maxValues, repetition=OPTIONAL, keyconverted=UTF8"`
	NullCount   map[string]int64 `json:"nullCount" parquet:"name=nullCount, repetition=OPTIONAL, keyconverted=UTF8, valuetype=INT64"`
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

// UpdateStats computes Stats.NullCount, Stats.MinValues, Stats.MaxValues for a given k,v struct property
// the struct property is passed in as a pointer to ensure that it can be evaluated as nil[NULL]
// TODO Handle struct types
func UpdateStats[T constraints.Ordered](s *Stats, k string, vpt *T) {
	var v T
	if s.NullCount == nil {
		s.NullCount = make(map[string]int64)
	}
	if _, hasPriorNullCount := s.NullCount[k]; !hasPriorNullCount {
		s.NullCount[k] = 0
	}

	if vpt == nil {
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
