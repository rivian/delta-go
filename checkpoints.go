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
	"fmt"

	"github.com/rivian/delta-go/state"
	"github.com/rivian/delta-go/storage"
)

// / Metadata for a checkpoint file
type CheckPoint struct {
	/// Delta table version
	Version state.DeltaDataTypeVersion
	// 20 digits decimals
	Size DeltaDataTypeLong
	// 10 digits decimals
	Parts uint32
}

type CheckpointEntry[RowType any, PartitionType any] struct {
	Txn      *Txn                         `parquet:"txn"`
	Add      *Add[RowType, PartitionType] `parquet:"add"`
	Remove   *Remove                      `parquet:"remove"`
	MetaData *MetaData                    `parquet:"metaData"`
	Protocol *Protocol                    `parquet:"protocol"`
	Cdc      *Cdc                         `parquet:"-"`
}

func CheckpointFromBytes(bytes []byte) (*CheckPoint, error) {
	checkpoint := new(CheckPoint)
	err := json.Unmarshal(bytes, checkpoint)
	if err != nil {
		return nil, err
	}
	return checkpoint, nil
}

func LastCheckpointPath() *storage.Path {
	path := storage.PathFromIter([]string{"_delta_log", "_last_checkpoint"})
	return &path
}

func CreateCheckpointFor[RowType any, PartitionType any](tableState *DeltaTableState[RowType, PartitionType], store storage.ObjectStore) error {
	lastCheckpointPath := LastCheckpointPath()
	parquetBytes, err := tableState.GetCheckpointBytes()
	if err != nil {
		return err
	}
	// TODO multipart
	checkpoint := CheckPoint{Version: tableState.Version, Size: DeltaDataTypeLong(len(parquetBytes)), Parts: 0}
	checkpointFileName := fmt.Sprintf("%020d.checkpoint.parquet", checkpoint.Version)
	checkpointPath := storage.PathFromIter([]string{"_delta_log", checkpointFileName})
	err = store.Put(&checkpointPath, parquetBytes)
	if err != nil {
		return err
	}
	checkpointBytes, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	err = store.Put(lastCheckpointPath, checkpointBytes)
	if err != nil {
		return err
	}
	return nil
}

func CheckpointAddFromState[RowType any, PartitionType any](add *Add[RowType, PartitionType]) (*Add[RowType, PartitionType], error) {
	checkpointAdd := new(Add[RowType, PartitionType])
	*checkpointAdd = *add
	checkpointAdd.DataChange = false

	stats, err := StatsFromJson([]byte(add.Stats))
	if err != nil {
		return nil, err
	}
	parsedStats, err := StatsAsGenericStats[RowType](stats)
	if err != nil {
		return nil, err
	}
	checkpointAdd.StatsParsed = *parsedStats

	partitionValuesParsed, err := PartitionValuesAsGeneric[PartitionType](add.PartitionValues)
	if err != nil {
		return nil, err
	}
	checkpointAdd.PartitionValuesParsed = *partitionValuesParsed

	return checkpointAdd, nil
}
