// Copyright 2023 Rivian Automotive, Inc.
// Licensed under the Apache License, Version 2.0 (the “License”);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an “AS IS” BASIS
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package delta contains the resources required to interact with a Delta table.
package delta

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

// ConfigKey represents a Delta configuration.
type ConfigKey string

var (
	//ErrConfigValidation is returned when a Delta configuration cannot be validated.
	ErrConfigValidation = errors.New("error validating delta configuration")
)

const (
	// AppendOnlyDeltaConfigKey represents the Delta configuration to specify whethere a table is append-only.
	AppendOnlyDeltaConfigKey ConfigKey = "delta.appendOnly"
	// CheckpointIntervalDeltaConfigKey represents the Delta configuration to specify a checkpoint interval.
	CheckpointIntervalDeltaConfigKey ConfigKey = "delta.checkpointInterval"
	// AutoOptimizeAutoCompactDeltaConfigKey represents the Delta configuration to specify whether auto compaction needs to be enabled.
	AutoOptimizeAutoCompactDeltaConfigKey ConfigKey = "delta.autoOptimize.autoCompact"
	// AutoOptimizeOptimizeWriteDeltaConfigKey represents the Delta configuration to specify whether optimized writing needs to be enabled.
	AutoOptimizeOptimizeWriteDeltaConfigKey ConfigKey = "delta.autoOptimize.optimizeWrite"
	// CheckpointWriteStatsAsJSONDeltaConfigKey represents the Delta configuration to specify whether stats need to be written as a JSON object in a checkpoint.
	CheckpointWriteStatsAsJSONDeltaConfigKey ConfigKey = "delta.checkpoint.writeStatsAsJson"
	// CheckpointWriteStatsAsStructDeltaConfigKey represents the Delta configuration to specify whether stats need to be written as a struct in a checkpoint.
	CheckpointWriteStatsAsStructDeltaConfigKey ConfigKey = "delta.checkpoint.writeStatsAsStruct"
	// ColumnMappingModeDeltaConfigKey represents the Delta configuration to specify whether column mapping needs to be enabled.
	ColumnMappingModeDeltaConfigKey ConfigKey = "delta.columnMapping.mode"
	// DataSkippingNumIndexedColsDeltaConfigKey represents the Delta configuration to specify the number of columns for which to collect stats.
	DataSkippingNumIndexedColsDeltaConfigKey ConfigKey = "delta.dataSkippingNumIndexedCols"
	// DeletedFileRetentionDurationDeltaConfigKey represents the Delta configuration to specify the retention duration of a deleted file.
	DeletedFileRetentionDurationDeltaConfigKey ConfigKey = "delta.deletedFileRetentionDuration"
	// EnableChangeDataFeedDeltaConfigKey represents the Delta configuration to specify whether change data feed needs to be enabled.
	EnableChangeDataFeedDeltaConfigKey ConfigKey = "delta.enableChangeDataFeed"
	// IsolationLevelDeltaConfigKey represents the Delta configuration to specify what isolation level to use.
	IsolationLevelDeltaConfigKey ConfigKey = "delta.isolationLevel"
	// LogRetentionDurationDeltaConfigKey represents the Delta configuration to specify the retention duration of commit logs.
	LogRetentionDurationDeltaConfigKey ConfigKey = "delta.logRetentionDuration"
	// EnableExpiredLogCleanupDeltaConfigKey represents the Delta configuration to specify whether expired commit logs need be cleaned up.
	EnableExpiredLogCleanupDeltaConfigKey ConfigKey = "delta.enableExpiredLogCleanup"
	// MinReaderVersionDeltaConfigKey represents the Delta configuration tp specify the minimum reader version.
	MinReaderVersionDeltaConfigKey ConfigKey = "delta.minReaderVersion"
	// MinWriterVersionDeltaConfigKey represents the Delta configuration to specify the minimum writer version.
	MinWriterVersionDeltaConfigKey ConfigKey = "delta.minWriterVersion"
	// RandomizeFilePrefixesDeltaConfigKey represents the Delta configuration to specify whether file prefixes should be randomized.
	RandomizeFilePrefixesDeltaConfigKey ConfigKey = "delta.randomizeFilePrefixes"
	// RandomPrefixLengthDeltaConfigKey represents the Delta configuration to specify the number of characters generated for random prefixes.
	RandomPrefixLengthDeltaConfigKey ConfigKey = "delta.randomPrefixLength"
	// SetTransactionRetentionDurationDeltaConfigKey represents the Delta configuration to specify the retention duration of a transaction.
	SetTransactionRetentionDurationDeltaConfigKey ConfigKey = "delta.setTransactionRetentionDuration"
	// TargetFileSizeDeltaConfigKey represents the Delta configuration to specify the target size of a file.
	TargetFileSizeDeltaConfigKey ConfigKey = "delta.targetFileSize"
	// TuneFileSizesForRewritesDeltaConfigKey represents the Delta configuration to specify whether file sizes need to be tuned for rewrites.
	TuneFileSizesForRewritesDeltaConfigKey ConfigKey = "delta.tuneFileSizesForRewrites"
)

// See safeStringToInterval at
// https://apache.googlesource.com/spark/+/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/IntervalUtils.scala
func parseInterval(input string) (time.Duration, error) {
	input = strings.TrimPrefix(strings.ToLower(input), "interval ")
	components := strings.Fields(input)
	if len(components) == 0 || len(components)%2 != 0 {
		return time.Duration(0), ErrConfigValidation
	}
	i := 0
	var totalDuration time.Duration
	for i < len(components) {
		numericComponentStr := components[i]
		// TODO fractional values should likely be permitted
		numericComponent, err := strconv.ParseInt(numericComponentStr, 10, 64)
		durationComponent := time.Duration(numericComponent)
		if err != nil {
			return time.Duration(0), errors.Join(ErrConfigValidation, err)
		}
		i++
		unit := components[i]
		i++
		var durationUnit time.Duration

		switch unit {
		case "nanosecond", "nanoseconds":
			durationUnit = time.Nanosecond
		case "microsecond", "microseconds":
			durationUnit = time.Microsecond
		case "millisecond", "milliseconds":
			durationUnit = time.Millisecond
		case "second", "seconds":
			durationUnit = time.Second
		case "minute", "minutes":
			durationUnit = time.Minute
		case "hour", "hours":
			durationUnit = time.Hour
		case "day", "days":
			// Note that the CalendarInterval type in spark implements variable length days
			durationUnit = time.Hour * 24
		case "week", "weeks":
			durationUnit = time.Hour * 24 * 7
		case "month", "months":
			// Note that the CalendarInterval type in spark implements variable length months
			durationUnit = time.Hour * 24 * 7 * 31
		case "year", "years":
			durationUnit = time.Hour * 24 * 7 * 365
		default:
			return time.Duration(0), ErrConfigValidation
		}
		totalDuration += (durationUnit * durationComponent)
	}
	return totalDuration, nil
}
