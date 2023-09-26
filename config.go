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
package delta

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

type DeltaConfigKey string

var (
	ErrConfigValidation = errors.New("error validating delta configuration")
)

const (
	AppendOnlyDeltaConfigKey                      DeltaConfigKey = "delta.appendOnly"
	CheckpointIntervalDeltaConfigKey              DeltaConfigKey = "delta.checkpointInterval"
	AutoOptimizeAutoCompactDeltaConfigKey         DeltaConfigKey = "delta.autoOptimize.autoCompact"
	AutoOptimizeOptimizeWriteDeltaConfigKey       DeltaConfigKey = "delta.autoOptimize.optimizeWrite"
	CheckpointWriteStatsAsJsonDeltaConfigKey      DeltaConfigKey = "delta.checkpoint.writeStatsAsJson"
	CheckpointWriteStatsAsStructDeltaConfigKey    DeltaConfigKey = "delta.checkpoint.writeStatsAsStruct"
	ColumnMappingModeDeltaConfigKey               DeltaConfigKey = "delta.columnMapping.mode"
	DataSkippingNumIndexedColsDeltaConfigKey      DeltaConfigKey = "delta.dataSkippingNumIndexedCols"
	DeletedFileRetentionDurationDeltaConfigKey    DeltaConfigKey = "delta.deletedFileRetentionDuration"
	EnableChangeDataFeedDeltaConfigKey            DeltaConfigKey = "delta.enableChangeDataFeed"
	IsolationLevelDeltaConfigKey                  DeltaConfigKey = "delta.isolationLevel"
	LogRetentionDurationDeltaConfigKey            DeltaConfigKey = "delta.logRetentionDuration"
	EnableExpiredLogCleanupDeltaConfigKey         DeltaConfigKey = "delta.enableExpiredLogCleanup"
	MinReaderVersionDeltaConfigKey                DeltaConfigKey = "delta.minReaderVersion"
	MinWriterVersionDeltaConfigKey                DeltaConfigKey = "delta.minWriterVersion"
	RandomizeFilePrefixesDeltaConfigKey           DeltaConfigKey = "delta.randomizeFilePrefixes"
	RandomPrefixLengthDeltaConfigKey              DeltaConfigKey = "delta.randomPrefixLength"
	SetTransactionRetentionDurationDeltaConfigKey DeltaConfigKey = "delta.setTransactionRetentionDuration"
	TargetFileSizeDeltaConfigKey                  DeltaConfigKey = "delta.targetFileSize"
	TuneFileSizesForRewritesDeltaConfigKey        DeltaConfigKey = "delta.tuneFileSizesForRewrites"
)

// See safeStringToInterval at
// https://apache.googlesource.com/spark/+/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/IntervalUtils.scala
func ParseInterval(input string) (time.Duration, error) {
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
