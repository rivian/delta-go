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
	"math"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/metadata"
	"github.com/apache/arrow/go/v14/parquet/schema"
	"github.com/rivian/delta-go/storage"
	"golang.org/x/exp/constraints"
)

// Stats contains statistics about a Parquet file in an Add action
type Stats struct {
	NumRecords  int64            `json:"numRecords" parquet:"name=numRecords, repetition=OPTIONAL"`
	TightBounds bool             `json:"tightBounds" parquet:"name=tightBounds, repetition=OPTIONAL"`
	MinValues   map[string]any   `json:"minValues" parquet:"name=minValues, repetition=OPTIONAL, keyconverted=UTF8"`
	MaxValues   map[string]any   `json:"maxValues" parquet:"name=maxValues, repetition=OPTIONAL, keyconverted=UTF8"`
	NullCount   map[string]int64 `json:"nullCount" parquet:"name=nullCount, repetition=OPTIONAL, keyconverted=UTF8, valuetype=INT64"`
}

const (
	stringStatMaxLen        = 32
	stringStatMaxTiebreaker = '\ufffd'
	microsecondsPerSecond   = int64(time.Second / time.Microsecond)
	millisecondsPerSecond   = int64(time.Second / time.Millisecond)
)

// JSON converts the stats into JSON
func (s *Stats) JSON() ([]byte, error) {
	b, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// StatsFromJSON parses JSON into a Stats object
func StatsFromJSON(b []byte) (*Stats, error) {
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
	}
	v = *vpt
	// Allow for different format for string max value
	vMax := v

	// Strings are truncated to 32 characters
	// and there is a trailing "unicode max" character for the max stat
	switch tv := any(v).(type) {
	case string:
		v = any(truncatedStringStat(tv, false)).(T)
		vMax = any(truncatedStringStat(tv, true)).(T)
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
		if vMax > priorMax.(T) {
			s.MaxValues[k] = vMax
		}
	} else {
		if s.MaxValues == nil {
			s.MaxValues = make(map[string]any)
		}
		s.MaxValues[k] = vMax
	}
}

// StatsFromParquet retrieves stats directly from the Parquet file in the Add action
// It does not currently support nested types, or logical types that can't be generated in Spark
// (UUID, interval, JSON, BSON)
// It also will not return stats for timestamps stored in int96 columns because the Parquet file
// won't have those stats
func StatsFromParquet(store storage.ObjectStore, add *Add) (*Stats, []string, error) {
	readerAtSeeker, err := storage.NewObjectReaderAtSeeker(storage.NewPath(add.Path), store)
	if err != nil {
		return nil, nil, err
	}
	parquetReader, err := file.NewParquetReader(readerAtSeeker)
	if err != nil {
		return nil, nil, err
	}
	meta := parquetReader.MetaData()

	validStatsColumns := make([]bool, meta.Schema.NumColumns())
	validCount := 0
	for i := 0; i < meta.Schema.NumColumns(); i++ {
		c := meta.Schema.Column(i)
		name := c.Name()
		_, isPartitionColumn := add.PartitionValues[name]
		if isPartitionColumn {
			validStatsColumns[i] = false
			continue
		}
		// TODO support nested types
		if c.LogicalType().IsNested() {
			return nil, nil, errors.New("nested types are not supported")
		}
		if c.PhysicalType() == parquet.Types.Boolean {
			validStatsColumns[i] = false
			continue
		}
		// No stats (except null count) for non-string binary type, based on inspection of Spark-generated delta table
		// TODO investigate additional parquet logical types UUID, interval, JSON, BSON
		if c.PhysicalType() == parquet.Types.ByteArray && !c.LogicalType().Equals(schema.StringLogicalType{}) {
			validStatsColumns[i] = false
			continue
		}

		// Do not include nested fields, such as map keys or values
		if len(c.ColumnPath()) > 1 {
			validStatsColumns[i] = false
			continue
		}

		validStatsColumns[i] = true
		validCount++
	}

	s := new(Stats)
	s.MaxValues = make(map[string]any, validCount)
	s.MinValues = make(map[string]any, validCount)
	s.NullCount = make(map[string]int64, validCount)
	typedStatistics := make(map[string]metadata.TypedStatistics, validCount)

	missingStatsColumns := make([]string, 0)

	// Get cumulative statistics from all column chunks in all rowgroups
	for r := 0; r < len(meta.RowGroups); r++ {
		rg := meta.RowGroup(r)
		s.NumRecords += rg.NumRows()

		for c := 0; c < rg.NumColumns(); c++ {
			cc, err := rg.ColumnChunk(c)
			if err != nil {
				return nil, nil, err
			}
			typedStats, err := cc.Statistics()
			if err != nil {
				return nil, nil, err
			}
			if typedStats == nil {
				missingStatsColumns = append(missingStatsColumns, cc.PathInSchema().String())
				continue
			}
			if !validStatsColumns[c] {
				// Don't save min/max stats for these columns
				nullCount := typedStats.NullCount()
				typedStats.Reset()
				typedStats.IncNulls(nullCount)
			}
			name := parquetReader.MetaData().Schema.Column(c).Name()
			if priorStats, hasStats := typedStatistics[name]; hasStats {
				typedStats.Merge(priorStats)
			}
			typedStatistics[name] = typedStats
		}
	}

	// Convert to our stats type
	for name, stats := range typedStatistics {
		if stats.HasNullCount() {
			s.NullCount[name] = stats.NullCount()
		}
		if stats.HasMinMax() {
			index := meta.Schema.ColumnIndexByName(name)
			if index == -1 {
				return nil, nil, fmt.Errorf("column %s not found in schema", name)
			}
			c := meta.Schema.Column(index)
			switch t := c.LogicalType().(type) {
			case schema.StringLogicalType:
				setStringStats(stats, s, name)
			case schema.DateLogicalType:
				d := metadata.GetStatValue(stats.Type(), stats.EncodeMin())
				dateInt, ok := d.(int32)
				if ok {
					s.MinValues[name] = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(dateInt)).Format("2006-01-02")
				}
				d = metadata.GetStatValue(stats.Type(), stats.EncodeMax())
				dateInt, ok = d.(int32)
				if ok {
					s.MaxValues[name] = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(dateInt)).Format("2006-01-02")
				}
			case schema.TimestampLogicalType, *schema.TimestampLogicalType:
				ts := metadata.GetStatValue(stats.Type(), stats.EncodeMin())
				formatted, err := formattedTimestampStat(ts, t)
				if err != nil {
					return nil, nil, err
				}
				s.MinValues[name] = formatted
				ts = metadata.GetStatValue(stats.Type(), stats.EncodeMax())
				formatted, err = formattedTimestampStat(ts, t)
				if err != nil {
					return nil, nil, err
				}
				s.MaxValues[name] = formatted
			case schema.TimeLogicalType, *schema.TimeLogicalType:
				ts := metadata.GetStatValue(stats.Type(), stats.EncodeMin())
				formatted, err := formattedTimeStat(ts, t)
				if err != nil {
					return nil, nil, err
				}
				s.MinValues[name] = formatted
				ts = metadata.GetStatValue(stats.Type(), stats.EncodeMax())
				formatted, err = formattedTimeStat(ts, t)
				if err != nil {
					return nil, nil, err
				}
				s.MaxValues[name] = formatted
			case *schema.DecimalLogicalType:
				// TODO support int64 and byte array (more precision)
				d := metadata.GetStatValue(stats.Type(), stats.EncodeMin())
				s.MinValues[name] = d
				switch d := d.(type) {
				case int32:
					// For example, if precision is 5 we want "%5d"
					formatString := fmt.Sprintf("%%%dd", t.Precision())
					withoutDecimal := fmt.Sprintf(formatString, d)
					decimalIndex := t.Precision() - t.Scale()
					s.MinValues[name] = StatsDecimal(strings.TrimSpace(withoutDecimal[:decimalIndex] + "." + withoutDecimal[decimalIndex:]))
				}
				d = metadata.GetStatValue(stats.Type(), stats.EncodeMax())
				switch d := d.(type) {
				case int32:
					formatString := fmt.Sprintf("%%%dd", t.Precision())
					withoutDecimal := fmt.Sprintf(formatString, d)
					decimalIndex := t.Precision() - t.Scale()
					s.MaxValues[name] = StatsDecimal(strings.TrimSpace(withoutDecimal[:decimalIndex] + "." + withoutDecimal[decimalIndex:]))
				}
			default:
				if c.ConvertedType() == schema.ConvertedTypes.UTF8 {
					setStringStats(stats, s, name)
				} else {
					s.MinValues[name] = metadata.GetStatValue(stats.Type(), stats.EncodeMin())
					s.MaxValues[name] = metadata.GetStatValue(stats.Type(), stats.EncodeMax())
					if stats.Type() == parquet.Types.Double {
						// Wrap float and double stats so we can handle NaN, inf, -inf
						s.MinValues[name] = StatsFloat64(s.MinValues[name].(float64))
						s.MaxValues[name] = StatsFloat64(s.MaxValues[name].(float64))
					} else if stats.Type() == parquet.Types.Float {
						s.MinValues[name] = StatsFloat32(s.MinValues[name].(float32))
						s.MaxValues[name] = StatsFloat32(s.MaxValues[name].(float32))
					}
				}
			}
		}
	}

	return s, missingStatsColumns, nil
}

// StatsDecimal allows us to store decimal stats as a string and write to JSON without quotes
type StatsDecimal string

// MarshalJSON writes the decimal string without surrounding quotes
func (sd StatsDecimal) MarshalJSON() ([]byte, error) {
	return []byte(sd), nil
}

// StatsFloat64 allows us to marshal and unmarshal inf and -inf as strings
type StatsFloat64 float64

// StatsFloat32 allows us to marshal and unmarshal inf and -inf as strings
type StatsFloat32 float32

// MarshalJSON writes the float as a string if it is NaN, inf or -inf
func (sf StatsFloat64) MarshalJSON() ([]byte, error) {
	if math.IsInf(float64(sf), 1) {
		return []byte("\"Infinity\""), nil
	}
	if math.IsInf(float64(sf), -1) {
		return []byte("\"-Infinity\""), nil
	}
	if math.IsNaN(float64(sf)) {
		return []byte("\"NaN\""), nil
	}
	return json.Marshal(float64(sf))
}

// MarshalJSON writes the float as a string if it is NaN, inf or -inf
func (sf StatsFloat32) MarshalJSON() ([]byte, error) {
	if math.IsInf(float64(sf), 1) {
		return []byte("\"Infinity\""), nil
	}
	if math.IsInf(float64(sf), -1) {
		return []byte("\"-Infinity\""), nil
	}
	if math.IsNaN(float64(sf)) {
		return []byte("\"NaN\""), nil
	}
	return json.Marshal(float32(sf))
}

// Set the string statistics from typedStats
func setStringStats(typedStats metadata.TypedStatistics, stats *Stats, name string) {
	stats.MinValues[name] = truncatedStringStat(string(typedStats.EncodeMin()), false)
	stats.MaxValues[name] = truncatedStringStat(string(typedStats.EncodeMax()), true)
}

// Truncate to length 32
// If this is for the max stat, also append the unicode max character; need to check for
// trailing characters > unicode max and append them first.
// See:
// https://github.com/delta-io/delta/blob/4f9c8b9cc294ec7b321847115bf87909c356bc5a/spark/src/main/scala/org/apache/spark/sql/delta/stats/StatisticsCollection.scala#L759C66-L760C1
func truncatedStringStat(s string, maxStat bool) string {
	if len(s) >= stringStatMaxLen {
		r := []rune(s)
		remainder := r[stringStatMaxLen:]
		r = r[:stringStatMaxLen]
		if maxStat {
			for i := 0; i < len(remainder); i++ {
				if remainder[i] > stringStatMaxTiebreaker {
					r = append(r, remainder[i])
				} else {
					break
				}
			}
			s = string(r) + string(stringStatMaxTiebreaker)
		} else {
			s = string(r)
		}
	}
	return s
}

// Format a timestamp for stats
func formattedTimestampStat(v interface{}, t schema.LogicalType) (string, error) {
	vInt, ok := v.(int64)
	if !ok {
		return "", errors.New("could not convert timestamp to int64")
	}

	// TODO current version of Spark in Delta can't handle INT64 nano so that's not tested
	// https://issues.apache.org/jira/browse/SPARK-40819
	// https://community.databricks.com/t5/missing-questionpost/discrepancies-between-official-spark-3-3-2-and-what-s-provided/td-p/2678
	var ts time.Time
	var err error
	switch t := t.(type) {
	case schema.TimestampLogicalType:
		ts, err = timeStat(t.TimeUnit(), vInt)
		if err != nil {
			return "", err
		}
	case *schema.TimestampLogicalType:
		ts, err = timeStat(t.TimeUnit(), vInt)
		if err != nil {
			return "", err
		}
	case schema.TimeLogicalType:
	case *schema.TimeLogicalType:
	default:
		return "", errors.New("unexpected timestamp logical type")
	}
	return ts.UTC().Format("2006-01-02T15:04:05.000Z07:00"), nil
}

// Format a time for stats
// This hasn't been validated against Spark output since the current Spark+Delta can't handle
// int32 time millis, int64 time micro, and int64 time nano
func formattedTimeStat(v interface{}, t schema.LogicalType) (string, error) {
	var ts time.Time
	var err error
	isInt32 := false

	vInt, ok := v.(int64)
	if !ok {
		vInt32, ok := v.(int32)
		if !ok {
			return "", errors.New("could not convert time to int64 or int32")
		}
		isInt32 = true
		vInt = int64(vInt32)
	}

	switch t := t.(type) {
	case schema.TimeLogicalType:
		tu := t.TimeUnit()
		if isInt32 && tu != schema.TimeUnitMillis {
			return "", errors.New("time stored in int32 must use millis unit")
		}
		ts, err = timeStat(tu, vInt)
		if err != nil {
			return "", err
		}
	case *schema.TimeLogicalType:
		tu := t.TimeUnit()
		if isInt32 && tu != schema.TimeUnitMillis {
			return "", errors.New("time stored in int32 must use millis unit")
		}
		ts, err = timeStat(tu, vInt)
		if err != nil {
			return "", err
		}
	default:
		return "", errors.New("unexpected time logical type")
	}
	return ts.UTC().Format("15:04:05.000"), nil

}

// Convert a numeric stat value to a time.Time
func timeStat(t schema.TimeUnitType, vInt int64) (time.Time, error) {
	var ts time.Time
	switch t {
	case schema.TimeUnitMillis:
		ts = time.Unix(vInt/millisecondsPerSecond, (vInt%millisecondsPerSecond)*int64(time.Millisecond))
	case schema.TimeUnitMicros:
		ts = time.Unix(vInt/microsecondsPerSecond, (vInt%microsecondsPerSecond)*int64(time.Microsecond))
	case schema.TimeUnitNanos:
		ts = time.Unix(0, vInt)
	default:
		return ts, errors.New("unknown TimeUnit")
	}
	return ts, nil
}
