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
	"errors"
	"testing"
	"time"
)

func TestParseInterval(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Duration
		wantErr bool
	}{
		{name: "seconds", input: "interval 30 seconds", want: 30 * time.Second},
		{name: "minutes", input: "interval 5 minutes", want: 5 * time.Minute},
		{name: "hours", input: "interval 12 hours", want: 12 * time.Hour},
		{name: "days", input: "interval 7 days", want: 7 * 24 * time.Hour},
		{name: "weeks", input: "interval 2 weeks", want: 2 * 7 * 24 * time.Hour},
		{name: "one month is 30 days", input: "interval 1 month", want: 30 * 24 * time.Hour},
		{name: "one year is 365 days", input: "interval 1 year", want: 365 * 24 * time.Hour},
		{name: "combined", input: "interval 1 day 12 hours", want: 36 * time.Hour},
		{name: "no interval prefix", input: "45 seconds", want: 45 * time.Second},
		{name: "mixed case", input: "INTERVAL 1 HOUR", want: time.Hour},
		{name: "empty", input: "", wantErr: true},
		{name: "odd component count", input: "interval 1", wantErr: true},
		{name: "unknown unit", input: "interval 1 fortnight", wantErr: true},
		{name: "non-numeric", input: "interval one day", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseInterval(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("parseInterval(%q) expected error, got %v", tt.input, got)
				} else if !errors.Is(err, ErrConfigValidation) {
					t.Errorf("parseInterval(%q) error %v is not ErrConfigValidation", tt.input, err)
				}
				return
			}
			if err != nil {
				t.Errorf("parseInterval(%q) unexpected error: %v", tt.input, err)
				return
			}
			if got != tt.want {
				t.Errorf("parseInterval(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
