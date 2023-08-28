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
package nillock

import (
	"github.com/rivian/delta-go/lock"
)

// / An NilLock implements the Locker interface but is not backed by anything
// / It is intended for use in a scenario where there is guaranteed to be only one process writing to a delta table
// / so that locking is not necessary
// /
// / WARNING:
// / It provides NO concurrency support.
// / It must be used with a single goroutine only.
// / If used while there are other goroutines or applications (including Spark) writing to the table then
// / it is likely that commits will be overwritten and lost.
// / This is intended only for testing, or for use with local tables.
type NilLock struct {
}

// Compile time check that NilLock implements lock.Locker
var _ lock.Locker = (*NilLock)(nil)

func (*NilLock) NewLock(key string, opt interface{}, metadata interface{}) (interface{}, error) {
	return new(NilLock), nil
}

func New() *NilLock {
	return new(NilLock)
}

// Does nothing
func (*NilLock) Unlock() error {
	return nil
}

// Always returns true
func (*NilLock) TryLock() (bool, error) {
	return true, nil
}
