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
package lock

import (
	"errors"
)

var (
	ErrorLockNotObtained error = errors.New("the lock could not be obtained")
	ErrorUnableToUnlock  error = errors.New("the lock could not be released")
)

// Locker is the abstract interface for providing a lock client that stores data in the lock
// The data can be used to provide information about the application using the lock including
// the prior lock client version.
type Locker interface {
	// Creates a new lock with an existing lock client
	NewLock(string, interface{}, interface{}) (interface{}, error)

	// Releases the lock
	// Otherwise returns ErrorUnableToUnlock.
	Unlock() error

	// Attempts to acquire lock. If successful, returns the true.
	// Otherwise returns false, ErrorLockNotObtained.
	TryLock() (bool, error)
}
