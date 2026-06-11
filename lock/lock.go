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

// Package lock contains the resources required to create a lock.
package lock

import (
	"errors"
)

var (
	// ErrLockNotObtained is returned when a lock cannot be returned.
	ErrLockNotObtained error = errors.New("the lock could not be obtained")
	// ErrUnableToUnlock is returned when a lock cannot be released.
	ErrUnableToUnlock error = errors.New("the lock could not be released")
	// ErrUnableToExtendLock is returned when a lock's expiry cannot be extended.
	ErrUnableToExtendLock error = errors.New("the lock could not be extended")
)

// Locker is the abstract interface for providing a lock client that stores data in the lock
// The data can be used to provide information about the application using the lock including
// the prior lock client version.
type Locker interface {
	// Creates a new lock using an existing lock instance
	NewLock(key string) (Locker, error)

	// Releases the lock
	// Otherwise returns ErrorUnableToUnlock.
	Unlock() error

	// Attempts to acquire lock. If successful, returns the true.
	// Otherwise returns false, ErrorLockNotObtained.
	TryLock() (bool, error)
}

// Extender is an optional interface for lock implementations that support extending the
// expiry of a held lock. Long-running operations (e.g. large checkpoints) can extend the
// lock periodically instead of requiring an oversized TTL up front.
// Check for support with a type assertion: if e, ok := l.(lock.Extender); ok { e.Extend() }
type Extender interface {
	// Extend resets the expiry of a held lock to its full TTL.
	// Returns false and ErrUnableToExtendLock if the lock could not be extended,
	// e.g. because it has already expired or is held by another client.
	Extend() (bool, error)
}
