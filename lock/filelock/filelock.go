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

// Package filelock provides the resources required to create a file lock.
package filelock

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
	"github.com/rivian/delta-go/lock"
	"github.com/rivian/delta-go/storage"
)

const (
	defaultTTL time.Duration = 60 * time.Second
)

// FileLock represents a lock for a key stored as a single file.
type FileLock struct {
	baseURI storage.Path
	key     string
	lock    *flock.Flock
	opts    Options
}

// Compile time check that FileLock implements lock.Locker
var _ lock.Locker = (*FileLock)(nil)

// Options contains settings that can be adjusted to change the behavior of a file lock.
type Options struct {
	// The amount of time (in seconds) that the owner has this lock for.
	// If lease_duration is None then the lock is non-expirable.
	TTL time.Duration
	// Block=true sets the behavior of TryLock() to blocking, if Block=false TryLock() will
	// be non-blocking and will return false if the lock is currently held by a different client
	Block           bool
	DeleteOnRelease bool
}

// setOptionsDefaults sets the default options.
func (opts *Options) setOptionsDefaults() {
	if opts.TTL == 0 {
		opts.TTL = defaultTTL
	}
}

// New creates a new FileLock instance.
func New(baseURI storage.Path, key string, opts Options) *FileLock {
	opts.setOptionsDefaults()

	l := new(FileLock)
	l.baseURI = baseURI
	l.key = key
	l.opts = opts

	return l
}

// NewLock creates a new FileLock instance using an existing FileLock instance.
func (l *FileLock) NewLock(key string) (lock.Locker, error) {
	nl := new(FileLock)
	nl.baseURI = l.baseURI
	nl.key = key
	nl.opts = l.opts

	return nl, nil
}

// TryLock attempts to acquire a file lock.
func (l *FileLock) TryLock() (acquiredLock bool, returnErr error) {
	lockPath := filepath.Join(l.baseURI.Raw, l.key)
	if l.lock == nil {
		l.lock = flock.New(lockPath)
	}

	// locked, err := l.lock.TryLock()
	var err error
	var locked bool
	switch l.opts.Block {
	case true:
		err = l.lock.Lock()
		//Enforce a TTL
		go func() {
			time.Sleep(l.opts.TTL)

			if err := l.Unlock(); err != nil {
				returnErr = errors.Join(lock.ErrUnableToUnlock, err)
			}
		}()
		locked = true
	case false:
		locked, err = l.lock.TryLock()
	}

	if err != nil || !locked {
		return locked, errors.Join(lock.ErrLockNotObtained, err)
	}
	return locked, err
}

// Unlock releases a file lock.
func (l *FileLock) Unlock() error {
	err := l.lock.Unlock()
	if err != nil {
		return errors.Join(lock.ErrUnableToUnlock, err)
	}

	if l.opts.DeleteOnRelease {
		err := os.Remove(l.lock.Path())
		if err != nil {
			return err
		}
	}

	return nil
}
