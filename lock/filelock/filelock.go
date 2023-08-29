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
package filelock

import (
	"errors"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
	"github.com/rivian/delta-go/lock"
	"github.com/rivian/delta-go/storage"
)

type FileLock struct {
	BaseURI *storage.Path
	Key     string
	lock    *flock.Flock
	Options Options
}

// Compile time check that FileLock implements lock.Locker
var _ lock.Locker = (*FileLock)(nil)

type Options struct {
	// The amount of time (in seconds) that the owner has this lock for.
	// If lease_duration is None then the lock is non-expirable.
	TTL time.Duration
	// Block=true sets the behavior of TryLock() to blocking, if Block=false TryLock() will
	// be non-blocking and will return false if the lock is currently held by a different client
	Block bool
}

const (
	TTL time.Duration = 60 * time.Second
)

func (fl *FileLock) NewLock(key string) (lock.Locker, error) {
	newFl := new(FileLock)
	newFl.BaseURI = fl.BaseURI
	newFl.Key = key
	newFl.Options = fl.Options

	return newFl, nil
}

func New(baseURI *storage.Path, key string, opt Options) *FileLock {
	fl := new(FileLock)
	fl.BaseURI = baseURI
	fl.Key = key

	if opt.TTL == 0 {
		fl.Options.TTL = TTL
	}
	fl.Options.Block = opt.Block

	return fl
}

func (fl *FileLock) TryLock() (bool, error) {
	lockPath := filepath.Join(fl.BaseURI.Raw, fl.Key)
	if fl.lock == nil {
		fl.lock = flock.New(lockPath)
	}

	// locked, err := l.lock.TryLock()
	var err error
	var locked bool
	switch fl.Options.Block {
	case true:
		err = fl.lock.Lock()
		//Enforce a TTL
		go func() {
			time.Sleep(fl.Options.TTL)
			fl.Unlock()
		}()
		locked = true
	case false:
		locked, err = fl.lock.TryLock()
	}

	if err != nil || !locked {
		return locked, errors.Join(lock.ErrorLockNotObtained, err)
	}
	return locked, err
}

func (fl *FileLock) Unlock() error {
	err := fl.lock.Unlock()
	if err != nil {
		return errors.Join(lock.ErrorUnableToUnlock, err)
	}
	return nil
}
