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
	"testing"
	"time"

	"github.com/rivian/delta-go/lock"
	"github.com/rivian/delta-go/storage"
	"github.com/stretchr/testify/assert"
)

func TestTryLock(t *testing.T) {
	tmpDir := t.TempDir()

	tmpPath := storage.NewPath(tmpDir)
	fl := New(tmpPath, "_commit.lock", Options{TTL: 2 * time.Second})

	locked, err := fl.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !locked {
		t.Errorf("locked = %v; want true", locked)
	}

	otherFileLock := FileLock{BaseURI: tmpPath, Key: "_commit.lock"}
	hasLock, err := otherFileLock.TryLock()
	if !errors.Is(err, lock.ErrorLockNotObtained) {
		t.Errorf("err = %e; expected %e", err, lock.ErrorLockNotObtained)
	}
	if hasLock {
		t.Errorf("hasLock = %v; want false", hasLock)
	}

	fl.Unlock()
	hasLock, err = otherFileLock.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !hasLock {
		t.Errorf("hasLock = %v; want true", hasLock)
	}

}

func TestNewLock(t *testing.T) {
	tmpDir := t.TempDir()

	tmpPath := storage.NewPath(tmpDir)
	fl := New(tmpPath, "_commit.lock", Options{TTL: 2 * time.Second})
	newFl, err := fl.NewLock("_new_commit.lock")
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, newFl.(*FileLock).Key, "_new_commit.lock", "The name of the key should be updated.")

	locked, err := newFl.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !locked {
		t.Errorf("locked = %v; want true", locked)
	}

	otherFileLock := FileLock{BaseURI: tmpPath, Key: "_new_commit.lock"}
	hasLock, err := otherFileLock.TryLock()
	if !errors.Is(err, lock.ErrorLockNotObtained) {
		t.Errorf("err = %e; expected %e", err, lock.ErrorLockNotObtained)
	}
	if hasLock {
		t.Errorf("hasLock = %v; want false", hasLock)
	}

	newFl.(*FileLock).Unlock()
	hasLock, err = otherFileLock.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !hasLock {
		t.Errorf("hasLock = %v; want true", hasLock)
	}

}

func TestTryLockBlocking(t *testing.T) {
	tmpDir := t.TempDir()

	tmpPath := storage.NewPath(tmpDir)
	fl := New(tmpPath, "_commit.lock", Options{TTL: 2 * time.Second, Block: true})

	locked, err := fl.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !locked {
		t.Errorf("locked = %v; want true", locked)
	}

	otherFileLock := New(tmpPath, "_commit.lock", Options{TTL: 2 * time.Second, Block: true})
	hasLock, err := otherFileLock.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !hasLock {
		t.Errorf("hasLock = %v; want true", hasLock)
	}

	fl.Unlock()
	hasLock, err = otherFileLock.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !hasLock {
		t.Errorf("hasLock = %v; want true", hasLock)
	}
}
