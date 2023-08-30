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
)

func TestTryLock(t *testing.T) {
	tmpDir := t.TempDir()

	tmpPath := storage.NewPath(tmpDir)
	l := New(tmpPath, "_commit.lock", Options{TTL: 2 * time.Second})

	t.Log(l.String())

	locked, err := l.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !locked {
		t.Errorf("locked = %v; want true", locked)
	}

	t.Log(l.String())

	otherFileLock := FileLock{baseURI: tmpPath, key: "_commit.lock"}
	hasLock, err := otherFileLock.TryLock()
	if !errors.Is(err, lock.ErrorLockNotObtained) {
		t.Errorf("err = %e; expected %e", err, lock.ErrorLockNotObtained)
	}
	if hasLock {
		t.Errorf("hasLock = %v; want false", hasLock)
	}

	t.Log(otherFileLock.String())

	l.Unlock()
	hasLock, err = otherFileLock.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !hasLock {
		t.Errorf("hasLock = %v; want true", hasLock)
	}

	t.Log(l.String())
}

func TestNewLock(t *testing.T) {
	tmpDir := t.TempDir()

	tmpPath := storage.NewPath(tmpDir)
	l := New(tmpPath, "_commit.lock", Options{TTL: 2 * time.Second})
	nl, err := l.NewLock("_new_commit.lock")
	if err != nil {
		t.Error(err)
	}

	t.Log(nl.(*FileLock).String())

	if nl.(*FileLock).key != "_new_commit.lock" {
		t.Error("Name of key should be updated")
	}

	locked, err := nl.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !locked {
		t.Errorf("locked = %v; want true", locked)
	}

	t.Log(nl.(*FileLock).String())

	otherFileLock := FileLock{baseURI: tmpPath, key: "_new_commit.lock"}
	hasLock, err := otherFileLock.TryLock()
	if !errors.Is(err, lock.ErrorLockNotObtained) {
		t.Errorf("err = %e; expected %e", err, lock.ErrorLockNotObtained)
	}
	if hasLock {
		t.Errorf("hasLock = %v; want false", hasLock)
	}

	t.Log(otherFileLock.String())

	nl.(*FileLock).Unlock()
	hasLock, err = otherFileLock.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !hasLock {
		t.Errorf("hasLock = %v; want true", hasLock)
	}

	t.Log(nl.(*FileLock).String())
}

func TestTryLockBlocking(t *testing.T) {
	tmpDir := t.TempDir()

	tmpPath := storage.NewPath(tmpDir)
	l := New(tmpPath, "_commit.lock", Options{TTL: 2 * time.Second, Block: true})

	t.Log(l.String())

	locked, err := l.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !locked {
		t.Errorf("locked = %v; want true", locked)
	}

	t.Log(l.String())

	otherFileLock := New(tmpPath, "_commit.lock", Options{TTL: 2 * time.Second, Block: true})
	hasLock, err := otherFileLock.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !hasLock {
		t.Errorf("hasLock = %v; want true", hasLock)
	}

	t.Log(otherFileLock.String())

	l.Unlock()
	hasLock, err = otherFileLock.TryLock()
	if err != nil {
		t.Errorf("err = %e;", err)
	}
	if !hasLock {
		t.Errorf("hasLock = %v; want true", hasLock)
	}

	t.Log(l.String())
}
