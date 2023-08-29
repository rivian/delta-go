package nillock

import "testing"

func TestNilLock(t *testing.T) {
	nl := New()

	locked, err := nl.TryLock()
	if err != nil {
		t.Error(err)
	}
	if !locked {
		t.Error("NilLock's TryLock should always succeed")
	}

	locked, err = nl.TryLock()
	if err != nil {
		t.Error(err)
	}
	if !locked {
		t.Error("NilLock's TryLock should always succeed")
	}
	err = nl.Unlock()
	if err != nil {
		t.Error(err)
	}
}

func TestNewLock(t *testing.T) {
	nl := New()
	newNl, err := nl.NewLock("new-lock")
	if err != nil {
		t.Error(err)
	}

	locked, err := newNl.TryLock()
	if err != nil {
		t.Error(err)
	}
	if !locked {
		t.Error("NilLock's TryLock should always succeed")
	}

	locked, err = newNl.TryLock()
	if err != nil {
		t.Error(err)
	}
	if !locked {
		t.Error("NilLock's TryLock should always succeed")
	}
	err = newNl.Unlock()
	if err != nil {
		t.Error(err)
	}
}
