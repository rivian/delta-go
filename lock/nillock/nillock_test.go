package nillock

import "testing"

func TestNilLock(t *testing.T) {
	l := New()

	locked, err := l.TryLock()
	if err != nil {
		t.Error(err)
	}
	if !locked {
		t.Error("NilLock's TryLock should always succeed")
	}

	locked, err = l.TryLock()
	if err != nil {
		t.Error(err)
	}
	if !locked {
		t.Error("NilLock's TryLock should always succeed")
	}

	if err := l.Unlock(); err != nil {
		t.Error(err)
	}
}

func TestNewLock(t *testing.T) {
	l := New()
	nl, err := l.NewLock("new-lock")
	if err != nil {
		t.Error(err)
	}

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

	if err := nl.Unlock(); err != nil {
		t.Error(err)
	}
}
