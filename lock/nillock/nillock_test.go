package nillock

import "testing"

func TestNilLock(t *testing.T) {
	nilLock := new(NilLock)
	locked, err := nilLock.TryLock()
	if err != nil {
		t.Error(err)
	}
	if !locked {
		t.Error("NilLock's TryLock should always succeed")
	}
	locked, err = nilLock.TryLock()
	if err != nil {
		t.Error(err)
	}
	if !locked {
		t.Error("NilLock's TryLock should always succeed")
	}
	err = nilLock.Unlock()
	if err != nil {
		t.Error(err)
	}
}

func TestNewLock(t *testing.T) {
	nilLock, _ := (&NilLock{}).NewLock("", nil, nil)
	locked, err := nilLock.(*NilLock).TryLock()
	if err != nil {
		t.Error(err)
	}
	if !locked {
		t.Error("NilLock's TryLock should always succeed")
	}
	locked, err = nilLock.(*NilLock).TryLock()
	if err != nil {
		t.Error(err)
	}
	if !locked {
		t.Error("NilLock's TryLock should always succeed")
	}
	err = nilLock.(*NilLock).Unlock()
	if err != nil {
		t.Error(err)
	}
}
