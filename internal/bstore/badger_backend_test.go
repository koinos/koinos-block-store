package bstore

import (
	"bytes"
	"testing"
)

func TestBadgerBackendBasic(t *testing.T) {
	b := NewBackend(BadgerBackendType)
	b.Put([]byte("test"), []byte("case"))
	v, e := b.Get([]byte("test"))
	if e != nil {
		t.Errorf("error: %s", e)
	}
	if !bytes.Equal(v, []byte("case")) {
		t.Errorf("error: slice not equivalent")
	}
	v, e = b.Get([]byte("notfound"))
	if len(v) != 0 {
		t.Errorf("expected empty slice")
	}
	if e != nil {
		t.Error("Expected no error, recieved:", e)
	}
	b.Put([]byte("test"), []byte("second"))
	v, e = b.Get([]byte("test"))
	if e != nil {
		t.Errorf("error: %s", e)
	}
	if !bytes.Equal(v, []byte("second")) {
		t.Errorf("error: slice not equivalent")
	}
	CloseBackend(b)
}
