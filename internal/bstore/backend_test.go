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
	if err := b.Put([]byte("test2"), nil); err == nil {
		t.Error("Putting a nil value should give an error")
	}
	if err := b.Put(nil, []byte("hello")); err == nil {
		t.Error("Putting a nil value should give an error")
	}

	CloseBackend(b)
}

func TestMapBackendBasic(t *testing.T) {
	b := NewBackend(MapBackendType)
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
	if err := b.Put([]byte("test2"), nil); err == nil {
		t.Error("Putting a nil value should give an error")
	}
	if err := b.Put(nil, []byte("hello")); err == nil {
		t.Error("Putting a nil value should give an error")
	}
}
