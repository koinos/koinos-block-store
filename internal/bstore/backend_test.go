package bstore

import (
	"bytes"
	"testing"
)

func TestBadgerBackendBasic(t *testing.T) {
	b := NewBackend(BadgerBackendType)
	e := b.Put([]byte("test"), []byte("case"))
	if e != nil {
		t.Error(e)
	}
	v, e := b.Get([]byte("test"))
	if e != nil {
		t.Error(e)
	}
	if !bytes.Equal(v, []byte("case")) {
		t.Errorf("error: slice not equivalent")
	}
	v, e = b.Get([]byte("notfound"))
	if len(v) != 0 {
		t.Errorf("expected empty slice")
	}
	if e != nil {
		t.Error("expected no error, received:", e)
	}
	e = b.Put([]byte("test"), []byte("second"))
	if e != nil {
		t.Error(e)
	}
	v, e = b.Get([]byte("test"))
	if e != nil {
		t.Error(e)
	}
	if !bytes.Equal(v, []byte("second")) {
		t.Errorf("error: slice not equivalent")
	}
	if err := b.Put([]byte("test2"), nil); err == nil {
		t.Error("putting a nil value should give an error")
	}
	if err := b.Put(nil, []byte("hello")); err == nil {
		t.Error("putting a nil value should give an error")
	}
	_, e = b.Get([]byte{})
	if e == nil {
		t.Error("expected error empty key")
	}
	_, e = b.Get(nil)
	if e == nil {
		t.Error("expected error empty key")
	}

	// Test reset

	// First put new value into database
	e = b.Put([]byte("test_reset"), []byte("val"))
	if e != nil {
		t.Error(e)
	}
	v, e = b.Get([]byte("test_reset"))
	if e != nil {
		t.Error(e)
	}
	if !bytes.Equal(v, []byte("val")) {
		t.Errorf("error: slice not equivalent")
	}

	// Reset the database
	err := b.Reset()
	if err != nil {
		t.Error(err)
	}

	// Ensure the value is gone
	v, e = b.Get([]byte("test_reset"))
	if e != nil {
		t.Error(e)
	}
	if len(v) != 0 {
		t.Errorf("expected empty slice")
	}

	CloseBackend(b)
}

func TestMapBackendBasic(t *testing.T) {
	b := NewBackend(MapBackendType)
	e := b.Put([]byte("test"), []byte("case"))
	if e != nil {
		t.Error(e)
	}
	v, e := b.Get([]byte("test"))
	if e != nil {
		t.Error(e)
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
	e = b.Put([]byte("test"), []byte("second"))
	if e != nil {
		t.Error(e)
	}
	v, e = b.Get([]byte("test"))
	if e != nil {
		t.Error(e)
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
	_, e = b.Get([]byte{})
	if e == nil {
		t.Error("expected error empty key")
	}
	_, e = b.Get(nil)
	if e == nil {
		t.Error("expected error empty key")
	}

	// Test reset

	// First put new value into database
	e = b.Put([]byte("test_reset"), []byte("val"))
	if e != nil {
		t.Error(e)
	}
	v, e = b.Get([]byte("test_reset"))
	if e != nil {
		t.Error(e)
	}
	if !bytes.Equal(v, []byte("val")) {
		t.Errorf("error: slice not equivalent")
	}

	// Reset the database
	err := b.Reset()
	if err != nil {
		t.Error(err)
	}

	// Ensure the value is gone
	v, e = b.Get([]byte("test_reset"))
	if e != nil {
		t.Error(e)
	}
	if len(v) != 0 {
		t.Errorf("expected empty slice")
	}
}
