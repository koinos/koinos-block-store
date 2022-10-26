package bstore

import (
	"errors"
)

// MapBackend implements a key-value store backed by a simple map
type MapBackend struct {
	storage map[string][]byte
}

// NewMapBackend creates and returns a reference to a map backend instance
func NewMapBackend() *MapBackend {
	return &MapBackend{make(map[string][]byte)}
}

// Reset resets the database
func (backend *MapBackend) Reset() error {
	backend.storage = make(map[string][]byte)
	return nil
}

// Put adds the requested value to the database
func (backend *MapBackend) Put(key []byte, value []byte) error {
	if key == nil {
		return errors.New("cannot put a nil key")
	} else if len(key) == 0 {
		return errors.New("cannot put an empty key")
	}
	if value == nil {
		return errors.New("cannot put a nil value")
	}

	backend.storage[string(key)] = value
	return nil
}

// Delete an item from the database
func (backend *MapBackend) Delete(key []byte) error {
	if key == nil {
		return errors.New("cannot remove a nil key")
	} else if len(key) == 0 {
		return errors.New("cannot remove an empty key")
	}

	delete(backend.storage, string(key))

	return nil
}

// Get fetches the requested value from the database
func (backend *MapBackend) Get(key []byte) ([]byte, error) {
	if key == nil {
		return nil, errors.New("cannot get a nil key")
	} else if len(key) == 0 {
		return nil, errors.New("cannot get an empty key")
	}

	val, ok := backend.storage[string(key)]
	if ok {
		return val, nil
	}

	return make([]byte, 0), nil
}
