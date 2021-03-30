package bstore

import (
	"errors"

	"github.com/dgraph-io/badger"
)

// BadgerBackend Badger backend implementation
type BadgerBackend struct {
	DB *badger.DB
}

// NewBadgerBackend BadgerBackend constructor
func NewBadgerBackend(opts badger.Options) *BadgerBackend {
	badgerDB, _ := badger.Open(opts)
	return &BadgerBackend{DB: badgerDB}
}

// Close cleans backend resources
func (backend *BadgerBackend) Close() {
	backend.DB.Close()
}

// Reset
func (backend *BadgerBackend) Reset() error {
	return backend.DB.DropAll()
}

// Put backend setter
func (backend *BadgerBackend) Put(key, value []byte) error {
	if value == nil {
		return errors.New("Cannot put a nil value")
	}
	return backend.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Get backend getter
func (backend *BadgerBackend) Get(key []byte) ([]byte, error) {
	var value []byte = nil
	err := backend.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			value = make([]byte, 0)
			return nil
		} else if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			value = append([]byte{}, val...)
			return nil
		})
		return err
	})

	return value, err
}
