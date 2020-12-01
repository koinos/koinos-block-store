package bstore

import (
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

// Put backend setter
func (backend *BadgerBackend) Put(key, value []byte) error {
	return backend.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Get backend getter
func (backend *BadgerBackend) Get(key []byte) ([]byte, error) {
	var value []byte = nil
	err := backend.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
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
