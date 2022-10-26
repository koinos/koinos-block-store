package bstore

import (
	"errors"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"go.uber.org/zap"
)

// BadgerBackend Badger backend implementation
type BadgerBackend struct {
	DB *badger.DB
}

// NewBadgerBackend BadgerBackend constructor
func NewBadgerBackend(opts badger.Options) (*BadgerBackend, error) {
	badgerDB, err := badger.Open(opts)
	return &BadgerBackend{DB: badgerDB}, err
}

// Close cleans backend resources
func (backend *BadgerBackend) Close() {
	backend.DB.Close()
}

// Reset resets the database
func (backend *BadgerBackend) Reset() error {
	return backend.DB.DropAll()
}

// Put backend setter
func (backend *BadgerBackend) Put(key, value []byte) error {
	if key == nil {
		return errors.New("cannot put a nil key")
	}
	if value == nil {
		return errors.New("cannot put a nil value")
	}

	return backend.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Delete an item from the database
func (backend *BadgerBackend) Delete(key []byte) error {
	if key == nil {
		return errors.New("cannot remove a nil key")
	}

	return backend.DB.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// Get backend getter
func (backend *BadgerBackend) Get(key []byte) ([]byte, error) {
	var value []byte = nil

	if key == nil {
		return value, errors.New("cannot get a nil key")
	}

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

// KoinosBadgerLogger implements the badger.Logger interface in roder to pass badger logs the the koinos logger
type KoinosBadgerLogger struct {
}

// Errorf implements formatted error message handling for badger
func (kbl KoinosBadgerLogger) Errorf(msg string, args ...interface{}) {
	zap.S().Errorf(strings.TrimSpace(msg), args...)
}

// Warningf implements formatted warning message handling for badger
func (kbl KoinosBadgerLogger) Warningf(msg string, args ...interface{}) {
	zap.S().Warnf(strings.TrimSpace(msg), args...)
}

// Infof implements formatted info message handling for badger
func (kbl KoinosBadgerLogger) Infof(msg string, args ...interface{}) {
	zap.S().Infof(strings.TrimSpace(msg), args...)
}

// Debugf implements formatted debug message handling for badger
func (kbl KoinosBadgerLogger) Debugf(msg string, args ...interface{}) {
	zap.S().Debugf(strings.TrimSpace(msg), args...)
}
