package bstore

import (
	"encoding/hex"
	"errors"
)

//import "fmt"

type MapBackend struct {
	storage map[string][]byte
}

func NewMapBackend() *MapBackend {
	return &MapBackend{make(map[string][]byte)}
}

func (backend *MapBackend) Put(key []byte, value []byte) error {
	if value == nil {
		return errors.New("Cannot put a nil value")
	}
	k := hex.EncodeToString(key)
	//fmt.Println("Putting key:", k)
	backend.storage[k] = value
	return nil
}

func (backend *MapBackend) Get(key []byte) ([]byte, error) {
	k := hex.EncodeToString(key)
	//fmt.Println("Getting key:", k)
	val, ok := backend.storage[k]
	if ok {
		return val, nil
	} else {
		return make([]byte, 0), nil
	}
}
