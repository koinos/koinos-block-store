
package bstore

import "encoding/hex"
//import "fmt"

type MapBackend struct {
   storage   map[string] []byte
}

func NewMapBackend() *MapBackend {
   return &MapBackend{ make( map[string] []byte ) }
}

func (backend *MapBackend) Put( key []byte, value []byte ) (error) {
   k := hex.EncodeToString( key )
   //fmt.Println("Putting key:", k)
   backend.storage[k] = value
   return nil
}

func (backend *MapBackend) Get( key []byte ) ( []byte, error ) {
   k := hex.EncodeToString( key )
   //fmt.Println("Getting key:", k)
   val, ok := backend.storage[k]
   if ok {
      return val, nil
   } else {
      return nil, nil
   }
}
