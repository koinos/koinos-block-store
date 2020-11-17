
package bstore

import "crypto/sha256"
import "encoding/binary"
import "fmt"
import "testing"

import . "github.com/koinos/koinos-block-store/internal/types"

func TestHandleReservedRequest( t *testing.T ) {
   handler := RequestHandler{ NewMapBackend() }

   test_req := BlockStoreReq{ ReservedReq{} }
   result, err := handler.HandleRequest( &test_req )
   if result != nil {
      t.Error("Result should be nil")
   }
   if err == nil {
      t.Error("Err should not be nil")
   }

   if _, ok := err.(*ReservedReqError) ; !ok {
      t.Error("Err should be ReservedReqError")
   }
}

type UnknownReq struct {
}

func TestHandleUnknownRequestType( t *testing.T ) {
   handler := RequestHandler{ NewMapBackend() }

   test_req := BlockStoreReq{ UnknownReq{} }
   result, err := handler.HandleRequest( &test_req )
   if result != nil {
      t.Error("Result should be nil")
   }
   if err == nil {
      t.Error("Err should not be nil")
   }

   if _, ok := err.(*UnknownReqError) ; !ok {
      t.Error("Err should be UnknownReqError")
   }
}

func SliceEqual( a []uint64, b []uint64 ) bool {
   n := len(a)
   if len(b) != n {
      return false
   }
   for i := 0; i < n; i++ {
      if a[i] != b[i] {
         return false
      }
   }
   return true
}

func TestGetPreviousHeights( t *testing.T ) {
   test_cases := [][][]uint64 {
      {{ 0}, {}},
      {{ 1}, {0}},
      {{ 2}, {1, 0}},
      {{ 3}, {2}},
      {{ 4}, {3, 2, 0}},
      {{ 5}, {4}},
      {{ 6}, {5, 4}},
      {{ 7}, {6}},
      {{ 8}, {7, 6, 4, 0}},
      {{ 9}, {8}},
      {{10}, {9, 8}},
      {{11}, {10}},
      {{12}, {11, 10, 8}},
      {{13}, {12}},
      {{14}, {13, 12}},
      {{15}, {14}},
      {{16}, {15, 14, 12, 8, 0}},
      {{17}, {16}},
   }

   for i := 0; i < len(test_cases); i++ {
      x := test_cases[i][0][0]
      y_ref := test_cases[i][1]
      y_test := GetPreviousHeights( x )

      if ! SliceEqual( y_ref, y_test ) {
         t.Errorf( "Testing %d, expected %v, got %v", x, y_ref, y_test )
      }
   }
}

func GetBlockId( num uint64 ) Multihash {
   if num == 0 {
      return GetEmptyBlockId()
   }
   data_bytes := make([]byte, binary.MaxVarintLen64)
   count := binary.PutUvarint(data_bytes, num)

   hash := sha256.Sum256( data_bytes[:count] )

   var vb VariableBlob = VariableBlob( hash[:] )

   return Multihash{ 0x12, vb }
   // return Multihash{ 0x12, data_bytes[:count] }
}

func GetEmptyBlockId() Multihash {
   vb := VariableBlob( make([]byte, 32) )
   return Multihash{ 0x12, vb }
}

func GetBlockBody(num uint64) VariableBlob {
   greetings := []string {
      "Hello this is block %d.",
      "Greetings from block %d.",
      "I like being in block %d.",
   }

   return []byte( fmt.Sprintf(greetings[int(num)%len(greetings)], num) )
}

func TestAddBlocks( t *testing.T ) {
   // A compact notation of the tree of forks we want to create for the test
   tree := [][]uint64 {
      {0, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120},
      {             103, 204, 205, 206, 207, 208, 209, 210, 211                                             },
      {             103, 304, 305, 306, 307                                                                 },
      {                            106, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 419     },
      {                                           109, 510, 511                                             },
      {                                                          112, 613, 614                              },
   }

   handler := RequestHandler{ NewMapBackend() }
   for i := 0; i < len(tree); i++ {
      for j := 1; j < len(tree[i]); j++ {
         block_id := GetBlockId(tree[i][j])
         parent_id := GetBlockId(tree[i][j-1])

         // fmt.Printf("Block %d has ID %v\n", tree[i][j], hex.EncodeToString( block_id.Digest ) );
         add_req := AddBlockReq{}
         add_req.BlockToAdd.BlockId = block_id
         add_req.PreviousBlockId = parent_id
         add_req.BlockToAdd.BlockHeight = BlockHeightType( tree[i][j] % 100 )
         add_req.BlockToAdd.BlockBlob = GetBlockBody(tree[i][j])
         add_req.BlockToAdd.BlockReceiptBlob = VariableBlob( make([]byte, 0) )

         generic_req := BlockStoreReq{ add_req }

         json, err := generic_req.MarshalJSON()
         if err != nil {
            t.Error("Could not marshal JSON", err)
         }
         fmt.Printf("%s\n", string(json))

         result, err := handler.HandleRequest( &generic_req )
         if err != nil {
            t.Error("Got error adding block", err)
         }
         if result == nil {
            t.Error("Got nil result")
         }
      }
   }
}
