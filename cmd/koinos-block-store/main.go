
package main

import "bufio"
import "encoding/hex"
import "fmt"
import "os"
import "github.com/koinos/koinos-block-store/internal/bstore"
import types "github.com/koinos/koinos-block-store/internal/types"

// Send block to store
//
// Key-value store backend for block data
//

// TODO create block_receipt

func debugTesting() {
   // Some testing stuff
   h, _ := hex.DecodeString("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
   block_id := types.Multihash{ Id : 0x12, Digest : types.VariableBlob( h ) }
   test_req := types.BlockStoreReq{ types.GetBlocksByIdReq{ BlockId : types.VectorMultihash{ block_id } } }
   test_req_json, _ := test_req.MarshalJSON()
   fmt.Println(string(test_req_json))
}

func main() {
   //debugTesting()

   handler := bstore.RequestHandler{}

   scanner := bufio.NewScanner(os.Stdin)
   for scanner.Scan() {
      var req types.BlockStoreReq

      err := req.UnmarshalJSON([]byte(scanner.Text()))
      if err != nil {
         fmt.Println("Couldn't unmarshal request")
         continue
      }

      resp, err := handler.HandleRequest( &req )
      if err != nil {
         fmt.Println("Error:", err)
         continue
      }
      fmt.Println(resp.Value)

      resp_json, err := resp.MarshalJSON()
      if err != nil {
         fmt.Println("Couldn't marshal response")
         continue
      }

      fmt.Println(string(resp_json))
   }

   //op := types.CreateSystemContractOperation{}
   //fmt.Println("hello world")
   //fmt.Println(op)
}
