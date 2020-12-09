package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/koinos/koinos-block-store/internal/bstore"
	types "github.com/koinos/koinos-types-golang"
)

// Send block to store
//
// Key-value store backend for block data
//

// TODO create block_receipt

func debugTesting() {
	// Some testing stuff
	h, _ := hex.DecodeString("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	blockID := types.Multihash{ID: 0x12, Digest: types.VariableBlob(h)}
	testReq := types.BlockStoreReq{Value: &types.GetBlocksByIDReq{BlockID: types.VectorMultihash{blockID}}}
	testReqJSON, _ := testReq.MarshalJSON()
	fmt.Println(string(testReqJSON))
}

func main() {
	var dFlag = flag.String("d", "./db", "the database directory")

	var opts = badger.DefaultOptions(*dFlag)
	var backend = bstore.NewBadgerBackend(opts)
	defer backend.Close()

	handler := bstore.RequestHandler{Backend: backend}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var req types.BlockStoreReq

		b := types.BlockStoreReq{}
		err := json.Unmarshal([]byte(scanner.Text()), &b)
		if err != nil {
			fmt.Println("Couldn't unmarshal request")
			continue
		}

		resp, err := handler.HandleRequest(&req)
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}
		fmt.Println(resp.Value)

		respJSON, err := json.Marshal(resp)
		if err != nil {
			fmt.Println("Couldn't marshal response")
			continue
		}

		fmt.Println(string(respJSON))
	}

	//op := types.CreateSystemContractOperation{}
	//fmt.Println("hello world")
	//fmt.Println(op)
}
