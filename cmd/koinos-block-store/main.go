package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/koinos/koinos-block-store/internal/bstore"
	types "github.com/koinos/koinos-block-store/internal/types"
	"os"
)

// Send block to store
//
// Key-value store backend for block data
//

// TODO create block_receipt

func debugTesting() {
	// Some testing stuff
	h, _ := hex.DecodeString("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	block_id := types.Multihash{Id: 0x12, Digest: types.VariableBlob(h)}
	test_req := types.BlockStoreReq{types.GetBlocksByIdReq{BlockId: types.VectorMultihash{block_id}}}
	test_req_json, _ := test_req.MarshalJSON()
	fmt.Println(string(test_req_json))
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

		err := req.UnmarshalJSON([]byte(scanner.Text()))
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
