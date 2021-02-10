package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/koinos/koinos-block-store/internal/bstore"
	koinosmq "github.com/koinos/koinos-mq-golang"
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
	var amqpFlag = flag.String("a", "amqp://guest:guest@localhost:5672/", "AMQP server URL")

	var opts = badger.DefaultOptions(*dFlag)
	var backend = bstore.NewBadgerBackend(opts)
	defer backend.Close()

	mq := koinosmq.NewKoinosMQ(*amqpFlag)

	handler := bstore.RequestHandler{Backend: backend}

	mq.SetRPCHandler("koinos_block", func(rpcType string, data []byte) ([]byte, error) {
		//req, ok := rpc.(types.BlockStoreReq)
		req := types.NewBlockStoreReq()
		err := json.Unmarshal(data, req)
		if err != nil {
			return nil, err
		}

		var resp = types.NewBlockStoreResp()
		resp, err = handler.HandleRequest(req)
		if err != nil {
			return nil, err
		}

		var outputBytes []byte
		outputBytes, err = json.Marshal(&resp)

		return outputBytes, err
	})
	mq.SetBroadcastHandler("koinos.block.accept", func(topic string, data []byte) {
		fmt.Println("Received message on koinos.block.accept")

		sub := types.NewBlockSubmission()
		err := json.Unmarshal(data, sub)
		if err != nil {
			return
		}
		opaqueBlock := types.NewOpaqueBlockFromNative(sub.Block)

		req := types.BlockStoreReq{
			Value: &types.AddBlockReq{
				BlockToAdd: types.BlockItem{
					BlockID:     sub.Topology.ID,
					BlockHeight: sub.Topology.Height,
					Block:       *opaqueBlock,
					// TODO: block receipt
				},
				PreviousBlockID: sub.Topology.Previous,
			},
		}
		handler.HandleRequest(&req)

		fmt.Println("Success")
	})
	mq.Start()
	for {
		time.Sleep(time.Duration(1))
	}
}
