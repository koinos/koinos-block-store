package main

import (
	"encoding/hex"
	"encoding/json"
	"errors"
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

type jsonContentTypeHandler struct {
}

func (*jsonContentTypeHandler) FromBytes(data []byte) (interface{}, error) {
	req := types.NewBlockStoreReq()
	err := json.Unmarshal(data, &req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func (*jsonContentTypeHandler) ToBytes(resp interface{}) ([]byte, error) {
	respBytes, err := json.Marshal(&resp)
	if err != nil {
		return nil, err
	}
	return respBytes, nil
}

func main() {
	var dFlag = flag.String("d", "./db", "the database directory")
	var amqpFlag = flag.String("a", "amqp://guest:guest@localhost:5672/", "AMQP server URL")

	var opts = badger.DefaultOptions(*dFlag)
	var backend = bstore.NewBadgerBackend(opts)
	defer backend.Close()

	mq := koinosmq.NewKoinosMQ(*amqpFlag)
	mq.SetContentTypeHandler("application/json", &jsonContentTypeHandler{})

	handler := bstore.RequestHandler{Backend: backend}

	mq.SetRPCHandler("koinos_block", func(rpcType string, rpc interface{}) (interface{}, error) {
		req, ok := rpc.(types.BlockStoreReq)
		if !ok {
			return nil, errors.New("Unexpected request type")
		}
		resp, err := handler.HandleRequest(&req)
		if err != nil {
			return nil, err
		}
		return resp, nil
	})
	mq.SetBroadcastHandler("koinos.block.accept", func(topic string, msg interface{}) {
		// TODO:  Do something with koinos.block.accept message
	})
	mq.Start()
	for {
		time.Sleep(time.Duration(1))
	}
}
