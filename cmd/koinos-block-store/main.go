package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dgraph-io/badger"
	"github.com/koinos/koinos-block-store/internal/bstore"
	koinosmq "github.com/koinos/koinos-mq-golang"
	types "github.com/koinos/koinos-types-golang"
	flag "github.com/spf13/pflag"
)

const (
	blockstoreRPC     string = "koinos_block"
	blockAccept       string = "koinos.block.accept"
	blockIrreversible string = "koinos.block.irreversible"
)

func main() {
	var dFlag = flag.StringP("data", "d", "./db", "the database directory")
	var amqpFlag = flag.StringP("amqp", "a", "amqp://guest:guest@localhost:5672/", "AMQP server URL")

	flag.Parse()

	if _, err := os.Stat(*dFlag); os.IsNotExist(err) {
		os.Mkdir(*dFlag, os.ModePerm)
	}
	var backendDir = *dFlag + "/kv"
	var metaDir = *dFlag + "/meta"
	var backendOpts = badger.DefaultOptions(backendDir)
	var metaOpts = badger.DefaultOptions(metaDir)
	var backend = bstore.NewBadgerBackend(backendOpts)
	var metadata = bstore.NewBadgerBackend(metaOpts)
	defer backend.Close()
	defer metadata.Close()

	mq := koinosmq.NewKoinosMQ(*amqpFlag)

	handler := bstore.RequestHandler{Backend: backend, Metadata: metadata}

	mq.SetRPCHandler(blockstoreRPC, func(rpcType string, data []byte) ([]byte, error) {
		//req, ok := rpc.(types.BlockStoreReq)
		req := types.NewBlockStoreRequest()
		err := json.Unmarshal(data, req)
		if err != nil {
			return nil, err
		}

		var resp = types.NewBlockStoreResponse()
		resp = handler.HandleRequest(req)

		var outputBytes []byte
		outputBytes, err = json.Marshal(&resp)

		return outputBytes, err
	})

	mq.SetBroadcastHandler(blockAccept, func(topic string, data []byte) {
		log.Println("Received message on koinos.block.accept")

		sub := types.NewBlockAccepted()
		err := json.Unmarshal(data, sub)
		if err != nil {
			log.Println("Unable to parse BlockAccepted broadcast")
			return
		}

		req := types.BlockStoreRequest{
			Value: &types.AddBlockRequest{
				BlockToAdd: types.BlockItem{
					BlockID:      sub.Topology.ID,
					BlockHeight:  sub.Topology.Height,
					Block:        *types.NewOpaqueBlockFromNative(sub.Block),
					BlockReceipt: *types.NewOpaqueBlockReceiptFromBlob(types.NewVariableBlob()),
				},
				PreviousBlockID: sub.Topology.Previous,
			},
		}
		_ = handler.HandleRequest(&req)
	})

	mq.SetBroadcastHandler(blockIrreversible, func(topic string, data []byte) {
		log.Println("Received message on koinos.block.irreversible")

		broadcastMessage := types.NewBlockIrreversible()
		err := json.Unmarshal(data, broadcastMessage)
		if err != nil {
			log.Println("Unable to parse BlockIrreversible broadcast")
			return
		}

		err = handler.UpdateLastIrreversible(&broadcastMessage.Topology.ID)
		if err != nil {
			log.Println("Error while storing last irreverisible block topology")
			return
		}
	})

	mq.Start()

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Println("Shutting down node...")
}
