package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	base58 "github.com/btcsuite/btcutil/base58"
	"github.com/dgraph-io/badger"
	"github.com/koinos/koinos-block-store/internal/bstore"
	koinosmq "github.com/koinos/koinos-mq-golang"
	types "github.com/koinos/koinos-types-golang"
	flag "github.com/spf13/pflag"
)

const (
	blockstoreRPC     string = "block_store"
	blockAccept       string = "koinos.block.accept"
	blockIrreversible string = "koinos.block.irreversible"
)

func main() {
	var dFlag = flag.StringP("data", "d", "./db", "the database directory")
	var amqpFlag = flag.StringP("amqp", "a", "amqp://guest:guest@localhost:5672/", "AMQP server URL")

	flag.Parse()

	var opts = badger.DefaultOptions(*dFlag)
	var backend = bstore.NewBadgerBackend(opts)
	defer backend.Close()

	mq := koinosmq.NewKoinosMQ(*amqpFlag)

	handler := bstore.RequestHandler{Backend: backend}

	mq.SetRPCHandler(blockstoreRPC, func(rpcType string, data []byte) ([]byte, error) {
		req := types.NewBlockStoreRequest()
		err := json.Unmarshal(data, req)
		if err != nil {
			return nil, err
		}

		log.Println("Received RPC request")
		log.Println(" - Request:", string(data))

		var resp = types.NewBlockStoreResponse()
		resp = handler.HandleRequest(req)

		var outputBytes []byte
		outputBytes, err = json.Marshal(&resp)

		return outputBytes, err
	})

	mq.SetBroadcastHandler(blockAccept, func(topic string, data []byte) {
		sub := types.NewBlockAccepted()
		err := json.Unmarshal(data, sub)
		if err != nil {
			log.Println("Unable to parse BlockAccepted broadcast")
			return
		}

		log.Println("Received broadcasted block")
		jsonPrevious, _ := json.Marshal(sub.Block.Header.Previous)

		log.Printf(" - ID: %v\n", base58.Encode(sub.Block.ID.Digest))
		log.Printf(" - Previous: %v\n", jsonPrevious)
		log.Printf(" - Height: %v\n", sub.Block.Header.Height)

		req := types.BlockStoreRequest{
			Value: &types.AddBlockRequest{
				BlockToAdd: types.BlockItem{
					Block:        *types.NewOpaqueBlockFromNative(sub.Block),
					BlockReceipt: *types.NewOpaqueBlockReceiptFromBlob(types.NewVariableBlob()),
				},
			},
		}
		_ = handler.HandleRequest(&req)

		err = handler.UpdateHighestBlock(&types.BlockTopology{
			ID:       sub.Block.ID,
			Height:   sub.Block.Header.Height,
			Previous: sub.Block.Header.Previous,
		})
		if err != nil {
			log.Println("Error while updating highest block")
		}
	})

	mq.Start()

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Println("Shutting down node...")
}
