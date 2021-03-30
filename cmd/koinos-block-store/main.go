package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"path"
	"runtime"
	"syscall"

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
	appName           string = "block_store"
)

func main() {
	var baseDir = flag.StringP("basedir", "b", getKoinosDir(), "the base directory")
	var amqp = flag.StringP("amqp", "a", "amqp://guest:guest@localhost:5672/", "AMQP server URL")
	var reset = flag.BoolP("reset", "r", false, "reset the database")

	flag.Parse()

	// Costruct the db directory and ensure it exists
	dbDir := path.Join((*baseDir), appName, "db")
	ensureDir(dbDir)
	log.Println(dbDir)

	var opts = badger.DefaultOptions(dbDir)
	var backend = bstore.NewBadgerBackend(opts)

	// Reset backend if requested
	if *reset {
		log.Println("Resetting the database")
		backend.Reset()
	}

	defer backend.Close()

	mq := koinosmq.NewKoinosMQ(*amqp)

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
		jsonID, _ := json.Marshal(sub.Block.ID)
		jsonPrevious, _ := json.Marshal(sub.Block.Header.Previous)

		log.Printf(" - ID: %s\n", string(jsonID))
		log.Printf(" - Previous: %s\n", string(jsonPrevious))
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

func getHomeDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		panic("There was a problem finding the user's home directory")
	}

	if runtime.GOOS == "windows" {
		home = path.Join(home, "AppData")
	}

	return home
}

func getKoinosDir() string {
	return path.Join(getHomeDir(), ".koinos")
}

func getAppDir(baseDir string, appName string) string {
	return path.Join(baseDir, appName)
}

func ensureDir(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}
}
