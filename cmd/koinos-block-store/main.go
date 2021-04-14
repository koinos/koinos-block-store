package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/dgraph-io/badger"
	"github.com/koinos/koinos-block-store/internal/bstore"
	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	types "github.com/koinos/koinos-types-golang"
	util "github.com/koinos/koinos-util-golang"
	flag "github.com/spf13/pflag"
)

const (
	basedirOption    = "basedir"
	amqpOption       = "amqp"
	instanceIDOption = "instance-id"
	logLevelOption   = "log-level"
)

const (
	basedirDefault    = ".koinos"
	amqpDefault       = "amqp://guest.guest@localhost:5672/"
	instanceIDDefault = ""
	logLevelDefault   = "info"
)

const (
	blockstoreRPC     = "block_store"
	blockAccept       = "koinos.block.accept"
	blockIrreversible = "koinos.block.irreversible"
	appName           = "block_store"
	logDir            = "logs"
)

func main() {
	var baseDir = flag.StringP(basedirOption, "d", basedirDefault, "the base directory")
	var amqp = flag.StringP(amqpOption, "a", "", "AMQP server URL")
	var reset = flag.BoolP("reset", "r", false, "reset the database")
	instanceID := flag.StringP(instanceIDOption, "i", instanceIDDefault, "The instance ID to identify this service")
	logLevel := flag.StringP(logLevelOption, "v", logLevelDefault, "The log filtering level (debug, info, warn, error)")

	flag.Parse()

	*baseDir = util.InitBaseDir(*baseDir)

	yamlConfig := util.InitYamlConfig(*baseDir)

	*amqp = util.GetStringOption(amqpOption, amqpDefault, *amqp, yamlConfig.BlockStore, yamlConfig.Global)

	// Generate Instance ID
	if *instanceID == "" {
		*instanceID = util.GenerateBase58ID(5)
	}

	appID := fmt.Sprintf("%s.%s", appName, *instanceID)

	// Initialize logger
	logFilename := path.Join(util.GetAppDir(*baseDir, appName), logDir, "p2p.log")
	err := log.InitLogger(*logLevel, false, logFilename, appID)
	if err != nil {
		panic(fmt.Sprintf("Invalid log-level: %s. Please choose one of: debug, info, warn, error", *logLevel))
	}

	// Costruct the db directory and ensure it exists
	dbDir := path.Join(util.GetAppDir((*baseDir), appName), "db")
	util.EnsureDir(dbDir)
	log.Infof("Opening database at %s", dbDir)

	var opts = badger.DefaultOptions(dbDir)
	var backend = bstore.NewBadgerBackend(opts)

	// Reset backend if requested
	if *reset {
		log.Info("Resetting database")
		err := backend.Reset()
		if err != nil {
			panic(fmt.Sprintf("Error resetting database: %s\n", err.Error()))
		}
	}

	defer backend.Close()

	requestHandler := koinosmq.NewRequestHandler(*amqp)

	handler := bstore.RequestHandler{Backend: backend}

	_, err = handler.GetHighestBlock(types.NewGetHighestBlockRequest())
	if err != nil {
		if _, ok := err.(*bstore.UnexpectedHeightError); ok {
			handler.UpdateHighestBlock(&types.BlockTopology{
				Height: 0,
			})
		}
	}

	requestHandler.SetRPCHandler(blockstoreRPC, func(rpcType string, data []byte) ([]byte, error) {
		req := types.NewBlockStoreRequest()
		err := json.Unmarshal(data, req)
		if err != nil {
			return nil, err
		}

		log.Info("Received RPC request")
		log.Infof(" - Request: %s", string(data))

		var resp = types.NewBlockStoreResponse()
		resp = handler.HandleRequest(req)

		var outputBytes []byte
		outputBytes, err = json.Marshal(&resp)

		return outputBytes, err
	})

	requestHandler.SetBroadcastHandler(blockAccept, func(topic string, data []byte) {
		sub := types.NewBlockAccepted()
		err := json.Unmarshal(data, sub)
		if err != nil {
			log.Warn("Unable to parse BlockAccepted broadcast")
			return
		}

		log.Infof("Received broadcasted block - %s", util.BlockString(&sub.Block))

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
			log.Warn("Error while updating highest block")
		}
	})

	requestHandler.Start()

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Info("Shutting down node...")
}
