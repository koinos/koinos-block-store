package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/dgraph-io/badger"
	"github.com/koinos/koinos-block-store/internal/bstore"
	log "github.com/koinos/koinos-log-golang"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-proto-golang/koinos"
	"github.com/koinos/koinos-proto-golang/koinos/broadcast"
	"github.com/koinos/koinos-proto-golang/koinos/rpc"
	"github.com/koinos/koinos-proto-golang/koinos/rpc/block_store"
	util "github.com/koinos/koinos-util-golang"
	"github.com/multiformats/go-multihash"
	flag "github.com/spf13/pflag"
	"google.golang.org/protobuf/proto"
)

const (
	basedirOption    = "basedir"
	amqpOption       = "amqp"
	instanceIDOption = "instance-id"
	logLevelOption   = "log-level"
	resetOption      = "reset"
)

const (
	basedirDefault    = ".koinos"
	amqpDefault       = "amqp://guest:guest@localhost:5672/"
	instanceIDDefault = ""
	logLevelDefault   = "info"
	resetDefault      = false
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
	var reset = flag.BoolP(resetOption, "r", resetDefault, "reset the database")
	instanceID := flag.StringP(instanceIDOption, "i", instanceIDDefault, "The instance ID to identify this service")
	logLevel := flag.StringP(logLevelOption, "v", logLevelDefault, "The log filtering level (debug, info, warn, error)")

	flag.Parse()

	*baseDir = util.InitBaseDir(*baseDir)

	yamlConfig := util.InitYamlConfig(*baseDir)

	*amqp = util.GetStringOption(amqpOption, amqpDefault, *amqp, yamlConfig.BlockStore, yamlConfig.Global)
	*logLevel = util.GetStringOption(logLevelOption, logLevelDefault, *logLevel, yamlConfig.BlockStore, yamlConfig.Global)
	*instanceID = util.GetStringOption(instanceIDOption, util.GenerateBase58ID(5), *instanceID, yamlConfig.BlockStore, yamlConfig.Global)
	*reset = util.GetBoolOption(resetOption, resetDefault, *reset, yamlConfig.BlockStore, yamlConfig.Global)

	appID := fmt.Sprintf("%s.%s", appName, *instanceID)

	// Initialize logger
	logFilename := path.Join(util.GetAppDir(*baseDir, appName), logDir, "block_store.log")
	err := log.InitLogger(*logLevel, false, logFilename, appID)
	if err != nil {
		fmt.Sprintf("Invalid log-level: %s. Please choose one of: debug, info, warn, error", *logLevel)
		os.Exit(1)
	}

	// Costruct the db directory and ensure it exists
	dbDir := path.Join(util.GetAppDir((*baseDir), appName), "db")
	util.EnsureDir(dbDir)
	log.Infof("Opening database at %s", dbDir)

	var opts = badger.DefaultOptions(dbDir)
	opts.Logger = bstore.KoinosBadgerLogger{}
	backend, err := bstore.NewBadgerBackend(opts)

	if err != nil {
		log.Errorf("Could not open database, %s", err.Error())
		os.Exit(1)
	}

	// Reset backend if requested
	if *reset {
		log.Info("Resetting database")
		err := backend.Reset()
		if err != nil {
			log.Errorf("Could not reset database, %s", err.Error())
			os.Exit(1)
		}
	}

	defer backend.Close()

	requestHandler := koinosmq.NewRequestHandler(*amqp)

	handler := bstore.RequestHandler{Backend: backend}

	_, err = handler.GetHighestBlock(&block_store.GetHighestBlockRequest{})
	if err != nil {
		if _, ok := err.(*bstore.UnexpectedHeightError); ok {
			mh, _ := multihash.EncodeName(make([]byte, 32), "sha2-256")
			bt := koinos.BlockTopology{Id: mh, Height: 0}
			handler.UpdateHighestBlock(&bt)
		}
	}

	requestHandler.SetRPCHandler(blockstoreRPC, func(rpcType string, data []byte) ([]byte, error) {
		req := &block_store.BlockStoreRequest{}
		resp := &block_store.BlockStoreResponse{}

		err := proto.Unmarshal(data, req)
		if err != nil {
			log.Warnf("Received malformed request: 0x%v", hex.EncodeToString(data))
			eResp := rpc.ErrorResponse{Message: err.Error()}
			rErr := block_store.BlockStoreResponse_Error{Error: &eResp}
			resp.Response = &rErr
		} else {
			log.Debugf("Received RPC request: 0x%v", hex.EncodeToString(data))
			resp = handler.HandleRequest(req)
		}

		var outputBytes []byte
		outputBytes, err = proto.Marshal(resp)

		return outputBytes, err
	})

	requestHandler.SetBroadcastHandler(blockAccept, func(topic string, data []byte) {
		sub := broadcast.BlockAccepted{}
		err := proto.Unmarshal(data, &sub)
		if err != nil {
			log.Warnf("Unable to parse koinos.block.accept broadcast: %s", string(data))
			return
		}

		log.Infof("Received broadcasted block - Height: %d, ID: 0x%s", sub.Block.Header.Height, hex.EncodeToString(sub.Block.Id))

		iReq := block_store.AddBlockRequest{
			BlockToAdd:   sub.GetBlock(),
			ReceiptToAdd: sub.GetReceipt(),
		}
		bsReq := block_store.BlockStoreRequest_AddBlock{AddBlock: &iReq}
		req := block_store.BlockStoreRequest{Request: &bsReq}

		_ = handler.HandleRequest(&req)

		err = handler.UpdateHighestBlock(&koinos.BlockTopology{
			Id:       sub.GetBlock().GetId(),
			Height:   sub.GetBlock().GetHeader().GetHeight(),
			Previous: sub.GetBlock().GetHeader().GetPrevious(),
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
