package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"path"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/koinos/koinos-block-store/internal/bstore"
	log "github.com/koinos/koinos-log-golang/v2"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-proto-golang/v2/koinos"
	"github.com/koinos/koinos-proto-golang/v2/koinos/broadcast"
	"github.com/koinos/koinos-proto-golang/v2/koinos/rpc"
	"github.com/koinos/koinos-proto-golang/v2/koinos/rpc/block_store"
	util "github.com/koinos/koinos-util-golang/v2"
	"github.com/multiformats/go-multihash"
	flag "github.com/spf13/pflag"
	"google.golang.org/protobuf/proto"
)

const (
	basedirOption     = "basedir"
	amqpOption        = "amqp"
	instanceIDOption  = "instance-id"
	logLevelOption    = "log-level"
	logDirOption      = "log-dir"
	logColorOption    = "log-color"
	logDatetimeOption = "log-datetime"
	resetOption       = "reset"
	jobsOption        = "jobs"
	versionOption     = "version"
)

const (
	basedirDefault     = ".koinos"
	amqpDefault        = "amqp://guest:guest@localhost:5672/"
	instanceIDDefault  = ""
	logLevelDefault    = "info"
	logColorDefault    = true
	logDatetimeDefault = true
	resetDefault       = false
)

const (
	blockstoreRPC  = "block_store"
	blockAccept    = "koinos.block.accept"
	appName        = "block_store"
	maxMessageSize = 536870912
)

// Version display values
const (
	DisplayAppName = "Koinos Block Store"
	Version        = "v1.1.0"
)

// Gets filled in by the linker
var Commit string

func main() {
	jobsDefault := runtime.NumCPU()

	baseDirPtr := flag.StringP(basedirOption, "d", basedirDefault, "Koinos base directory")
	amqp := flag.StringP(amqpOption, "a", "", "AMQP server URL")
	reset := flag.BoolP(resetOption, "r", resetDefault, "Reset the database")
	instanceID := flag.StringP(instanceIDOption, "i", instanceIDDefault, "The instance ID to identify this service")
	logLevel := flag.StringP(logLevelOption, "l", logLevelDefault, "The log filtering level (debug, info, warning, error)")
	logDir := flag.String(logDirOption, "", "The logging directory")
	logColor := flag.Bool(logColorOption, logColorDefault, "Log color toggle")
	logDatetime := flag.Bool(logDatetimeOption, logDatetimeDefault, "Log datetime on console toggle")
	jobs := flag.IntP(jobsOption, "j", jobsDefault, "Number of RPC jobs to run")
	version := flag.BoolP(versionOption, "v", false, "Print version and exit")

	flag.Parse()

	if *version {
		fmt.Println(makeVersionString())
		os.Exit(0)
	}

	baseDir, err := util.InitBaseDir(*baseDirPtr)
	if err != nil {
		fmt.Printf("Could not initialize base directory '%v'\n", baseDir)
		os.Exit(1)
	}

	yamlConfig := util.InitYamlConfig(baseDir)

	*amqp = util.GetStringOption(amqpOption, amqpDefault, *amqp, yamlConfig.BlockStore, yamlConfig.Global)
	*logLevel = util.GetStringOption(logLevelOption, logLevelDefault, *logLevel, yamlConfig.BlockStore, yamlConfig.Global)
	*logDir = util.GetStringOption(logDirOption, *logDir, *logDir, yamlConfig.BlockStore, yamlConfig.Global)
	*logColor = util.GetBoolOption(logColorOption, logColorDefault, *logColor, yamlConfig.BlockStore, yamlConfig.Global)
	*logDatetime = util.GetBoolOption(logDatetimeOption, logDatetimeDefault, *logDatetime, yamlConfig.BlockStore, yamlConfig.Global)
	*instanceID = util.GetStringOption(instanceIDOption, util.GenerateBase58ID(5), *instanceID, yamlConfig.BlockStore, yamlConfig.Global)
	*reset = util.GetBoolOption(resetOption, resetDefault, *reset, yamlConfig.BlockStore, yamlConfig.Global)
	*jobs = util.GetIntOption(jobsOption, jobsDefault, *jobs, yamlConfig.BlockStore, yamlConfig.Global)

	if len(*logDir) > 0 && !path.IsAbs(*logDir) {
		*logDir = path.Join(util.GetAppDir(baseDir, appName), *logDir)
	}

	err = log.InitLogger(appName, *instanceID, *logLevel, *logDir, *logColor, *logDatetime)
	if err != nil {
		fmt.Printf("Invalid log-level: %s. Please choose one of: debug, info, warning, error", *logLevel)
		os.Exit(1)
	}

	log.Info(makeVersionString())

	if *jobs < 1 {
		log.Errorf("Option '%v' must be greater than 0 (was %v)", jobsOption, *jobs)
		os.Exit(1)
	}

	// Costruct the db directory and ensure it exists
	dbDir := path.Join(util.GetAppDir((baseDir), appName), "db")
	err = util.EnsureDir(dbDir)
	if err != nil {
		log.Errorf("Could not create database folder %v", dbDir)
		os.Exit(1)
	}

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

	requestHandler := koinosmq.NewRequestHandler(*amqp, uint(*jobs), koinosmq.ExponentialBackoff)

	handler := bstore.RequestHandler{Backend: backend}

	if _, err = handler.GetHighestBlock(&block_store.GetHighestBlockRequest{}); err != nil {
		if _, ok := err.(*bstore.UnexpectedHeightError); ok {
			mh, _ := multihash.EncodeName(make([]byte, 32), "sha2-256")
			bt := koinos.BlockTopology{Id: mh, Height: 0}
			if err := handler.UpdateHighestBlock(&bt); err != nil {
				log.Warnf("Unable to update highest block: %s", err)
			}
		}
	}

	requestHandler.SetRPCHandler(blockstoreRPC, func(rpcType string, data []byte) ([]byte, error) {
		req := &block_store.BlockStoreRequest{}
		resp := &block_store.BlockStoreResponse{}

		err := proto.Unmarshal(data, req)
		if err != nil {
			log.Warnf("Received malformed request: 0x%v", hex.EncodeToString(data))
			eResp := rpc.ErrorStatus{Message: err.Error()}
			rErr := block_store.BlockStoreResponse_Error{Error: &eResp}
			resp.Response = &rErr
		} else {
			log.Debugf("Received RPC request: 0x%v", hex.EncodeToString(data))
			resp = handler.HandleRequest(req)
		}

		var outputBytes []byte
		outputBytes, err = proto.Marshal(resp)

		if len(outputBytes) > maxMessageSize {
			eResp := rpc.ErrorStatus{Message: "Response would exceed maximum MQ message size"}
			rErr := block_store.BlockStoreResponse_Error{Error: &eResp}
			resp.Response = &rErr
			outputBytes, err = proto.Marshal(resp)
		}

		return outputBytes, err
	})

	var recentBlocks uint32

	requestHandler.SetBroadcastHandler(blockAccept, func(topic string, data []byte) {
		sub := broadcast.BlockAccepted{}
		err := proto.Unmarshal(data, &sub)
		if err != nil {
			log.Warnf("Unable to parse koinos.block.accept broadcast: %s", string(data))
			return
		}

		if sub.GetLive() {
			log.Debugf("Received broadcasted block - Height: %d, ID: 0x%s", sub.Block.Header.Height, hex.EncodeToString(sub.Block.Id))
		} else if sub.GetBlock().GetHeader().GetHeight()%1000 == 0 {
			log.Infof("Sync block progress - Height: %d, ID: 0x%s", sub.Block.Header.Height, hex.EncodeToString(sub.Block.Id))
		}

		atomic.AddUint32(&recentBlocks, 1)

		iReq := block_store.AddBlockRequest{
			BlockToAdd:   sub.GetBlock(),
			ReceiptToAdd: sub.GetReceipt(),
		}
		bsReq := block_store.BlockStoreRequest_AddBlock{AddBlock: &iReq}
		req := block_store.BlockStoreRequest{Request: &bsReq}

		_ = handler.HandleRequest(&req)
	})

	ctx, ctxCancel := context.WithCancel(context.Background())
	requestHandler.Start(ctx)

	go func() {
		for {
			select {
			case <-time.After(60 * time.Second):
				numBlocks := atomic.SwapUint32(&recentBlocks, 0)

				if numBlocks > 0 {
					log.Infof("Recently added %v block(s)", numBlocks)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for a SIGINT or SIGTERM signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Info("Shutting down node...")
	ctxCancel()
	backend.Close()
}

func makeVersionString() string {
	commitString := ""
	if len(Commit) >= 8 {
		commitString = fmt.Sprintf("(%s)", Commit[0:8])
	}

	return fmt.Sprintf("%s %s %s", DisplayAppName, Version, commitString)
}
