package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/dgraph-io/badger"
	"github.com/koinos/koinos-block-store/internal/bstore"
	koinosmq "github.com/koinos/koinos-mq-golang"
	types "github.com/koinos/koinos-types-golang"
	flag "github.com/spf13/pflag"
	"gopkg.in/yaml.v2"
)

const (
	basedirOption = "basedir"
	amqpOption    = "amqp"
)

const (
	basedirDefault = ".koinos"
	amqpDefault    = "amqp://guest.guest@localhost:5672/"
)

const (
	blockstoreRPC     string = "block_store"
	blockAccept       string = "koinos.block.accept"
	blockIrreversible string = "koinos.block.irreversible"
	appName           string = "block_store"
)

func main() {
	var baseDir = flag.StringP(basedirOption, "d", basedirDefault, "the base directory")
	var amqp = flag.StringP(amqpOption, "a", "", "AMQP server URL")
	var reset = flag.BoolP("reset", "r", false, "reset the database")

	flag.Parse()

	if !filepath.IsAbs(*baseDir) {
		homedir, err := os.UserHomeDir()
		if err != nil {
			panic(err)
		}
		*baseDir = filepath.Join(homedir, *baseDir)
	}
	ensureDir(*baseDir)

	yamlConfigPath := filepath.Join(*baseDir, "config.yml")
	if _, err := os.Stat(yamlConfigPath); os.IsNotExist(err) {
		yamlConfigPath = filepath.Join(*baseDir, "config.yaml")
	}

	yamlConfig := yamlConfig{}
	if _, err := os.Stat(yamlConfigPath); err == nil {
		data, err := ioutil.ReadFile(yamlConfigPath)
		if err != nil {
			panic(err)
		}

		err = yaml.Unmarshal(data, &yamlConfig)
		if err != nil {
			panic(err)
		}
	} else {
		yamlConfig.Global = make(map[string]interface{})
		yamlConfig.BlockStore = make(map[string]interface{})
	}

	*amqp = getStringOption(amqpOption, amqpDefault, *amqp, yamlConfig.BlockStore, yamlConfig.Global)

	// Costruct the db directory and ensure it exists
	dbDir := path.Join(getAppDir((*baseDir), appName), "db")
	ensureDir(dbDir)
	log.Printf("Opening database at %s", dbDir)

	var opts = badger.DefaultOptions(dbDir)
	var backend = bstore.NewBadgerBackend(opts)

	// Reset backend if requested
	if *reset {
		log.Println("Resetting database")
		err := backend.Reset()
		if err != nil {
			panic(fmt.Sprintf("Error resetting database: %s\n", err.Error()))
		}
	}

	defer backend.Close()

	requestHandler := koinosmq.NewRequestHandler(*amqp)

	handler := bstore.RequestHandler{Backend: backend}

	_, err := handler.GetHighestBlock(types.NewGetHighestBlockRequest())
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

		log.Println("Received RPC request")
		log.Println(" - Request:", string(data))

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

	requestHandler.Start()

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

type yamlConfig struct {
	Global     map[string]interface{} `yaml:"global,omitempty"`
	BlockStore map[string]interface{} `yaml:"block-store,omitempty"`
}

func getStringOption(key string, defaultValue string, cliArg string, configs ...map[string]interface{}) string {
	if cliArg != "" {
		return cliArg
	}

	for _, config := range configs {
		if v, ok := config[key]; ok {
			if option, ok := v.(string); ok {
				return option
			}
		}
	}

	return defaultValue
}

func getAppDir(baseDir string, appName string) string {
	return path.Join(baseDir, appName)
}

func ensureDir(dir string) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.ModePerm)
	}
}
