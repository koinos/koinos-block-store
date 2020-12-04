package main

import (
	"flag"
	"github.com/dgraph-io/badger"
	"github.com/koinos/koinos-block-store/internal/bstore"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
)

// Networks allows for multiple networks to be defined on the command line
type Networks []string

// String representation of Networks
func (i *Networks) String() string {
	return strings.Join(*i, ", ")
}

// Set method for Networks
func (i *Networks) Set(value string) error {
	*i = append(*i, value)
	return nil
}

// Addresses allows for multiple networks to be defined on the command line
type Addresses []string

// String representation of Addresses
func (i *Addresses) String() string {
	return strings.Join(*i, ", ")
}

// Set method for Addresses
func (i *Addresses) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func main() {
	var networks Networks
	var addresses Addresses
	flag.Var(&networks, "n", "Set the network to listen on, multiple values are supported, valid options are (tcp, tcp4, tcp6, unix, unixpacket)")
	flag.Var(&addresses, "a", "Set the server address to listen on, multiple values are supported, valid options are (0.0.0.0:8100, [::1]:8100)")

	databaseDirectory := flag.String("d", "./db", "The database directory")
	endpoint := flag.String("e", "/rpc", "Set the HTTP endpoint to listen on")
	stdinFlag := flag.Bool("stdin", false, "Listen for requests on stdin")
	flag.Parse()

	sync := &sync.WaitGroup{}

	opts := badger.DefaultOptions(*databaseDirectory)
	backend := bstore.NewBadgerBackend(opts)
	defer backend.Close()

	reqHandler := bstore.RequestHandler{}
	httpHandler := HTTPRPCHandler{ReqHandler: &reqHandler}
	stdinHandler := StreamRPCHandler{ReqHandler: &reqHandler}

	httpMux := http.NewServeMux()
	httpMux.Handle(*endpoint, &httpHandler)

	runners := make([]func(), 0)

	if *stdinFlag {
		log.Println("Listening for requests on stdin...")
		sync.Add(1)
		runners = append(runners, func() {
			err := ServeStream(os.Stdin, os.Stdout, &stdinHandler)
			if err != nil {
				log.Printf("Listening on stdin has halted with error: %s", err.Error())
			}
			sync.Done()
		})
	}

	if len(addresses) != len(networks) {
		log.Fatalf("Number of networks (%d) do not match the number of addresses (%d)\n", len(networks), len(addresses))
	}

	for i := 0; i < len(networks); i++ {
		httpServer := http.Server{
			Addr:    addresses[i],
			Handler: httpMux,
		}

		netListener, err := net.Listen(networks[i], addresses[i])
		if err != nil {
			panic(err)
		}

		sync.Add(1)
		runners = append(runners, func() {
			err := httpServer.Serve(netListener)
			if err != nil {
				log.Printf("Listening on network %s at %s has halted with error: %s", networks[i], addresses[i], err.Error())
			}
			sync.Done()
		})
		log.Printf("Listening to %s network on %s\n", networks[i], addresses[i])
	}

	for _, run := range runners {
		go run()
	}

	sync.Wait()
}
