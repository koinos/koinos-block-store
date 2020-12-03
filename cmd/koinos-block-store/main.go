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
)

func main() {
	directoryFlag := flag.String("d", "./db", "the database directory")
	listenFlag := flag.String("l", "-", "Listen address (addr:port for TCP, unix:/path/to/flag for socket, - for stdin, comma-delimited for multiple)")
	flag.Parse()

	opts := badger.DefaultOptions(*directoryFlag)
	backend := bstore.NewBadgerBackend(opts)
	defer backend.Close()

	listens := strings.Split(*listenFlag, ",")

	hasStdinListener := false

	reqHandler := bstore.RequestHandler{}
	httpHandler := HTTPRPCHandler{ReqHandler: &reqHandler}
	stdinHandler := StreamRPCHandler{ReqHandler: &reqHandler}

	httpMux := http.NewServeMux()
	httpMux.Handle("/rpc", &httpHandler)

	runners := make([]func(), 0)

	for i := 0; i < len(listens); i++ {
		if listens[i] == "-" {
			if hasStdinListener {
				continue
			}
			runners = append(runners, func() { runFileEndpoint(os.Stdin, os.Stdout, &stdinHandler) })
			hasStdinListener = true
			log.Printf("Added stdin logger\n")
			continue
		}

		split := strings.SplitN(listens[i], ":", 2)
		if len(split) != 2 {
			log.Fatal("Could not parse listen specifier")
		}

		var network string
		var addr string

		if (split[0] == "unix") || (split[0] == "tcp4") || (split[0] == "tcp6") {
			// See https://gist.github.com/teknoraver/5ffacb8757330715bcbcc90e6d46ac74 for more on Unix sockets in Golang
			network = split[0]
			addr = split[1]
		} else if split[0] == "" {
			network = "tcp4"
			addr = "127.0.0.1:" + split[1]
		} else {
			log.Fatal("Could not parse listen specifier ", listens[i])
		}

		log.Printf("Parsed network %s address %s\n", network, addr)

		httpServer := http.Server{
			Addr:    listens[i],
			Handler: httpMux,
		}
		netListener, err := net.Listen(network, addr)
		if err != nil {
			panic(err)
		}

		runners = append(runners, func() {
			log.Fatal(httpServer.Serve(netListener))
		})
	}

	log.Printf("Running %d loggers\n", len(runners))

	for i := 1; i < len(runners); i++ {
		go runners[i]()
	}
	runners[0]()
}
