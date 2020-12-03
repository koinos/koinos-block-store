package main

import (
	"bufio"
	"errors"
	"github.com/koinos/koinos-block-store/internal/bstore"
	"log"
	"os"
)

// StreamRPCHandler handles those RPCs
type StreamRPCHandler struct {
	ReqHandler *bstore.RequestHandler
}

func runFileEndpoint(infile *os.File, outfile *os.File, handler *StreamRPCHandler) error {
	scanner := bufio.NewScanner(infile)
	for scanner.Scan() {
		line := []byte(scanner.Text())

		jsonResponse, ok := HandleJSONRPCRequest(handler.ReqHandler, line)

		if !ok {
			return errors.New("Connection-fatal error handling request")
		}
		jsonResponse = append(jsonResponse, byte('\n'))

		// According to docs for Write(), err is non-nil whenever the first result value is unexpected
		_, err := outfile.Write(jsonResponse)
		if err != nil {
			log.Printf("Error writing to output stream\n")
			return err
		}
	}
	return nil
}
