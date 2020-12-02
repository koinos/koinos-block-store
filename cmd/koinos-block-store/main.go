package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"github.com/dgraph-io/badger"
	"github.com/koinos/koinos-block-store/internal/bstore"
	types "github.com/koinos/koinos-block-store/internal/types"
)

// Send block to store
//
// Key-value store backend for block data
//

// TODO create block_receipt

func debugTesting() {
	// Some testing stuff
	h, _ := hex.DecodeString("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	block_id := types.Multihash{Id: 0x12, Digest: types.VariableBlob(h)}
	test_req := types.BlockStoreReq{types.GetBlocksByIdReq{BlockId: types.VectorMultihash{block_id}}}
	test_req_json, _ := test_req.MarshalJSON()
	fmt.Println(string(test_req_json))
}

type ResponseOrError struct {
	Response types.BlockStoreResp
	Error    error
}

type StreamRpcHandler struct {
	ReqHandler *bstore.RequestHandler
}

type JsonRpcRequestGeneric struct {
	JsonRpc string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	Id      interface{}   `json:"id"`
}

type JsonRpcRequest struct {
	JsonRpc string                `json:"jsonrpc"`
	Method  string                `json:"method"`
	Params  []types.BlockStoreReq `json:"params"`
	Id      interface{}           `json:"id"`
}

type JsonRpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data",omitempty`
}

type JsonRpcResponse struct {
	JsonRpc string               `json:"jsonrpc"`
	Result  types.BlockStoreResp `json:"result,omitempty"`
	Error   interface{}          `json:"error,omitempty"`
	Id      interface{}          `json:"id"`
}

func CreateJsonRpcResponse(handler *bstore.RequestHandler, req *JsonRpcRequest) JsonRpcResponse {
	// JSONRPC_PARSE_ERROR := -32700
	JSONRPC_INVALID_REQ := -32600
	JSONRPC_METHOD_NOT_FOUND := -32601
	JSONRPC_INVALID_PARAMS := -32602
	// JSONRPC_INTERNAL_ERROR := -32603

	JSONRPC_APP_ERROR := -32001

	err_resp := func(code int, message string) JsonRpcResponse {
		return JsonRpcResponse{JsonRpc: "2.0", Id: req.Id, Error: JsonRpcError{Code: code, Message: message}}
	}

	if req.JsonRpc != "2.0" {
		return err_resp(JSONRPC_INVALID_REQ, "Only jsonrpc 2.0 is supported")
	}
	maybe_id := req.Id
	if maybe_id != nil {
		_, is_float := req.Id.(float64)
		_, is_str := req.Id.(string)
		if !(is_float || is_str) {
			maybe_id = nil
		}
	}
	if maybe_id == nil {
		return err_resp(JSONRPC_INVALID_REQ, "Id is required by this server")
	}
	if req.Method != "call" {
		return err_resp(JSONRPC_METHOD_NOT_FOUND, "Method not found")
	}
	if len(req.Params) != 1 {
		return err_resp(JSONRPC_INVALID_PARAMS, "Invalid parameters")
	}

	result, err := handler.HandleRequest(&req.Params[0])
	if err != nil {
		return err_resp(JSONRPC_APP_ERROR, err.Error())
	}

	return JsonRpcResponse{JsonRpc: "2.0", Id: req.Id, Result: *result}
}

func HandleJsonRpcRequest(handler *bstore.RequestHandler, req_bytes []byte) ([]byte, bool) {
	JSONRPC_PARSE_ERROR := -32700
	JSONRPC_INTERNAL_ERROR := -32603
	JSONRPC_APP_ERROR := -32001

	// Any error that occurs will be returned in an error response instead of propagating to the caller
	// If ok = false is retured, it means the client cannot recover from this error and the caller should close the connection

	req := JsonRpcRequest{}
	var resp JsonRpcResponse
	var ok bool

	err := json.Unmarshal(req_bytes, &req)
	if err != nil {
		//
		// The client gave us a request we can't parse.
		// We'll retry with interface{} instead of the actual request type.
		// We know we're going to return an error, but we can return it with the correct ID.
		//

		generic_req := JsonRpcRequestGeneric{}
		generic_err := json.Unmarshal(req_bytes, &generic_req)
		if generic_err != nil {
			// We couldn't even parse it to the point where we can get an ID to identify the bad request
			// The remote end may be speaking some non-JSON protocol
			// At this point in the code, we're ready to give up on returning a meaningful result,
			// and simply advise the caller to close the connection
			resp = JsonRpcResponse{JsonRpc: "2.0", Id: req.Id, Error: JsonRpcError{Code: JSONRPC_PARSE_ERROR, Message: generic_err.Error()}}
			ok = false
		} else {
			// Send whatever serialization error message we got back to the client
			resp = JsonRpcResponse{JsonRpc: "2.0", Id: req.Id, Error: JsonRpcError{Code: JSONRPC_APP_ERROR, Message: err.Error()}}
			ok = true
		}
		// We still need to serialize the response, and we re-enter the normal code path to serialize and handle a serialization error
	} else {
		// The normal non-error code path
		resp = CreateJsonRpcResponse(handler, &req)
		ok = true
	}

	resp_json, err := json.Marshal(resp)

	if err != nil {
		//
		// CreateJsonRpcResponse reported success and returned a result
		// However we were unable to serialize the result, meaning CreateJsonRpcResponse()
		// returned some structure that's illegal to serialize.
		//
		// If this point in the code is ever reached, it's likely due to a bug somewhere in the
		// response construction or serialization.
		//
		err_resp := JsonRpcResponse{JsonRpc: "2.0", Id: req.Id, Error: JsonRpcError{Code: JSONRPC_INTERNAL_ERROR, Message: "Could not marshal JSONRPC result"}}
		resp_json, err = json.Marshal(err_resp)

		if err != nil {
			// Surely this manually constructed object will serialize, but just in case it doesn't,
			// as a very last resort we'll return an empty byte sequence and tell the caller to close the connection
			return []byte{}, false
		}
		// We now have a legal resp_json, so just fallthrough to the normal return
	}
	return resp_json, ok
}

func runFileEndpoint(infile *os.File, outfile *os.File, handler *StreamRpcHandler) error {
	scanner := bufio.NewScanner(infile)
	for scanner.Scan() {
		line := []byte(scanner.Text())

		resp_json, ok := HandleJsonRpcRequest(handler.ReqHandler, line)

		if !ok {
			return errors.New("Connection-fatal error handling request")
		}
		resp_json = append(resp_json, byte('\n'))

		// According to docs for Write(), err is non-nil whenever the first result value is unexpected
		_, err := outfile.Write(resp_json)
		if err != nil {
			log.Printf("Error writing to output stream\n")
			return err
		}
	}
	return nil
}

type HttpRpcHandler struct {
	ReqHandler *bstore.RequestHandler
}

func (handler *HttpRpcHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, %q", html.EscapeString(r.URL.Path))
}

func main() {
	dFlag      := flag.String("d", "./db", "the database directory")
	listenFlag := flag.String("l", "-", "Listen address (addr:port for TCP, unix:/path/to/flag for socket, - for stdin, comma-delimited for multiple)")
	flag.Parse()

	var opts = badger.DefaultOptions(*dFlag)
	var backend = bstore.NewBadgerBackend(opts)
	defer backend.Close()

	listens := strings.Split(*listenFlag, ",")

	hasStdinListener := false

	req_handler := bstore.RequestHandler{}
	http_handler := HttpRpcHandler{ReqHandler: &req_handler}
	stdin_handler := StreamRpcHandler{ReqHandler: &req_handler}

	http_mux := http.NewServeMux()
	http_mux.Handle("/rpc", &http_handler)

	runners := make([]func(), 0)

	for i := 0; i < len(listens); i++ {
		if listens[i] == "-" {
			if hasStdinListener {
				continue
			}
			runners = append(runners, func() { runFileEndpoint(os.Stdin, os.Stdout, &stdin_handler) })
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

		http_server := http.Server{
			Addr:    listens[i],
			Handler: http_mux,
		}
		net_listener, err := net.Listen(network, addr)
		if err != nil {
			panic(err)
		}

		runners = append(runners, func() {
			log.Fatal(http_server.Serve(net_listener))
		})
	}

	log.Printf("Running %d loggers\n", len(runners))

	for i := 1; i < len(runners); i++ {
		go runners[i]()
	}
	runners[0]()
}
