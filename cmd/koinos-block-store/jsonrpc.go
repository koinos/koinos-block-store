package main

import (
	"encoding/json"
	"errors"
	"github.com/koinos/koinos-block-store/internal/bstore"
	"github.com/koinos/koinos-block-store/internal/types"
	"log"
	"strings"
)

// The JSONRPCGenericRequest allows for parsing incoming JSON RPC
// while deferring the parsing of the params
type JSONRPCGenericRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	ID      json.RawMessage `json:"id"`
	Params  json.RawMessage `json:"params"`
}

// JSONRPCError represents a JSON RPC error
type JSONRPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// JSONRPCResponse represents a JSON RPC response
type JSONRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   interface{} `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

const (
	// JSONRPCAppError indicates an application error
	JSONRPCAppError = -32001

	// JSONRPCParseError indicates an unparseable request
	JSONRPCParseError = -32700

	// JSONRPCInvalidReq indicates an invalid request
	JSONRPCInvalidReq = -32600

	// JSONRPCMethodNotFound indicates the requested method is unknown
	JSONRPCMethodNotFound = -32601

	// JSONRPCInvalidParams indicates the provided params are not valid
	JSONRPCInvalidParams = -32602

	// JSONRPCInternalError indicates an internal server error
	JSONRPCInternalError = -32603
)

var (
	// ErrMalformedMethod indicates the method was not properly formed
	ErrMalformedMethod = errors.New("Methods should be in the format service_name.method_name")

	// ErrInvalidService indicates the correct ServiceName was not supplied
	ErrInvalidService = errors.New("Invalid service name provided")

	// ErrInvalidJSONRPCVersion indicates an improper JSON RPC version was specified
	ErrInvalidJSONRPCVersion = errors.New("Invalid or missing JSON RPC version was specified")

	// ErrInvalidJSONRPCID indicates an invalid JSON RPC ID was provided
	ErrInvalidJSONRPCID = errors.New("Invalid ID was specified")

	// ErrMissingJSONRPCID indicates the ID does not exist
	ErrMissingJSONRPCID = errors.New("Missing ID")

	// ErrFractionalJSONRPCID indicates a fractional number was identified as the ID
	ErrFractionalJSONRPCID = errors.New("ID must not contain fractional parts")

	// ErrUnsupportedJSONRPCIDType indicates an ID type that is unsupported
	ErrUnsupportedJSONRPCIDType = errors.New("An ID must be a Number (non-fractional), String, or Null")
)

const (
	// ServiceName is the name of this microservice, prefixed in the method name
	ServiceName = "block_store"

	// MethodSeparator is used to in the method name to split the microservice name and desired method to run
	MethodSeparator = "."

	// MethodSections defines the number of sections in the JSON RPC method
	MethodSections = 2
)

func errorWithID(e error) bool {
	switch e {
	case ErrInvalidJSONRPCID:
	case ErrMissingJSONRPCID:
	case ErrFractionalJSONRPCID:
	case ErrUnsupportedJSONRPCIDType:
	default:
		return false
	}
	return true
}
func translateRequest(j *JSONRPCGenericRequest) (*koinos.BlockStoreReq, error) {
	methodData := strings.SplitN(j.Method, MethodSeparator, MethodSections)
	if len(methodData) != MethodSections {
		return nil, ErrMalformedMethod
	}
	if methodData[0] != ServiceName {
		return nil, ErrInvalidService
	}
	variantBytes := []byte(`{"type":"koinos::types::` + methodData[0] + `::` + methodData[1] + `","value":` + string(j.Params) + `}`)

	var req koinos.BlockStoreReq
	err := json.Unmarshal(variantBytes, &req)
	return &req, err
}

func parseRequest(request []byte) (*JSONRPCGenericRequest, error) {
	var genericRequest JSONRPCGenericRequest
	err := json.Unmarshal(request, &genericRequest)
	if err != nil {
		return nil, err
	}
	return &genericRequest, nil
}

func validateRequest(request *JSONRPCGenericRequest) error {
	// Check ID first, an invalid ID must return a Null ID in the response!

	// The client MUST provide an ID with a request
	if len(request.ID) <= 0 {
		return ErrMissingJSONRPCID
	}

	// Valid IDs are Number, String, or Null
	var id interface{}
	err := json.Unmarshal(request.ID, &id)
	if err != nil {
		return ErrInvalidJSONRPCID
	}

	switch t := id.(type) {
	case string:
	case float64:
		// Numbers SHOULD NOT contain fractional parts
		if t != float64(int64(t)) {
			return ErrFractionalJSONRPCID
		}
	case nil:
	default:
		return ErrUnsupportedJSONRPCIDType
	}

	// We require that JSON RPC is 2.0
	if request.JSONRPC != "2.0" {
		return ErrInvalidJSONRPCVersion
	}

	return nil
}

// HandleJSONRPCRequest handles JSON RPC requests
// Any error that occurs will be returned in an error response instead of propagating to the caller
// If ok = false is retured, it means the client cannot recover from this error and the caller should close the connection
func HandleJSONRPCRequest(handler *bstore.RequestHandler, reqBytes []byte) ([]byte, bool) {
	genericRequest, err := parseRequest(reqBytes)
	if err != nil {
		jsonError, e := json.Marshal(JSONRPCResponse{
			JSONRPC: "2.0",
			// If there was an error in detecting the id in the Request object (e.g. Parse error/Invalid Request), it MUST be Null.
			ID: nil,
			Error: &JSONRPCError{
				Code:    JSONRPCParseError,
				Message: "Unable to parse request",
				Data:    err.Error(),
			},
		})
		if e != nil {
			log.Println("An unexpected error has occurred: ", e.Error())
			return make([]byte, 0), false
		}
		return jsonError, true
	}

	err = validateRequest(genericRequest)
	if err != nil {
		// If there was an error in detecting the id in the Request object (e.g. Parse error/Invalid Request), it MUST be Null.
		id := genericRequest.ID
		if errorWithID(err) {
			id = nil
		}
		jsonError, e := json.Marshal(JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      id,
			Error: &JSONRPCError{
				Code:    JSONRPCInvalidReq,
				Message: "Invalid request",
				Data:    err.Error(),
			},
		})
		if e != nil {
			log.Println("An unexpected error has occurred: ", e.Error())
			return make([]byte, 0), false
		}
		return jsonError, true
	}

	request, err := translateRequest(genericRequest)
	if err != nil {
		jsonError, e := json.Marshal(JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      genericRequest.ID,
			Error: &JSONRPCError{
				Code:    JSONRPCMethodNotFound,
				Message: "Unable to translate request",
				Data:    err.Error(),
			},
		})
		if e != nil {
			log.Println("An unexpected error has occurred: ", e.Error())
			return make([]byte, 0), false
		}
		return jsonError, true
	}

	response, err := handler.HandleRequest(request)
	if err != nil {
		jsonError, e := json.Marshal(JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      genericRequest.ID,
			Error: &JSONRPCError{
				Code:    JSONRPCInternalError,
				Message: "An internal server error has occurred",
				Data:    err.Error(),
			},
		})
		if e != nil {
			log.Println("An unexpected error has occurred: ", e.Error())
			return make([]byte, 0), false
		}
		return jsonError, true
	}

	rpcResponse := JSONRPCResponse{JSONRPC: "2.0", ID: genericRequest.ID, Result: response.Value}

	jsonResponse, err := json.Marshal(rpcResponse)
	if err != nil {
		log.Println("An unexpected error has occurred: ", err.Error())
		return make([]byte, 0), false
	}

	return jsonResponse, true
}
