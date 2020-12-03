package main

import (
	"github.com/koinos/koinos-block-store/internal/bstore"
	"io/ioutil"
	"net/http"
)

// HTTPRPCHandler handles HTTP RPC
type HTTPRPCHandler struct {
	ReqHandler *bstore.RequestHandler
}

func (handler *HTTPRPCHandler) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header()["Content-Type"] = []string{"application/json"}

	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if int64(len(body)) != request.ContentLength {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	contentTypes := request.Header.Values("Content-Type")
	if len(contentTypes) != 1 || contentTypes[0] != "application/json" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	response, ok := HandleJSONRPCRequest(handler.ReqHandler, body)
	if ok {
		w.WriteHeader(http.StatusOK)
		w.Write(response)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
