#!/bin/bash

set -e
set -x

if [[ -z $BUILD_DOCKER ]]; then
   go test -v github.com/koinos/koinos-block-store/internal/bstore -coverprofile=./build/blockstore.out -coverpkg=./internal/bstore
   gcov2lcov -infile=./build/blockstore.out -outfile=./build/blockstore.info

   golint -set_exit_status ./...
fi
