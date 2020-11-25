#!/bin/bash

set -e
set -x

go test -v github.com/koinos/koinos-block-store/internal/bstore -coverprofile=./build/blockstore.out -coverpkg=./internal/bstore
gcov2lcov -infile=./build/blockstore.out -outfile=./build/blockstore.info
