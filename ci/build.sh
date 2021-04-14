#!/bin/bash

set -e
set -x

if [[ -z $BUILD_DOCKER ]]; then
   go get ./...
   mkdir -p build
   go build -o build/koinos_block-store cmd/koinos-block-store/main.go
else
   docker build . -t koinos-block-store
fi
