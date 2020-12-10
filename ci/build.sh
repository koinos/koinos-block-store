#!/bin/bash

set -e
set -x

go get ./...
mkdir -p build
go build -o build/koinos-block-store cmd/koinos-block-store/main.go
