#!/bin/bash

set -e
set -x

# These commands can be removed once koinos-types-golang is public
echo -e "[url \"ssh://git@github.com/\"]\n   insteadOf = https://github.com/\n" >> ~/.gitconfig
export GOPRIVATE="`go env GOPRIVATE`,github.com/koinos/koinos-types-golang"

go get ./...
mkdir -p build
go build -o build/koinos-block-store cmd/koinos-block-store/main.go
