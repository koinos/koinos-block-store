#!/bin/bash

set -e
set -x

if [[ -z $BUILD_DOCKER ]]; then
   go test -v github.com/koinos/koinos-block-store/internal/bstore -coverprofile=./build/blockstore.out -coverpkg=./internal/bstore
   gcov2lcov -infile=./build/blockstore.out -outfile=./build/blockstore.info

   golint -set_exit_status ./...
else
   TAG="$TRAVIS_BRANCH"
   if [ "$TAG" = "master" ]; then
      TAG="latest"
   fi

   export BLOCK_STORE_TAG=$TAG

   git clone https://github.com/koinos/koinos-integration-tests.git

   cd koinos-integration-tests
   go get ./...
   cd tests
   ./run.sh
fi
