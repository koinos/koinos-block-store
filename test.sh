#!/bin/bash

set -e
set -x

exec go test -v github.com/koinos/koinos-block-store/internal/bstore
