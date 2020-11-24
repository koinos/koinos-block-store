#!/bin/bash

set -e
set -x

go get ./internal/bstore
go build cmd
