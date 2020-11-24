#!/bin/bash

set -e
set -x

go get ./internal/bstore

export PYTHONPATH=koinos-types/programs/koinos-types

KOINOS_SCHEMA_DIR=koinos-types/gen/schema
KOINOS_REFLECT_TEMPLATE_DIR=koinos-types/programs/koinos-types/lang
KOINOS_PACK_GEN_DIR=gen
KOINOS_SCHEMA_FILES="${KOINOS_SCHEMA_DIR}/block.schema"

KOINOS_REFLECT_SRC_DIR=koinos-types/types

KOINOS_REFLECT_SOURCES="
${KOINOS_REFLECT_SRC_DIR}/block.bt
${KOINOS_REFLECT_SRC_DIR}/types.hpp
${KOINOS_REFLECT_SRC_DIR}/block.hpp
${KOINOS_REFLECT_SRC_DIR}/submit.hpp
${KOINOS_REFLECT_SRC_DIR}/system_calls.hpp
${KOINOS_REFLECT_SRC_DIR}/thunks.hpp
${KOINOS_REFLECT_SRC_DIR}/system_call_ids.hpp
${KOINOS_REFLECT_SRC_DIR}/thunk_ids.hpp
${KOINOS_REFLECT_SRC_DIR}/chain.hpp
types/block_store.hpp
"

mkdir -p $KOINOS_SCHEMA_DIR

python3 -m koinos_reflect.analyze \
   ${KOINOS_REFLECT_SOURCES} \
   -s \
   -o ${KOINOS_SCHEMA_DIR}/block.schema

python3 -m koinos_codegen.codegen \
   --target-path "${KOINOS_REFLECT_TEMPLATE_DIR}" \
   --target golang \
   -p koinos/pack \
   -o "${KOINOS_PACK_GEN_DIR}" \
   ${KOINOS_SCHEMA_FILES}

mkdir -p internal/types
cp -p gen/koinos/pack/basetypes.go internal/types/basetypes.go
cp -p gen/koinos/pack/koinos.go internal/types/koinos.go

mkdir -p build
go build -o build/koinos-block-store cmd/koinos-block-store/main.go

