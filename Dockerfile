FROM golang:1.16.2-alpine as builder

ADD . /koinos-block-store
WORKDIR /koinos-block-store

RUN go get ./... && \
    go build -o koinos_block_store cmd/koinos-block-store/main.go

FROM alpine:latest
COPY --from=builder /koinos-block-store/koinos_block_store /usr/local/bin
CMD  /usr/local/bin/koinos_block_store
