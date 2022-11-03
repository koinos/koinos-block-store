FROM golang:1.18-alpine as builder

ADD . /koinos-block-store
WORKDIR /koinos-block-store

RUN apk update && \
    apk add \
        gcc \
        musl-dev \
        linux-headers

RUN go get ./... && \
    go build -ldflags="-X main.Commit=$(git rev-parse HEAD)" -o koinos_block_store cmd/koinos-block-store/main.go

FROM alpine:latest
COPY --from=builder /koinos-block-store/koinos_block_store /usr/local/bin
ENTRYPOINT [ "/usr/local/bin/koinos_block_store" ]
