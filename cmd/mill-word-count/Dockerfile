# syntax=docker/dockerfile:1

FROM golang:1.27.1 AS build

WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local

COPY go.mod go.sum ./
COPY internal/workload/ ./internal/workload/
COPY examples/word-count/wordcount.go ./examples/word-count/wordcount.go
COPY cmd/mill-word-count/ ./cmd/mill-word-count/

RUN go build -trimpath -ldflags="-s -w" -o /out/mill-word-count ./cmd/mill-word-count

FROM scratch

COPY --from=build --chown=65532:65532 /out/mill-word-count /mill-word-count

USER 65532:65532
ENTRYPOINT ["/mill-word-count"]
