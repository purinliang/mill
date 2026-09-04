# syntax=docker/dockerfile:1

FROM golang:1.27.1 AS build

WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local

COPY go.mod go.sum ./
COPY internal/workload/ ./internal/workload/
COPY cmd/mill-jsonl-copy/ ./cmd/mill-jsonl-copy/

RUN go build -trimpath -ldflags="-s -w" -o /out/mill-jsonl-copy ./cmd/mill-jsonl-copy

FROM scratch

COPY --from=build --chown=65532:65532 /out/mill-jsonl-copy /mill-jsonl-copy

USER 65532:65532
ENTRYPOINT ["/mill-jsonl-copy"]
