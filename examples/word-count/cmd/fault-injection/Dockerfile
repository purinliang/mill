# syntax=docker/dockerfile:1
FROM golang:1.27.1 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
COPY go.mod go.sum ./
COPY internal/workload/ ./internal/workload/
COPY examples/word-count/wordcount.go ./examples/word-count/wordcount.go
COPY examples/word-count/cmd/word-count/ ./examples/word-count/cmd/word-count/
COPY examples/word-count/cmd/fault-injection/ ./examples/word-count/cmd/fault-injection/
RUN go build -trimpath -ldflags="-s -w" -o /out/mill-word-count ./examples/word-count/cmd/word-count
RUN go build -trimpath -ldflags="-s -w" -o /out/fault-injection ./examples/word-count/cmd/fault-injection
FROM scratch
COPY --from=build --chown=65532:65532 /out/ /
USER 65532:65532
ENTRYPOINT ["/fault-injection"]
