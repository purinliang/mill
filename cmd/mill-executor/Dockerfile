# syntax=docker/dockerfile:1

FROM golang:1.27.1 AS build

WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local

COPY go.mod go.sum ./
RUN go mod download

COPY internal/ ./internal/
COPY cmd/mill-executor/ ./cmd/mill-executor/

RUN go build -trimpath -ldflags="-s -w" -o /out/mill-executor ./cmd/mill-executor

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=65532:65532 /out/mill-executor /mill-executor

USER 65532:65532
ENTRYPOINT ["/mill-executor"]
