# syntax=docker/dockerfile:1

FROM golang:1.27.1 AS build

WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local

COPY go.mod go.sum ./
RUN go mod download

COPY internal/ ./internal/
COPY cmd/mill-execution/ ./cmd/mill-execution/

RUN go build -trimpath -ldflags="-s -w" -o /out/mill-execution ./cmd/mill-execution

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=65532:65532 /out/mill-execution /mill-execution

USER 65532:65532
ENTRYPOINT ["/mill-execution"]
