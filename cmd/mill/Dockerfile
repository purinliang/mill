# syntax=docker/dockerfile:1

FROM golang:1.27.1 AS build

WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local

COPY go.mod go.sum ./
RUN go mod download

COPY internal/ ./internal/
COPY cmd/mill/ ./cmd/mill/

RUN go build -trimpath -ldflags="-s -w" -o /out/mill ./cmd/mill

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=65532:65532 /out/mill /mill

USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/mill"]
