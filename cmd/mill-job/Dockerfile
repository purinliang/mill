# syntax=docker/dockerfile:1
# Builds the minimal non-root Mill Job-service image.

FROM golang:1.27.1 AS build

WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local

COPY go.mod go.sum ./
RUN go mod download

COPY internal/ ./internal/
COPY cmd/mill-job/ ./cmd/mill-job/

RUN go build -trimpath -ldflags="-s -w" -o /out/mill-job ./cmd/mill-job

FROM scratch

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=65532:65532 /out/mill-job /mill-job

USER 65532:65532
EXPOSE 8080 9090
ENTRYPOINT ["/mill-job"]
