#!/usr/bin/env bash

set -euo pipefail

temporary_directory="$(mktemp -d)"
readonly temporary_directory
readonly raw_profile="${temporary_directory}/coverage.out"
readonly filtered_profile="${temporary_directory}/coverage-handwritten.out"

cleanup() {
	rm -rf -- "${temporary_directory}"
}

trap cleanup EXIT

go test ./... -coverprofile="${raw_profile}"

awk 'NR == 1 || $1 !~ /\.pb\.go:/' \
	"${raw_profile}" >"${filtered_profile}"

printf '\nHandwritten Go coverage (generated *.pb.go excluded):\n'
go tool cover -func="${filtered_profile}"
