#!/usr/bin/env bash
# Installs a pinned K3s server or agent for the multi-laptop lab.

set -euo pipefail

usage() {
	printf 'Usage: sudo MILL_NODE_NAME=<unique-name> K3S_TOKEN_FILE=<path> %s server-init|server-join|agent\n' "$0" >&2
}

[[ $# -eq 1 ]] || { usage; exit 1; }
role="$1"
case "${role}" in
server-init|server-join|agent) ;;
*) usage; exit 1 ;;
esac
(( EUID == 0 )) || { printf 'Run this installer through sudo.\n' >&2; exit 1; }
for required in curl mktemp rm sha256sum sh stat; do
	command -v "${required}" >/dev/null || { printf 'Missing command: %s\n' "${required}" >&2; exit 1; }
done
: "${MILL_NODE_NAME:?MILL_NODE_NAME must be unique in the cluster}"
: "${K3S_TOKEN_FILE:?K3S_TOKEN_FILE must name a root-readable token file}"
[[ -f "${K3S_TOKEN_FILE}" ]] || { printf 'Token file does not exist.\n' >&2; exit 1; }
token_mode="$(stat -c '%a' "${K3S_TOKEN_FILE}")"
[[ "${token_mode}" =~ ^[46]00$ ]] || { printf 'Token file mode must be 400 or 600.\n' >&2; exit 1; }
K3S_TOKEN="$(< "${K3S_TOKEN_FILE}")"
[[ -n "${K3S_TOKEN}" && "${K3S_TOKEN}" != *[[:space:]]* ]] || {
	printf 'Token must be non-empty and contain no whitespace.\n' >&2; exit 1;
}
[[ "${MILL_NODE_NAME}" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ ]] || {
	printf 'MILL_NODE_NAME must be a lowercase Kubernetes node name.\n' >&2; exit 1;
}
if [[ "${role}" != server-init ]]; then
	: "${K3S_SERVER_URL:?K3S_SERVER_URL is required when joining a node}"
	[[ "${K3S_SERVER_URL}" =~ ^https://[^[:space:]]+:6443$ ]] || {
		printf 'K3S_SERVER_URL must look like https://host-or-ip:6443.\n' >&2; exit 1;
	}
fi

readonly version='v1.36.3+k3s1'
readonly installer_sha256=46177d4c99440b4c0311b67233823a8e8a2fc09693f6c89af1a7161e152fbfad
temporary_directory="$(mktemp -d)"
cleanup() { rm -rf -- "${temporary_directory}"; }
trap cleanup EXIT

installer="${temporary_directory}/install-k3s.sh"
curl -fsSLo "${installer}" \
	'https://raw.githubusercontent.com/k3s-io/k3s/v1.36.3%2Bk3s1/install.sh'
printf '%s  %s\n' "${installer_sha256}" "${installer}" | sha256sum --check

export INSTALL_K3S_VERSION="${version}"
export K3S_TOKEN
export K3S_NODE_NAME="${MILL_NODE_NAME}"
arguments=()
case "${role}" in
server-init)
	arguments=(server --cluster-init)
	;;
server-join)
	arguments=(server --server "${K3S_SERVER_URL}")
	;;
agent)
	arguments=(agent --server "${K3S_SERVER_URL}")
	;;
esac
if [[ "${role}" != agent && -n "${K3S_TLS_SAN:-}" ]]; then
	[[ "${K3S_TLS_SAN}" != *[[:space:]]* ]] || { printf 'K3S_TLS_SAN cannot contain whitespace.\n' >&2; exit 1; }
	arguments+=(--tls-san "${K3S_TLS_SAN}")
fi

sh "${installer}" "${arguments[@]}"
printf 'K3s %s configured node %s as %s.\n' "${version}" "${MILL_NODE_NAME}" "${role}"
