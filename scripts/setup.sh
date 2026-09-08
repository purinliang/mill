#!/usr/bin/env bash

set -euo pipefail

: "${HOME:?HOME must identify the current user home directory}"

readonly KIND_VERSION="v0.33.0"
readonly KUBECTL_VERSION="v1.37.0"
readonly KIND_NODE_IMAGE="kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"
readonly CLUSTER_NAME="mill"
readonly TOOL_INSTALL_DIR="${MILL_TOOLS_DIR:-${HOME}/.local/bin}"

temporary_directory=""
kind_binary=""
kubectl_binary=""

cleanup() {
	if [[ -n "${temporary_directory}" && -d "${temporary_directory}" ]]; then
		rm -rf -- "${temporary_directory}"
	fi
}

fail() {
	printf 'setup: %s\n' "$*" >&2
	exit 1
}

require_command() {
	local command_name="$1"
	local purpose="$2"

	command -v "${command_name}" >/dev/null 2>&1 ||
		fail "${command_name} is required ${purpose}"
}

verify_host() {
	[[ "$(uname -s)" == "Linux" ]] ||
		fail "automatic tool installation currently supports Linux only"
	[[ "$(uname -m)" == "x86_64" ]] ||
		fail "automatic tool installation currently supports amd64 only"
}

verify_docker() {
	command -v docker >/dev/null 2>&1 ||
		fail "Docker is not installed; install Docker Engine before running this script"
	if ! docker info >/dev/null 2>&1; then
		fail "cannot reach the Docker daemon; run 'docker version' and fix daemon or group access first"
	fi
}

ensure_temporary_directory() {
	if [[ -z "${temporary_directory}" ]]; then
		temporary_directory="$(mktemp -d)"
	fi
}

download_verified() {
	local binary_url="$1"
	local checksum_url="$2"
	local destination="$3"
	local checksum

	require_command curl "to download local Kubernetes tools"
	require_command awk "to read downloaded checksums"
	require_command sha256sum "to verify downloaded local Kubernetes tools"
	ensure_temporary_directory

	curl -fsSLo "${destination}" "${binary_url}"
	checksum="$(curl -fsSL "${checksum_url}" | awk 'NR == 1 { print $1 }')"
	[[ "${checksum}" =~ ^[0-9a-f]{64}$ ]] ||
		fail "received an invalid checksum from ${checksum_url}"
	printf '%s  %s\n' "${checksum}" "${destination}" | sha256sum --check --status ||
		fail "checksum verification failed for ${binary_url}"
}

kind_has_pinned_version() {
	local candidate="$1"
	[[ "$("${candidate}" version 2>/dev/null)" == "kind ${KIND_VERSION} "* ]]
}

kubectl_has_pinned_version() {
	local candidate="$1"
	[[ "$("${candidate}" version --client 2>/dev/null)" == *"Client Version: ${KUBECTL_VERSION}"* ]]
}

install_kind() {
	local downloaded_binary

	require_command install "to install local Kubernetes tools"
	mkdir -p "${TOOL_INSTALL_DIR}"
	ensure_temporary_directory
	downloaded_binary="${temporary_directory}/kind"
	download_verified \
		"https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64" \
		"https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64.sha256sum" \
		"${downloaded_binary}"
	install -m 0755 "${downloaded_binary}" "${TOOL_INSTALL_DIR}/kind"
	kind_binary="${TOOL_INSTALL_DIR}/kind"
	printf 'Installed kind %s at %s\n' "${KIND_VERSION}" "${kind_binary}"
}

install_kubectl() {
	local downloaded_binary

	require_command install "to install local Kubernetes tools"
	mkdir -p "${TOOL_INSTALL_DIR}"
	ensure_temporary_directory
	downloaded_binary="${temporary_directory}/kubectl"
	download_verified \
		"https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
		"https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl.sha256" \
		"${downloaded_binary}"
	install -m 0755 "${downloaded_binary}" "${TOOL_INSTALL_DIR}/kubectl"
	kubectl_binary="${TOOL_INSTALL_DIR}/kubectl"
	printf 'Installed kubectl %s at %s\n' "${KUBECTL_VERSION}" "${kubectl_binary}"
}

ensure_kind() {
	local candidate

	if candidate="$(command -v kind 2>/dev/null)" && kind_has_pinned_version "${candidate}"; then
		kind_binary="${candidate}"
		return
	fi
	candidate="${TOOL_INSTALL_DIR}/kind"
	if [[ -x "${candidate}" ]] && kind_has_pinned_version "${candidate}"; then
		kind_binary="${candidate}"
		return
	fi
	install_kind
}

ensure_kubectl() {
	local candidate

	if candidate="$(command -v kubectl 2>/dev/null)" && kubectl_has_pinned_version "${candidate}"; then
		kubectl_binary="${candidate}"
		return
	fi
	candidate="${TOOL_INSTALL_DIR}/kubectl"
	if [[ -x "${candidate}" ]] && kubectl_has_pinned_version "${candidate}"; then
		kubectl_binary="${candidate}"
		return
	fi
	install_kubectl
}

ensure_cluster() {
	local node_image

	if "${kind_binary}" get clusters 2>/dev/null | grep -Fxq "${CLUSTER_NAME}"; then
		printf 'Reusing existing kind cluster %s\n' "${CLUSTER_NAME}"
		if ! node_image="$(docker inspect --format '{{.Config.Image}}' "${CLUSTER_NAME}-control-plane" 2>/dev/null)"; then
			fail "cluster ${CLUSTER_NAME} exists but its control-plane container is unavailable"
		fi
		[[ "${node_image}" == "${KIND_NODE_IMAGE}" ]] ||
			fail "cluster ${CLUSTER_NAME} uses ${node_image}; expected ${KIND_NODE_IMAGE}. Delete it explicitly before recreating it"
	else
		"${kind_binary}" create cluster \
			--name "${CLUSTER_NAME}" \
			--image "${KIND_NODE_IMAGE}" \
			--wait 120s
	fi

	"${kubectl_binary}" config use-context "kind-${CLUSTER_NAME}" >/dev/null
	"${kubectl_binary}" wait \
		--context "kind-${CLUSTER_NAME}" \
		--for=condition=Ready \
		nodes \
		--all \
		--timeout=120s >/dev/null
}

print_status() {
	printf '\nLocal Mill Kubernetes environment is ready.\n'
	"${kind_binary}" version
	"${kubectl_binary}" version --client
	"${kubectl_binary}" get nodes --context "kind-${CLUSTER_NAME}"

	case ":${PATH}:" in
		*":${TOOL_INSTALL_DIR}:"*) ;;
		*)
			printf '\nAdd %s to PATH to invoke locally installed tools directly.\n' "${TOOL_INSTALL_DIR}"
			printf 'For the current shell: export PATH="%s:$PATH"\n' "${TOOL_INSTALL_DIR}"
			;;
	esac
}

main() {
	[[ "$#" -eq 0 ]] || fail "does not accept arguments"
	verify_host
	verify_docker
	ensure_kind
	ensure_kubectl
	ensure_cluster
	print_status
}

trap cleanup EXIT
main "$@"
