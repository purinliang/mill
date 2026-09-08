#!/usr/bin/env bash

set -euo pipefail

readonly JOB_IMAGE="${MILL_JOB_IMAGE:-mill/job:dev}"
readonly EXECUTION_IMAGE="${MILL_EXECUTION_IMAGE:-mill/execution:dev}"

fail() {
	printf 'build-control-plane-images: %s\n' "$*" >&2
	exit 1
}

require_docker() {
	command -v docker >/dev/null 2>&1 || fail "Docker is required"
	docker info >/dev/null 2>&1 ||
		fail "cannot reach the Docker daemon; run 'docker version' and fix daemon access first"
}

verify_image() {
	local image="$1"
	local expected_entrypoint="$2"
	local actual_user
	local actual_entrypoint

	actual_user="$(docker image inspect --format '{{.Config.User}}' "${image}")"
	actual_entrypoint="$(docker image inspect --format '{{json .Config.Entrypoint}}' "${image}")"
	[[ "${actual_user}" == "65532:65532" ]] ||
		fail "${image} runs as ${actual_user:-the default user}; expected 65532:65532"
	[[ "${actual_entrypoint}" == "[\"${expected_entrypoint}\"]" ]] ||
		fail "${image} entrypoint is ${actual_entrypoint}; expected ${expected_entrypoint}"
}

main() {
	[[ "$#" -eq 0 ]] || fail "does not accept arguments; override tags with MILL_JOB_IMAGE or MILL_EXECUTION_IMAGE"
	require_docker

	docker build --file cmd/mill-job/Dockerfile --tag "${JOB_IMAGE}" .
	docker build --file cmd/mill-execution/Dockerfile --tag "${EXECUTION_IMAGE}" .

	verify_image "${JOB_IMAGE}" /mill-job
	verify_image "${EXECUTION_IMAGE}" /mill-execution

	printf '\nBuilt control-plane images:\n'
	docker image inspect \
		--format '{{.RepoTags}}  size={{.Size}} bytes  user={{.Config.User}}  entrypoint={{json .Config.Entrypoint}}' \
		"${JOB_IMAGE}" "${EXECUTION_IMAGE}"
}

main "$@"
