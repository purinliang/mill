#!/usr/bin/env bash
# Installs the pinned CloudNativePG operator used by availability demos.

set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'Usage: %s\n' "$0" >&2
	exit 1
fi

readonly context="${MILL_KUBE_CONTEXT:-k3s-default}"
readonly version=1.30.0
readonly manifest_sha256=f8bede43fe4ee0d478c2355b204a36876b2ae4faac60f2a9452280b293da3b88
temporary_directory="$(mktemp -d)"

cleanup() {
	rm -rf -- "${temporary_directory}"
}
trap cleanup EXIT

for required in curl sha256sum kubectl mktemp; do
	command -v "${required}" >/dev/null || { printf 'Missing command: %s\n' "${required}" >&2; exit 1; }
done

manifest="${temporary_directory}/cnpg-${version}.yaml"
curl -fsSLo "${manifest}" \
	"https://github.com/cloudnative-pg/cloudnative-pg/releases/download/v${version}/cnpg-${version}.yaml"
printf '%s  %s\n' "${manifest_sha256}" "${manifest}" | sha256sum --check
kubectl --context "${context}" apply --server-side -f "${manifest}"
kubectl --context "${context}" rollout status deployment/cnpg-controller-manager \
	--namespace cnpg-system --timeout=120s

printf 'CloudNativePG %s is ready in context %s.\n' "${version}" "${context}"
