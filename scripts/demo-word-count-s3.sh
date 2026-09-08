#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'Usage: %s\n' "$0" >&2
	exit 1
fi
cd "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
for required in go docker kind kubectl initdb pg_ctl psql curl jq mktemp wc openssl env grep tr; do
	command -v "${required}" >/dev/null || { printf 'Missing command: %s\n' "${required}" >&2; exit 1; }
done
curl --help all | grep -q -- '--aws-sigv4' || { printf 'curl must support --aws-sigv4\n' >&2; exit 1; }
docker info >/dev/null
docker network inspect kind >/dev/null
kubectl --context kind-mill get nodes >/dev/null
[[ "$(initdb --version)" == *' 18.'* ]] || { printf 'PostgreSQL 18 tools are required.\n' >&2; exit 1; }

parallelism="${MILL_PARALLELISM:-3}"
[[ "${parallelism}" =~ ^[1-9][0-9]*$ ]] && (( parallelism <= 10000 )) || { printf 'Invalid MILL_PARALLELISM\n' >&2; exit 1; }
port="${MILL_DEMO_PORT:-18081}"
[[ "${port}" =~ ^[0-9]+$ ]] && (( port > 1024 && port < 65536 )) || { printf 'Invalid MILL_DEMO_PORT\n' >&2; exit 1; }
grpc_port="${MILL_DEMO_GRPC_PORT:-$((port + 1))}"
[[ "${grpc_port}" =~ ^[0-9]+$ ]] && (( grpc_port > 1024 && grpc_port < 65536 && grpc_port != port )) || { printf 'Invalid MILL_DEMO_GRPC_PORT\n' >&2; exit 1; }

run_directory="$(mktemp -d /tmp/mill-s3.XXXXXXXX)"
run_token="$(basename "${run_directory}" | tr '[:upper:]' '[:lower:]' | tr '.' '-')"
storage_container="${run_token}-storage"
credentials_secret="${run_token}-credentials"
server_pid=""
execution_pid=""
postgres_started=false
storage_started=false
secret_created=false
job_id=""
cleanup() {
	local status=$?
	if [[ -n "${execution_pid}" ]]; then
		kill -TERM "${execution_pid}" 2>/dev/null || true
		wait "${execution_pid}" 2>/dev/null || status=1
	fi
	if [[ -n "${server_pid}" ]]; then
		kill -TERM "${server_pid}" 2>/dev/null || true
		wait "${server_pid}" 2>/dev/null || status=1
	fi
	if [[ "${postgres_started}" == true ]]; then
		pg_ctl -D "${run_directory}/postgres" -m fast -w stop >/dev/null || true
	fi
	if [[ "${secret_created}" == true ]]; then
		kubectl --context kind-mill -n default delete secret "${credentials_secret}" --ignore-not-found >/dev/null || true
	fi
	if [[ "${storage_started}" == true ]]; then
		docker rm -f "${storage_container}" >/dev/null || true
	fi
	if (( status != 0 )); then
		printf '\nDemo failed; inspect %s/server.log, %s/execution.log, %s/storage.log, and %s/postgres.log\n' \
			"${run_directory}" "${run_directory}" "${run_directory}" "${run_directory}" >&2
		if [[ -n "${job_id}" ]]; then
			kubectl --context kind-mill -n default get jobs,pods -l "mill.dev/job-id=${job_id}" || true
		fi
	fi
	exit "${status}"
}
trap cleanup EXIT

printf 'Demo files: %s\n' "${run_directory}"
mkdir "${run_directory}/input" "${run_directory}/output" "${run_directory}/socket" "${run_directory}/storage"
go run ./examples/word-count/generate --output "${run_directory}/input/records.jsonl"
go build -o "${run_directory}/mill-job" ./cmd/mill-job
go build -o "${run_directory}/mill-execution" ./cmd/mill-execution
docker build --file examples/word-count/cmd/word-count/Dockerfile --tag mill/word-count:dev .
kind load docker-image mill/word-count:dev --name mill

# SeaweedFS is only the local S3-compatible development service. Mill uses the
# AWS SDK and the same s3:// URIs when pointed at AWS S3.
region=us-east-1
access_key="mill$(openssl rand -hex 8)"
secret_key="$(openssl rand -hex 32)"
docker run --detach --name "${storage_container}" --network kind \
	--publish 127.0.0.1::8333 --volume "${run_directory}/storage:/data" \
	--env "AWS_ACCESS_KEY_ID=${access_key}" --env "AWS_SECRET_ACCESS_KEY=${secret_key}" \
	--env 'S3_BUCKET=mill-input,mill-output' chrislusf/seaweedfs:4.44 \
	mini -dir=/data > "${run_directory}/storage-container-id"
storage_started=true
host_address="$(docker port "${storage_container}" 8333/tcp | head -n 1)"
host_endpoint="http://${host_address}"
pod_address="$(docker inspect --format '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "${storage_container}")"
pod_endpoint="http://${pod_address}:8333"
ready=false
for _ in {1..120}; do
	if curl --silent --show-error "${host_endpoint}/" >/dev/null 2>&1; then ready=true; break; fi
	sleep 0.5
done
[[ "${ready}" == true ]] || { docker logs "${storage_container}" > "${run_directory}/storage.log" 2>&1; printf 'Object storage did not become ready\n' >&2; exit 1; }
docker logs "${storage_container}" > "${run_directory}/storage.log" 2>&1

curl_signature=(--silent --show-error --fail --aws-sigv4 "aws:amz:${region}:s3" --user "${access_key}:${secret_key}")
curl "${curl_signature[@]}" --upload-file "${run_directory}/input/records.jsonl" \
	"${host_endpoint}/mill-input/records.jsonl" >/dev/null
kubectl --context kind-mill -n default create secret generic "${credentials_secret}" \
	--from-literal="AWS_ACCESS_KEY_ID=${access_key}" \
	--from-literal="AWS_SECRET_ACCESS_KEY=${secret_key}" >/dev/null
secret_created=true

# Keep PostgreSQL private to this demo process. Object storage, unlike the
# earlier hostPath demo, is reachable independently by Mill and every Pod.
initdb -D "${run_directory}/postgres" --username=mill_demo --auth-local=trust --auth-host=reject --encoding=UTF8 --no-locale > "${run_directory}/initdb.log"
pg_ctl -D "${run_directory}/postgres" -l "${run_directory}/postgres.log" -o "-k ${run_directory}/socket -h ''" -w start
postgres_started=true
psql "postgresql://mill_demo@/postgres?host=${run_directory}/socket" -v ON_ERROR_STOP=1 -c 'CREATE DATABASE mill' >/dev/null
export MILL_DATABASE_URL="postgresql://mill_demo@/mill?host=${run_directory}/socket"
for migration in migrations/*.sql; do
	psql "${MILL_DATABASE_URL}" -v ON_ERROR_STOP=1 -f "${migration}" >/dev/null
done

export AWS_ACCESS_KEY_ID="${access_key}" AWS_SECRET_ACCESS_KEY="${secret_key}" AWS_REGION="${region}"
export MILL_S3_ENDPOINT="${host_endpoint}" MILL_OUTPUT_ROOT_URI=s3://mill-output
export MILL_HTTP_ADDR="127.0.0.1:${port}" MILL_PARALLELISM="${parallelism}"
export MILL_GRPC_ADDR="127.0.0.1:${grpc_port}"
export MILL_KUBE_CONTEXT=kind-mill MILL_KUBE_NAMESPACE=default
export MILL_WORKLOAD_S3_REGION="${region}" MILL_WORKLOAD_S3_ENDPOINT="${pod_endpoint}"
export MILL_WORKLOAD_S3_CREDENTIALS_SECRET="${credentials_secret}"
"${run_directory}/mill-job" > "${run_directory}/server.log" 2>&1 &
server_pid=$!
ready=false
for _ in {1..60}; do
	kill -0 "${server_pid}" 2>/dev/null || { printf 'Mill exited; inspect server.log\n' >&2; exit 1; }
	if curl -fsS "http://${MILL_HTTP_ADDR}/readyz" >/dev/null 2>&1; then ready=true; break; fi
	sleep 0.5
done
[[ "${ready}" == true ]] || { printf 'Mill did not become ready\n' >&2; exit 1; }

env -u MILL_DATABASE_URL -u MILL_OUTPUT_ROOT_URI -u MILL_PARALLELISM -u MILL_HTTP_ADDR -u MILL_GRPC_ADDR \
	MILL_JOB_GRPC_TARGET="127.0.0.1:${grpc_port}" "${run_directory}/mill-execution" > "${run_directory}/execution.log" 2>&1 &
execution_pid=$!
ready=false
for _ in {1..60}; do
	kill -0 "${execution_pid}" 2>/dev/null || { printf 'Execution exited; inspect execution.log\n' >&2; exit 1; }
	if grep -q 'Mill execution instance=' "${run_directory}/execution.log"; then ready=true; break; fi
	sleep 0.1
done
[[ "${ready}" == true ]] || { printf 'Execution did not initialize\n' >&2; exit 1; }
if tr '\0' '\n' < "/proc/${execution_pid}/environ" | grep -q '^MILL_DATABASE_URL='; then
	printf 'Execution inherited MILL_DATABASE_URL\n' >&2; exit 1
fi

jq -n --arg image mill/word-count:dev --arg uri s3://mill-input/records.jsonl \
	'{executable:{image:$image,args:[]},input:{uri:$uri}}' > "${run_directory}/submission.json"
curl -fsS "http://${MILL_HTTP_ADDR}/jobs" -H 'Content-Type: application/json' -H 'Idempotency-Key: word-count-s3' \
	--data-binary "@${run_directory}/submission.json" > "${run_directory}/job.json"
job_id="$(jq -er '.id' "${run_directory}/job.json")"
expected_tasks="$(jq -er '.progress.total' "${run_directory}/job.json")"
printf '\nMill job: %s (%s tasks, parallelism %s)\n' "${job_id}" "${expected_tasks}" "${parallelism}"
peak=0
finished=false
deadline=$((SECONDS + 180))
while (( SECONDS < deadline )); do
	kill -0 "${execution_pid}" 2>/dev/null || { printf 'Execution stopped during the batch\n' >&2; exit 1; }
	curl -fsS "http://${MILL_HTTP_ADDR}/jobs/${job_id}" > "${run_directory}/status.json"
	state="$(jq -r '.state' "${run_directory}/status.json")"
	running="$(jq -r '.progress.running' "${run_directory}/status.json")"
	(( running <= parallelism )) || { printf 'Concurrency limit exceeded\n' >&2; exit 1; }
	if (( running > peak )); then peak=${running}; fi
	jq -c '{state,progress}' "${run_directory}/status.json"
	if [[ "${state}" == completed ]]; then finished=true; break; fi
	if [[ "${state}" == failed && "${running}" == 0 ]]; then break; fi
	sleep 0.5
done
[[ "${finished}" == true ]] || { printf 'Job did not complete; inspect status.json\n' >&2; exit 1; }
jq -e --argjson tasks "${expected_tasks}" '.progress.completed == $tasks and (.results | length) == $tasks' "${run_directory}/status.json" >/dev/null

kubectl --context kind-mill -n default get jobs -l "mill.dev/job-id=${job_id}" -o json > "${run_directory}/kubernetes-jobs.json"
jq -e '.items | length > 0 and all(.[]; ((.spec.template.spec.nodeSelector // {}) | length) == 0 and ((.spec.template.spec.volumes // []) | length) == 0)' \
	"${run_directory}/kubernetes-jobs.json" >/dev/null

mapfile -t result_uris < <(jq -r '.results[].uri' "${run_directory}/status.json")
partials=()
for index in "${!result_uris[@]}"; do
	object_path="${result_uris[index]#s3://}"
	partial="${run_directory}/output/task-${index}.jsonl"
	curl "${curl_signature[@]}" "${host_endpoint}/${object_path}" > "${partial}"
	partials+=("${partial}")
done
go run ./examples/word-count/cmd/merge "${partials[@]}" > "${run_directory}/counts.jsonl"
input_size="$(wc -c < "${run_directory}/input/records.jsonl")"
go run ./examples/word-count/cmd/word-count --job-id "${job_id}" --task-id local-baseline --shard-index 0 \
	--input-uri "file://${run_directory}/input/records.jsonl" --input-start-byte 0 --input-end-byte "${input_size}" \
	--output-uri "file://${run_directory}/baseline.jsonl" --
cmp "${run_directory}/baseline.jsonl" "${run_directory}/counts.jsonl"

printf '\nPASS: %s Pods used S3-compatible ranged inputs and outputs with no hostPath or node pin.\n' "${expected_tasks}"
printf 'Observed peak running tasks: %s\nFinal counts: %s/counts.jsonl\nJob status: %s/status.json\n' \
	"${peak}" "${run_directory}" "${run_directory}"
printf 'Inspect Jobs/Pods: kubectl --context kind-mill -n default get jobs,pods -l mill.dev/job-id=%s\n' "${job_id}"
printf 'Remove Jobs/Pods when finished: kubectl --context kind-mill -n default delete jobs -l mill.dev/job-id=%s\n' "${job_id}"
