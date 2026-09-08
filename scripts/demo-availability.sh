#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 0 ]]; then
	printf 'Usage: %s\n' "$0" >&2
	exit 1
fi
cd "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

: "${MILL_WORKLOAD_IMAGE:?MILL_WORKLOAD_IMAGE must name the availability-delay workload image}"
: "${MILL_S3_ENDPOINT:?MILL_S3_ENDPOINT must be reachable from this host and every workload Pod}"
: "${MILL_DEMO_INPUT_ROOT_URI:?MILL_DEMO_INPUT_ROOT_URI must be an existing s3:// bucket or prefix}"
: "${AWS_REGION:?AWS_REGION is required}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required for this local S3-compatible demonstration}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required for this local S3-compatible demonstration}"
[[ "${MILL_DEMO_INPUT_ROOT_URI}" =~ ^s3://[^/]+(/.*)?$ ]] || {
	printf 'MILL_DEMO_INPUT_ROOT_URI must be an absolute s3:// bucket or prefix.\n' >&2
	exit 1
}
for required in cmp curl go grep jq kubectl mktemp tr wc; do
	command -v "${required}" >/dev/null || { printf 'Missing command: %s\n' "${required}" >&2; exit 1; }
done
curl --help all | grep -q -- '--aws-sigv4' || {
	printf 'curl must support --aws-sigv4.\n' >&2
	exit 1
}

readonly context="${MILL_KUBE_CONTEXT:-k3s-default}"
readonly system_namespace=mill-system
readonly workload_namespace=mill-workloads
readonly database_namespace=mill-database
readonly parallelism=3
readonly api_port="${MILL_DEMO_PORT:-18084}"
[[ "${api_port}" =~ ^[0-9]+$ ]] && (( api_port > 1024 && api_port < 65536 )) || {
	printf 'MILL_DEMO_PORT must be between 1025 and 65535.\n' >&2
	exit 1
}

run_directory="$(mktemp -d /tmp/mill-availability.XXXXXXXX)"
run_token="$(basename "${run_directory}" | tr '[:upper:]' '[:lower:]' | tr '.' '-')"
api_forward_pid=""
api_forward_pod=""
job_id=""

stop_api_forward() {
	if [[ -n "${api_forward_pid}" ]]; then
		kill -TERM "${api_forward_pid}" 2>/dev/null || true
		wait "${api_forward_pid}" 2>/dev/null || true
		api_forward_pid=""
	fi
}

capture_evidence() {
	kubectl --context "${context}" get nodes -o wide > "${run_directory}/nodes.txt" 2>&1 || true
	kubectl --context "${context}" -n "${system_namespace}" get all -o wide > "${run_directory}/mill-resources.txt" 2>&1 || true
	kubectl --context "${context}" -n "${database_namespace}" get cluster mill-postgres -o yaml > "${run_directory}/postgres-cluster.yaml" 2>&1 || true
	kubectl --context "${context}" -n "${database_namespace}" get pods -o wide > "${run_directory}/postgres-pods.txt" 2>&1 || true
	kubectl --context "${context}" -n "${workload_namespace}" get jobs,pods -o wide > "${run_directory}/workloads.txt" 2>&1 || true
	if [[ -n "${job_id}" ]]; then
		kubectl --context "${context}" -n "${workload_namespace}" get jobs \
			-l "mill.dev/job-id=${job_id}" -o json > "${run_directory}/final-kubernetes-jobs.json" 2>&1 || true
	fi
	for pod in $(kubectl --context "${context}" -n "${system_namespace}" get pods -o name 2>/dev/null); do
		kubectl --context "${context}" -n "${system_namespace}" logs "${pod}" --all-containers \
			> "${run_directory}/${pod#pod/}.log" 2>&1 || true
	done
}

cleanup() {
	local status=$?
	stop_api_forward
	capture_evidence
	if (( status != 0 )); then
		printf '\nAvailability demonstration failed; evidence retained in %s\n' "${run_directory}" >&2
	fi
	exit "${status}"
}
trap cleanup EXIT

ready_pod() {
	local namespace="$1" selector="$2"
	kubectl --context "${context}" -n "${namespace}" get pods -l "${selector}" -o json | jq -er '
		[.items[]
			| select(.status.phase == "Running")
			| select(any(.status.conditions[]; .type == "Ready" and .status == "True"))
			| .metadata.name][0]'
}

assert_two_node_spread() {
	local namespace="$1" selector="$2" description="$3" destination="$4"
	kubectl --context "${context}" -n "${namespace}" get pods -l "${selector}" -o json > "${destination}"
	jq -e --arg description "${description}" '
		[.items[]
			| select(.status.phase == "Running")
			| select(any(.status.conditions[]; .type == "Ready" and .status == "True"))] as $ready
		| ($ready | length) == 2
			and ([$ready[].spec.nodeName] | unique | length) == 2
	' "${destination}" >/dev/null || {
		printf '%s does not have two ready Pods on distinct nodes.\n' "${description}" >&2
		exit 1
	}
}

database_primary() {
	kubectl --context "${context}" -n "${database_namespace}" get cluster mill-postgres \
		-o jsonpath='{.status.currentPrimary}'
}

database_query() {
	local sql="$1" primary
	primary="$(database_primary)"
	[[ -n "${primary}" ]] || return 1
	kubectl --context "${context}" -n "${database_namespace}" exec "${primary}" -c postgres -- \
		psql -XAt -d mill -v ON_ERROR_STOP=1 -c "${sql}"
}

wait_for_database_query() {
	local sql="$1" expected="$2" output
	for _ in {1..120}; do
		if output="$(database_query "${sql}" 2>/dev/null)" && [[ "${output}" == "${expected}" ]]; then
			printf '%s' "${output}"
			return 0
		fi
		sleep 0.5
	done
	return 1
}

start_api_forward() {
	stop_api_forward
	api_forward_pod="$(ready_pod "${system_namespace}" app.kubernetes.io/name=mill-job)"
	kubectl --context "${context}" -n "${system_namespace}" port-forward \
		"pod/${api_forward_pod}" "${api_port}:8080" >> "${run_directory}/api-port-forward.log" 2>&1 &
	api_forward_pid=$!
}

wait_for_api() {
	for _ in {1..120}; do
		if curl -fsS "http://127.0.0.1:${api_port}/readyz" >/dev/null 2>&1; then
			return 0
		fi
		if ! kill -0 "${api_forward_pid}" 2>/dev/null; then
			start_api_forward
		fi
		sleep 0.5
	done
	printf 'Job service did not become reachable.\n' >&2
	return 1
}

get_job_status() {
	local destination="$1"
	for _ in {1..60}; do
		if curl -fsS "http://127.0.0.1:${api_port}/jobs/${job_id}" > "${destination}" 2>/dev/null; then
			return 0
		fi
		if ! kill -0 "${api_forward_pid}" 2>/dev/null; then
			start_api_forward
			wait_for_api
		fi
		sleep 0.5
	done
	printf 'Job status did not recover.\n' >&2
	return 1
}

snapshot_attempts() {
	local destination="$1"
	database_query "
		SELECT COALESCE(json_agg(snapshot ORDER BY shard_index), '[]'::json)
		FROM (
			SELECT t.shard_index, t.id::text AS task_id, a.id::text AS attempt_id,
				a.attempt_number, a.external_id, a.state, a.lease_owner,
				a.lease_token::text
			FROM tasks t JOIN attempts a ON a.task_id = t.id
			WHERE t.job_id = '${job_id}'::uuid AND a.attempt_number = 1
		) snapshot;" > "${destination}"
}

snapshot_jobs_for_attempts() {
	local attempts="$1" destination="$2"
	kubectl --context "${context}" -n "${workload_namespace}" get jobs \
		-l "mill.dev/job-id=${job_id}" -o json | jq --slurpfile attempts "${attempts}" '
		($attempts[0] | map("mill-" + .attempt_id)) as $names
		| [.items[]
			| select(.metadata.name as $name | $names | index($name))
			| {name:.metadata.name, uid:.metadata.uid}]
		| sort_by(.name)' > "${destination}"
}

assert_attempt_identities() {
	local before="$1" after="$2"
	jq -e --slurpfile before "${before}" '
		($before[0] | map({shard_index,task_id,attempt_id,attempt_number,external_id})) as $expected
		| map(select(.attempt_id as $id | ($expected | map(.attempt_id) | index($id))))
		| map({shard_index,task_id,attempt_id,attempt_number,external_id}) == $expected
	' "${after}" >/dev/null
}

printf 'Evidence directory: %s\n' "${run_directory}"
kubectl --context "${context}" get nodes -o json > "${run_directory}/nodes.json"
ready_hostnames="$(jq '[.items[]
	| select(any(.status.conditions[]; .type == "Ready" and .status == "True"))
	| .metadata.labels["kubernetes.io/hostname"]] | unique | length' "${run_directory}/nodes.json")"
(( ready_hostnames >= 2 )) || { printf 'At least two ready hostname failure domains are required.\n' >&2; exit 1; }

kubectl --context "${context}" -n "${system_namespace}" rollout status deployment/mill-job --timeout=120s
kubectl --context "${context}" -n "${system_namespace}" rollout status deployment/mill-execution --timeout=120s
assert_two_node_spread "${system_namespace}" app.kubernetes.io/name=mill-job \
	'Job service' "${run_directory}/job-placement.json"
assert_two_node_spread "${system_namespace}" app.kubernetes.io/name=mill-execution \
	'Execution' "${run_directory}/execution-placement.json"

kubectl --context "${context}" -n "${database_namespace}" wait --for=condition=Ready \
	cluster/mill-postgres --timeout=120s
assert_two_node_spread "${database_namespace}" cnpg.io/cluster=mill-postgres \
	'PostgreSQL' "${run_directory}/postgres-placement.json"
initial_primary="$(database_primary)"
[[ -n "${initial_primary}" ]] || { printf 'CloudNativePG reports no primary.\n' >&2; exit 1; }
wait_for_database_query \
	"SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming' AND sync_state IN ('sync', 'quorum');" \
	1 > "${run_directory}/initial-synchronous-standbys.txt" || {
	printf 'PostgreSQL has no synchronous streaming standby.\n' >&2
	exit 1
}

mkdir "${run_directory}/input" "${run_directory}/output"
go run ./examples/word-count/generate --output "${run_directory}/input/records.jsonl"
input_root="${MILL_DEMO_INPUT_ROOT_URI%/}"
input_path="${input_root#s3://}"
input_uri="s3://${input_path}/${run_token}/records.jsonl"
input_object_url="${MILL_S3_ENDPOINT%/}/${input_path}/${run_token}/records.jsonl"
curl_signature=(--silent --show-error --fail --aws-sigv4 "aws:amz:${AWS_REGION}:s3" \
	--user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}")
curl "${curl_signature[@]}" --upload-file "${run_directory}/input/records.jsonl" \
	"${input_object_url}" >/dev/null

start_api_forward
wait_for_api
jq -n --arg image "${MILL_WORKLOAD_IMAGE}" --arg uri "${input_uri}" \
	'{executable:{image:$image,args:["availability"]},input:{uri:$uri}}' \
	> "${run_directory}/submission.json"
curl -fsS "http://127.0.0.1:${api_port}/jobs" -H 'Content-Type: application/json' \
	-H "Idempotency-Key: ${run_token}" --data-binary "@${run_directory}/submission.json" \
	> "${run_directory}/job.json"
job_id="$(jq -er '.id' "${run_directory}/job.json")"
[[ "${job_id}" =~ ^[0-9a-f-]{36}$ ]] || { printf 'Job service returned an invalid job ID.\n' >&2; exit 1; }
[[ "$(jq -er '.progress.total' "${run_directory}/job.json")" == 12 ]] || {
	printf 'Availability fixture did not produce 12 tasks.\n' >&2
	exit 1
}
printf '\nMill job: %s\nWaiting for the first three workload Pods...\n' "${job_id}"

for _ in {1..120}; do
	get_job_status "${run_directory}/status.json"
	running="$(jq -r '.progress.running' "${run_directory}/status.json")"
	jq -c '{state,progress}' "${run_directory}/status.json"
	[[ "${running}" == "${parallelism}" ]] && break
	sleep 0.5
done
[[ "${running:-0}" == "${parallelism}" ]] || { printf 'Initial three-task wave did not start.\n' >&2; exit 1; }

snapshot_attempts "${run_directory}/execution-attempts-before.json"
jq -e --argjson expected "${parallelism}" '
	[.[] | select(.state == "running")] | length == $expected
	and all(.[] | select(.state == "running"); .external_id != null and .lease_owner != null and .lease_token != null)
' "${run_directory}/execution-attempts-before.json" >/dev/null
snapshot_jobs_for_attempts "${run_directory}/execution-attempts-before.json" \
	"${run_directory}/execution-jobs-before.json"
[[ "$(jq 'length' "${run_directory}/execution-jobs-before.json")" == "${parallelism}" ]] || {
	printf 'Expected three initial Kubernetes Jobs.\n' >&2
	exit 1
}

failed_owner="$(jq -er '[.[] | select(.state == "running")] | sort_by(.lease_owner) | group_by(.lease_owner) | max_by(length)[0].lease_owner' \
	"${run_directory}/execution-attempts-before.json")"
jq --arg owner "${failed_owner}" '[.[] | select(.state == "running" and .lease_owner == $owner)]' \
	"${run_directory}/execution-attempts-before.json" > "${run_directory}/failed-owner-attempts.json"
failed_execution_pod=""
for pod in $(kubectl --context "${context}" -n "${system_namespace}" get pods \
	-l app.kubernetes.io/name=mill-execution -o name); do
	if kubectl --context "${context}" -n "${system_namespace}" logs "${pod}" | grep -Fq "instance=${failed_owner} "; then
		failed_execution_pod="${pod#pod/}"
		break
	fi
done
[[ -n "${failed_execution_pod}" ]] || { printf 'Could not map the active lease owner to an execution Pod.\n' >&2; exit 1; }
printf '\nDeleting active execution Pod %s...\n' "${failed_execution_pod}"
kubectl --context "${context}" -n "${system_namespace}" logs "${failed_execution_pod}" \
	> "${run_directory}/deleted-execution.log"
kubectl --context "${context}" -n "${system_namespace}" delete pod "${failed_execution_pod}" --wait=false >/dev/null
kubectl --context "${context}" -n "${system_namespace}" wait --for=delete \
	"pod/${failed_execution_pod}" --timeout=60s >/dev/null

execution_takeover=false
for _ in {1..100}; do
	snapshot_attempts "${run_directory}/execution-attempts-after.json"
	if jq -e --slurpfile before "${run_directory}/failed-owner-attempts.json" --arg owner "${failed_owner}" '
		$before[0] as $old
		| [.[] | select(.attempt_id as $id | ($old | map(.attempt_id) | index($id)))] as $new
		| ($new | length) == ($old | length)
		and all($new[]; . as $current | any($old[];
			.attempt_id == $current.attempt_id
			and .external_id == $current.external_id
			and .task_id == $current.task_id
			and .lease_token != $current.lease_token))
		and all($new[]; .lease_owner != $owner)
	' "${run_directory}/execution-attempts-after.json" >/dev/null; then
		execution_takeover=true
		break
	fi
	sleep 0.5
done
[[ "${execution_takeover}" == true ]] || { printf 'Execution lease takeover was not observed.\n' >&2; exit 1; }
snapshot_jobs_for_attempts "${run_directory}/execution-attempts-before.json" \
	"${run_directory}/execution-jobs-after.json"
cmp "${run_directory}/execution-jobs-before.json" "${run_directory}/execution-jobs-after.json"
printf 'PASS: a surviving execution acquired new fencing tokens without replacing attempts or Jobs.\n'

snapshot_attempts "${run_directory}/job-attempts-before.json"
snapshot_jobs_for_attempts "${run_directory}/job-attempts-before.json" \
	"${run_directory}/job-jobs-before.json"
failed_job_service_pod="${api_forward_pod}"
printf '\nDeleting Job service endpoint Pod %s...\n' "${failed_job_service_pod}"
kubectl --context "${context}" -n "${system_namespace}" logs "${failed_job_service_pod}" \
	> "${run_directory}/deleted-job.log"
kubectl --context "${context}" -n "${system_namespace}" delete pod "${failed_job_service_pod}" --wait=false >/dev/null
kubectl --context "${context}" -n "${system_namespace}" wait --for=delete \
	"pod/${failed_job_service_pod}" --timeout=60s >/dev/null
start_api_forward
wait_for_api
get_job_status "${run_directory}/job-status-after.json"
snapshot_attempts "${run_directory}/job-attempts-after.json"
assert_attempt_identities "${run_directory}/job-attempts-before.json" \
	"${run_directory}/job-attempts-after.json"
snapshot_jobs_for_attempts "${run_directory}/job-attempts-before.json" \
	"${run_directory}/job-jobs-after.json"
cmp "${run_directory}/job-jobs-before.json" "${run_directory}/job-jobs-after.json"
printf 'PASS: REST recovered through a second Job service Pod without replacing durable work.\n'

snapshot_attempts "${run_directory}/postgres-attempts-before.json"
snapshot_jobs_for_attempts "${run_directory}/postgres-attempts-before.json" \
	"${run_directory}/postgres-jobs-before.json"
failed_primary="$(database_primary)"
printf '\nDeleting PostgreSQL primary Pod %s...\n' "${failed_primary}"
kubectl --context "${context}" -n "${database_namespace}" logs "${failed_primary}" -c postgres \
	> "${run_directory}/deleted-postgres-primary.log" 2>&1 || true
kubectl --context "${context}" -n "${database_namespace}" delete pod "${failed_primary}" --wait=false >/dev/null

promoted_primary=""
for _ in {1..120}; do
	candidate="$(database_primary 2>/dev/null || true)"
	if [[ -n "${candidate}" && "${candidate}" != "${failed_primary}" ]] && \
		[[ "$(kubectl --context "${context}" -n "${database_namespace}" get pod "${candidate}" \
			-o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" == True ]]; then
		promoted_primary="${candidate}"
		break
	fi
	sleep 0.5
done
[[ -n "${promoted_primary}" ]] || { printf 'CloudNativePG did not promote the standby.\n' >&2; exit 1; }
wait_for_database_query 'SELECT 1;' 1 > "${run_directory}/database-after-promotion.txt" || {
	printf 'Promoted PostgreSQL writer did not accept queries.\n' >&2
	exit 1
}
get_job_status "${run_directory}/postgres-status-after.json"
snapshot_attempts "${run_directory}/postgres-attempts-after.json"
assert_attempt_identities "${run_directory}/postgres-attempts-before.json" \
	"${run_directory}/postgres-attempts-after.json"
snapshot_jobs_for_attempts "${run_directory}/postgres-attempts-before.json" \
	"${run_directory}/postgres-jobs-after.json"
cmp "${run_directory}/postgres-jobs-before.json" "${run_directory}/postgres-jobs-after.json"
printf 'PASS: CloudNativePG promoted %s and Mill recovered its database connections.\n' "${promoted_primary}"

finished=false
deadline=$((SECONDS + 300))
last_progress=""
while (( SECONDS < deadline )); do
	get_job_status "${run_directory}/status.json"
	state="$(jq -r '.state' "${run_directory}/status.json")"
	progress="$(jq -c '{state,progress}' "${run_directory}/status.json")"
	if [[ "${progress}" != "${last_progress}" ]]; then
		printf '%s\n' "${progress}"
		last_progress="${progress}"
	fi
	if [[ "${state}" == completed ]]; then finished=true; break; fi
	[[ "${state}" != failed ]] || break
	sleep 0.5
done
[[ "${finished}" == true ]] || { printf 'The availability batch did not complete.\n' >&2; exit 1; }

final_attempt_shape="$(database_query "
	SELECT count(*)::text || '|' || max(a.attempt_number)::text
	FROM attempts a JOIN tasks t ON t.id = a.task_id
	WHERE t.job_id = '${job_id}'::uuid;")"
[[ "${final_attempt_shape}" == '12|1' ]] || {
	printf 'Failover produced unexpected attempts: %s\n' "${final_attempt_shape}" >&2
	exit 1
}
jq -e '.progress.total == 12 and .progress.completed == 12 and (.results | length) == 12' \
	"${run_directory}/status.json" >/dev/null

mapfile -t result_uris < <(jq -r '.results[].uri' "${run_directory}/status.json")
partials=()
for index in "${!result_uris[@]}"; do
	object_path="${result_uris[index]#s3://}"
	partial="${run_directory}/output/task-${index}.jsonl"
	curl "${curl_signature[@]}" "${MILL_S3_ENDPOINT%/}/${object_path}" > "${partial}"
	partials+=("${partial}")
done
go run ./examples/word-count/cmd/merge "${partials[@]}" > "${run_directory}/counts.jsonl"
input_size="$(wc -c < "${run_directory}/input/records.jsonl")"
go run ./examples/word-count/cmd/word-count --job-id "${job_id}" --task-id local-baseline \
	--shard-index 0 --input-uri "file://${run_directory}/input/records.jsonl" \
	--input-start-byte 0 --input-end-byte "${input_size}" \
	--output-uri "file://${run_directory}/baseline.jsonl" --
cmp "${run_directory}/baseline.jsonl" "${run_directory}/counts.jsonl"

kubectl --context "${context}" -n "${system_namespace}" rollout status deployment/mill-job --timeout=120s
kubectl --context "${context}" -n "${system_namespace}" rollout status deployment/mill-execution --timeout=120s
assert_two_node_spread "${system_namespace}" app.kubernetes.io/name=mill-job \
	'Job service after recovery' "${run_directory}/job-placement-final.json"
assert_two_node_spread "${system_namespace}" app.kubernetes.io/name=mill-execution \
	'Execution after recovery' "${run_directory}/execution-placement-final.json"
kubectl --context "${context}" -n "${database_namespace}" wait --for=condition=Ready \
	cluster/mill-postgres --timeout=180s
assert_two_node_spread "${database_namespace}" cnpg.io/cluster=mill-postgres \
	'PostgreSQL after recovery' "${run_directory}/postgres-placement-final.json"
wait_for_database_query \
	"SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming' AND sync_state IN ('sync', 'quorum');" \
	1 > "${run_directory}/final-synchronous-standbys.txt" || {
	printf 'PostgreSQL did not restore its synchronous standby.\n' >&2
	exit 1
}

printf '\nPASS: Milestone 7 two-node Pod and controlled database failover exercise completed.\n'
printf 'Original primary: %s\nPromoted primary: %s\n' "${initial_primary}" "${promoted_primary}"
printf 'Exact merged result: %s/counts.jsonl\nEvidence: %s\n' "${run_directory}" "${run_directory}"
