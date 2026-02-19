#!/usr/bin/env bash
set -euo pipefail

webapp_url="${DOCKER_INTEGRATION_WEBAPP_URL:-http://127.0.0.1:3030}"
supervisor_url="${DOCKER_INTEGRATION_SUPERVISOR_URL:-http://127.0.0.1:8020}"
wait_test_enabled="${DOCKER_WAIT_TEST_ENABLED:-1}"

start_supervisor="${DOCKER_INTEGRATION_START_SUPERVISOR:-1}"
cleanup_supervisor="${DOCKER_INTEGRATION_CLEANUP_SUPERVISOR:-1}"
reuse_supervisor="${DOCKER_INTEGRATION_REUSE_SUPERVISOR:-0}"

compose_project="${DOCKER_INTEGRATION_COMPOSE_PROJECT:-triggerdotdev-dev-docker}"
compose_file="${DOCKER_INTEGRATION_COMPOSE_FILE:-docker/dev-compose.yml}"
if [[ "${compose_file}" = /* ]]; then
  compose_file_abs="${compose_file}"
else
  compose_file_abs="$(pwd)/${compose_file}"
fi

managed_worker_secret="${DOCKER_INTEGRATION_MANAGED_WORKER_SECRET:-managed-secret}"
otel_endpoint="${DOCKER_INTEGRATION_OTEL_EXPORTER_OTLP_ENDPOINT:-${webapp_url}/otel}"
runner_networks="${DOCKER_INTEGRATION_DOCKER_RUNNER_NETWORKS:-bridge}"
runner_api_url="${DOCKER_INTEGRATION_RUNNER_API_URL:-${webapp_url}}"
runner_additional_env_vars="${DOCKER_INTEGRATION_RUNNER_ADDITIONAL_ENV_VARS:-}"
deploy_registry_host="${DOCKER_INTEGRATION_DEPLOY_REGISTRY_HOST:-127.0.0.1:5001}"

if [[ "${runner_api_url}" == "${webapp_url}" ]]; then
  runner_api_url="$(
    echo "${runner_api_url}" | sed -E \
      -e 's#^http://(127\.0\.0\.1|localhost)#http://host.docker.internal#' \
      -e 's#^https://(127\.0\.0\.1|localhost)#https://host.docker.internal#'
  )"
fi

auto_seed="${DOCKER_INTEGRATION_AUTO_SEED:-1}"
wait_fixture_deploy="${DOCKER_WAIT_FIXTURE_DEPLOY:-1}"
wait_fixture_force_deploy="${DOCKER_WAIT_FIXTURE_FORCE_DEPLOY:-1}"
wait_fixture_dir="${DOCKER_WAIT_FIXTURE_DIR:-references/test-tasks}"
wait_fixture_access_token="${DOCKER_WAIT_FIXTURE_ACCESS_TOKEN:-}"
wait_fixture_env="${DOCKER_WAIT_FIXTURE_ENV:-prod}"
wait_fixture_builder="${DOCKER_WAIT_FIXTURE_BUILDER:-desktop-linux}"
wait_fixture_load_local="${DOCKER_WAIT_FIXTURE_LOAD_LOCAL:-1}"
wait_fixture_local_build="${DOCKER_WAIT_FIXTURE_LOCAL_BUILD:-1}"

supervisor_pid=""
supervisor_log="${DOCKER_INTEGRATION_SUPERVISOR_LOG:-/tmp/docker-integration-supervisor.log}"
requested_supervisor_metrics_port="${DOCKER_INTEGRATION_SUPERVISOR_METRICS_PORT:-}"

# Ensure compose interpolation uses the same registry host across deploy + runtime.
export DOCKER_INTEGRATION_DEPLOY_REGISTRY_HOST="${deploy_registry_host}"

ensure_prereq() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "${cmd} is required but was not found in PATH"
    exit 1
  fi
}

wait_for_url() {
  local name="$1"
  local url="$2"
  local max_attempts="${3:-90}"
  local hint="${4:-}"
  local attempt=1

  echo "Waiting for ${name} at ${url} (timeout: ${max_attempts}s)"

  while [[ ${attempt} -le ${max_attempts} ]]; do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "${name} is healthy"
      return 0
    fi

    if (( attempt % 10 == 0 )); then
      echo "Still waiting for ${name}... (${attempt}/${max_attempts}s)"
    fi

    sleep 1
    attempt=$((attempt + 1))
  done

  echo "Timed out waiting for ${name} at ${url}"
  if [[ -n "${hint}" ]]; then
    echo "${hint}"
  fi
  return 1
}

cleanup() {
  local exit_code=$?

  if [[ -n "${supervisor_pid}" && "${cleanup_supervisor}" == "1" ]]; then
    echo "Stopping managed supervisor (pid ${supervisor_pid})"
    kill "${supervisor_pid}" >/dev/null 2>&1 || true
    wait "${supervisor_pid}" >/dev/null 2>&1 || true
  fi

  return ${exit_code}
}

on_interrupt() {
  echo "Interrupted, cleaning up..."
  exit 130
}

trap on_interrupt INT TERM
trap cleanup EXIT

run_webapp_eval() {
  local eval_script="$1"
  shift || true

  local local_output=""
  local_output="$(
    (
      cd apps/webapp
      env "$@" pnpm dlx tsx --eval "${eval_script}"
    ) 2>/dev/null || true
  )"

  if [[ -n "$(echo "${local_output}" | tr -d '[:space:]')" ]]; then
    echo "${local_output}"
    return 0
  fi

  if ! container_is_running "app"; then
    echo "${local_output}"
    return 0
  fi

  local eval_escaped
  eval_escaped="$(printf '%q' "${eval_script}")"

  local env_prefix=""
  local kv key value value_escaped
  for kv in "$@"; do
    key="${kv%%=*}"
    value="${kv#*=}"
    value_escaped="$(printf '%q' "${value}")"
    env_prefix="${env_prefix}${key}=${value_escaped} "
  done

  docker compose -p "${compose_project}" -f "${compose_file_abs}" exec -T app sh -lc \
    "cd /triggerdotdev/apps/webapp && ${env_prefix}pnpm dlx tsx --eval ${eval_escaped}" 2>/dev/null || true
}

run_webapp_seed() {
  if (
    cd apps/webapp
    pnpm dlx tsx seed.mts >/dev/null 2>&1
  ); then
    return 0
  fi

  if ! container_is_running "app"; then
    return 1
  fi

  docker compose -p "${compose_project}" -f "${compose_file_abs}" exec -T app sh -lc \
    "cd /triggerdotdev/apps/webapp && pnpm dlx tsx seed.mts" >/dev/null 2>&1
}

container_is_running() {
  local service="$1"
  if ! command -v docker >/dev/null 2>&1; then
    return 1
  fi

  local container_id
  container_id="$(docker compose -p "${compose_project}" -f "${compose_file_abs}" ps -q "${service}" 2>/dev/null || true)"
  if [[ -z "${container_id}" ]]; then
    return 1
  fi

  docker ps -q --no-trunc | grep -q "${container_id}"
}

discover_api_key_if_needed() {
  if [[ -n "${DOCKER_WAIT_API_KEY:-}" ]]; then
    return
  fi

  local eval_script='import { prisma } from "./app/db.server"; (async () => { const env = await prisma.runtimeEnvironment.findFirst({ where: { apiKey: { not: "" } }, orderBy: { createdAt: "desc" }, select: { apiKey: true } }); if (env?.apiKey) console.log("__RESULT__" + env.apiKey + "__END__"); process.exit(0); })();'
  local discovered_key
  discovered_key="$(run_webapp_eval "${eval_script}" 2>/dev/null | sed -n 's/.*__RESULT__\(.*\)__END__.*/\1/p' | tail -n1 || true)"
  discovered_key="$(echo "${discovered_key}" | tr -d '[:space:]')"

  if [[ -z "${discovered_key}" && "${auto_seed}" == "1" ]]; then
    echo "No RuntimeEnvironment apiKey found; running webapp seed data" >&2
    if ! run_webapp_seed; then
      echo "Webapp seed command failed; continuing without seeded API key" >&2
    fi
    discovered_key="$(run_webapp_eval "${eval_script}" 2>/dev/null | sed -n 's/.*__RESULT__\(.*\)__END__.*/\1/p' | tail -n1 || true)"
    discovered_key="$(echo "${discovered_key}" | tr -d '[:space:]')"
  fi

  if [[ -n "${discovered_key}" ]]; then
    export DOCKER_WAIT_API_KEY="${discovered_key}"
    echo "Discovered DOCKER_WAIT_API_KEY from RuntimeEnvironment" >&2
  fi
}

discover_project_ref_if_needed() {
  if [[ -n "${DOCKER_WAIT_PROJECT_REF:-}" ]]; then
    return
  fi

  local discovered_ref=""
  if [[ -n "${DOCKER_WAIT_API_KEY:-}" ]]; then
    local eval_script='import { prisma } from "./app/db.server"; (async () => { const apiKey = process.env.TARGET_API_KEY; if (!apiKey) process.exit(0); const env = await prisma.runtimeEnvironment.findFirst({ where: { apiKey }, orderBy: { createdAt: "desc" }, select: { project: { select: { externalRef: true } } } }); if (env?.project?.externalRef) console.log("__RESULT__" + env.project.externalRef + "__END__"); process.exit(0); })();'
    discovered_ref="$(run_webapp_eval "${eval_script}" TARGET_API_KEY="${DOCKER_WAIT_API_KEY}" 2>/dev/null | sed -n 's/.*__RESULT__\(.*\)__END__.*/\1/p' | tail -n1 || true)"
    discovered_ref="$(echo "${discovered_ref}" | tr -d '[:space:]')"
  fi

  if [[ -z "${discovered_ref}" ]]; then
    local fallback_script='import { prisma } from "./app/db.server"; (async () => { const env = await prisma.runtimeEnvironment.findFirst({ orderBy: { createdAt: "desc" }, select: { project: { select: { externalRef: true } } } }); if (env?.project?.externalRef) console.log("__RESULT__" + env.project.externalRef + "__END__"); process.exit(0); })();'
    discovered_ref="$(run_webapp_eval "${fallback_script}" 2>/dev/null | sed -n 's/.*__RESULT__\(.*\)__END__.*/\1/p' | tail -n1 || true)"
    discovered_ref="$(echo "${discovered_ref}" | tr -d '[:space:]')"
  fi

  if [[ -n "${discovered_ref}" ]]; then
    export DOCKER_WAIT_PROJECT_REF="${discovered_ref}"
    echo "Discovered DOCKER_WAIT_PROJECT_REF=${DOCKER_WAIT_PROJECT_REF}" >&2
  fi
}

discover_wait_task_id_if_needed() {
  if [[ -n "${DOCKER_WAIT_TASK_ID:-}" ]]; then
    return
  fi

  local exact_script='import { prisma } from "./app/db.server"; (async () => { const task = await prisma.backgroundWorkerTask.findFirst({ where: { slug: "test.wait" }, select: { slug: true } }); if (task?.slug) console.log("__RESULT__" + task.slug + "__END__"); process.exit(0); })();'
  local discovered_task_id
  discovered_task_id="$(run_webapp_eval "${exact_script}" 2>/dev/null | sed -n 's/.*__RESULT__\(.*\)__END__.*/\1/p' | tail -n1 || true)"
  discovered_task_id="$(echo "${discovered_task_id}" | tr -d '[:space:]')"

  if [[ -z "${discovered_task_id}" ]]; then
    local fuzzy_script='import { prisma } from "./app/db.server"; (async () => { const task = await prisma.backgroundWorkerTask.findFirst({ where: { slug: { contains: "wait", mode: "insensitive" } }, orderBy: { createdAt: "desc" }, select: { slug: true } }); if (task?.slug) console.log("__RESULT__" + task.slug + "__END__"); process.exit(0); })();'
    discovered_task_id="$(run_webapp_eval "${fuzzy_script}" 2>/dev/null | sed -n 's/.*__RESULT__\(.*\)__END__.*/\1/p' | tail -n1 || true)"
    discovered_task_id="$(echo "${discovered_task_id}" | tr -d '[:space:]')"
  fi

  if [[ -n "${discovered_task_id}" ]]; then
    export DOCKER_WAIT_TASK_ID="${discovered_task_id}"
    echo "Discovered DOCKER_WAIT_TASK_ID=${DOCKER_WAIT_TASK_ID}"
  fi
}

create_personal_access_token_in_webapp() {
  local eval_script='import { prisma } from "./app/db.server"; import { createPersonalAccessToken } from "./app/services/personalAccessToken.server"; (async () => { const user = await prisma.user.findFirst({ orderBy: { createdAt: "asc" } }); if (!user) throw new Error("No user found to create personal access token"); const token = await createPersonalAccessToken({ name: "docker-integration-fixture", userId: user.id }); console.log("__RESULT__" + token.token + "__END__"); process.exit(0); })();'
  local output
  output="$(run_webapp_eval "${eval_script}" 2>/dev/null || true)"

  local token
  token="$(echo "${output}" | sed -n 's/.*__RESULT__\(.*\)__END__.*/\1/p' | tail -n1 || true)"
  if [[ -z "${token}" ]]; then
    echo "Failed to create personal access token from local webapp code." >&2
    return 1
  fi

  wait_fixture_access_token="${token}"
  export DOCKER_WAIT_FIXTURE_ACCESS_TOKEN="${token}"
  echo "Created DOCKER_WAIT_FIXTURE_ACCESS_TOKEN via local webapp code" >&2
  echo "${token}"
}

refresh_wait_api_key_from_project_env() {
  local project_ref="$1"

  if [[ -z "${wait_fixture_access_token}" || -z "${project_ref}" ]]; then
    return
  fi

  local response
  response="$(curl -sS \
    -H "Authorization: Bearer ${wait_fixture_access_token}" \
    "${webapp_url}/api/v1/projects/${project_ref}/${wait_fixture_env}" \
    -w $'\n%{http_code}')" || return

  local status_code
  status_code="$(echo "${response}" | tail -n1 | tr -d '\r')"
  local response_body
  response_body="$(echo "${response}" | sed '$d')"

  if [[ "${status_code}" != "200" ]]; then
    echo "Could not refresh DOCKER_WAIT_API_KEY from /api/v1/projects/${project_ref}/${wait_fixture_env} (HTTP ${status_code})." >&2
    return
  fi

  local refreshed_key
  refreshed_key="$(echo "${response_body}" | node -e 'const fs=require("fs"); const body=JSON.parse(fs.readFileSync(0,"utf8")); if(!body?.apiKey){process.exit(2)} process.stdout.write(body.apiKey)')" || return

  if [[ -n "${refreshed_key}" ]]; then
    export DOCKER_WAIT_API_KEY="${refreshed_key}"
    echo "Refreshed DOCKER_WAIT_API_KEY from project env (${wait_fixture_env})" >&2
  fi
}

ensure_wait_runtime_api_env_vars() {
  local project_ref="$1"

  if [[ -z "${wait_fixture_access_token}" || -z "${project_ref}" ]]; then
    return
  fi

  local payload
  payload="$(printf '{"variables":{"TRIGGER_API_URL":"%s","TRIGGER_STREAM_URL":"%s"},"override":true}' "${runner_api_url}" "${runner_api_url}")"

  local response
  response="$(curl -sS \
    -X POST \
    -H "Authorization: Bearer ${wait_fixture_access_token}" \
    -H "Content-Type: application/json" \
    -d "${payload}" \
    "${webapp_url}/api/v1/projects/${project_ref}/envvars/${wait_fixture_env}/import" \
    -w $'\n%{http_code}')" || return

  local status_code
  status_code="$(echo "${response}" | tail -n1 | tr -d '\r')"
  if [[ "${status_code}" != "200" ]]; then
    echo "Could not update TRIGGER_API_URL/TRIGGER_STREAM_URL for ${project_ref}/${wait_fixture_env} (HTTP ${status_code})." >&2
    return
  fi

  echo "Updated TRIGGER_API_URL/TRIGGER_STREAM_URL for ${project_ref}/${wait_fixture_env} to ${runner_api_url}" >&2
}

validate_wait_api_key_for_project_env() {
  local api_key="$1"
  local project_ref="$2"

  local status_code
  status_code="$(curl -sS \
    -o /dev/null \
    -w '%{http_code}' \
    -H "Authorization: Bearer ${api_key}" \
    "${webapp_url}/api/v1/projects/${project_ref}/${wait_fixture_env}" || true)"

  [[ "${status_code}" == "200" ]]
}

ensure_valid_wait_api_key_for_project_env() {
  discover_project_ref_if_needed
  local project_ref="${DOCKER_WAIT_PROJECT_REF:-}"
  if [[ -z "${project_ref}" ]]; then
    return
  fi

  if [[ -n "${DOCKER_WAIT_API_KEY:-}" ]] && validate_wait_api_key_for_project_env "${DOCKER_WAIT_API_KEY}" "${project_ref}"; then
    return
  fi

  local candidate_script='import { prisma } from "./app/db.server"; (async () => { const projectRef = process.env.PROJECT_REF; if (!projectRef) process.exit(0); const envs = await prisma.runtimeEnvironment.findMany({ where: { project: { externalRef: projectRef }, apiKey: { not: "" } }, orderBy: { createdAt: "desc" }, take: 50, select: { apiKey: true } }); for (const env of envs) { if (env.apiKey) console.log("__RESULT__" + env.apiKey + "__END__"); } process.exit(0); })();'
  local candidates
  candidates="$(run_webapp_eval "${candidate_script}" PROJECT_REF="${project_ref}" 2>/dev/null | sed -n 's/.*__RESULT__\(.*\)__END__.*/\1/p' || true)"

  local candidate
  while IFS= read -r candidate; do
    candidate="$(echo "${candidate}" | tr -d '[:space:]')"
    if [[ -z "${candidate}" ]]; then
      continue
    fi
    if validate_wait_api_key_for_project_env "${candidate}" "${project_ref}"; then
      export DOCKER_WAIT_API_KEY="${candidate}"
      echo "Discovered valid DOCKER_WAIT_API_KEY for ${project_ref}/${wait_fixture_env}" >&2
      return
    fi
  done <<<"${candidates}"

  if [[ -z "${wait_fixture_access_token}" ]]; then
    wait_fixture_access_token="$(create_personal_access_token_in_webapp || true)"
  fi
  if [[ -n "${wait_fixture_access_token}" ]]; then
    refresh_wait_api_key_from_project_env "${project_ref}"
  fi
}

create_worker_token_in_webapp_for_project() {
  local group_name="$1"
  local project_ref="$2"
  local eval_script='import { prisma } from "./app/db.server"; import { WorkerGroupService } from "./app/v3/services/worker/workerGroupService.server"; (async () => { const projectRef = process.env.PROJECT_REF; if (!projectRef) throw new Error("PROJECT_REF is required"); const groupName = process.env.GROUP_NAME ?? "docker-integration"; const project = await prisma.project.findFirst({ where: { externalRef: projectRef }, select: { id: true, organizationId: true } }); if (!project) throw new Error("Project not found for externalRef=" + projectRef); const service = new WorkerGroupService(); const { workerGroup, token } = await service.createWorkerGroup({ projectId: project.id, organizationId: project.organizationId, name: groupName }); await service.setDefaultWorkerGroupForProject({ workerGroupId: workerGroup.id, projectId: project.id }); console.log("__RESULT__" + token.plaintext + "__END__"); process.exit(0); })();'
  local output
  output="$(run_webapp_eval "${eval_script}" GROUP_NAME="${group_name}" PROJECT_REF="${project_ref}" 2>/dev/null || true)"

  local token
  token="$(echo "${output}" | sed -n 's/.*__RESULT__\(.*\)__END__.*/\1/p' | tail -n1 || true)"
  if [[ -z "${token}" ]]; then
    echo "Failed to create project-scoped worker token from local webapp code." >&2
    return 1
  fi

  echo "${token}"
}

create_worker_token() {
  if [[ -n "${DOCKER_INTEGRATION_WORKER_TOKEN:-}" ]]; then
    echo "${DOCKER_INTEGRATION_WORKER_TOKEN}"
    return
  fi

  discover_api_key_if_needed
  discover_project_ref_if_needed

  local api_key="${DOCKER_WAIT_API_KEY:-}"
  local project_ref="${DOCKER_WAIT_PROJECT_REF:-}"
  if [[ -z "${api_key}" ]]; then
    echo "Could not auto-create worker token because DOCKER_WAIT_API_KEY is missing." >&2
    echo "Set DOCKER_INTEGRATION_WORKER_TOKEN manually or ensure RuntimeEnvironment.apiKey exists." >&2
    return 1
  fi
  if [[ -z "${project_ref}" ]]; then
    echo "Could not auto-create worker token because DOCKER_WAIT_PROJECT_REF is missing." >&2
    return 1
  fi

  local group_name="docker-integration-$(date +%s)"
  create_worker_token_in_webapp_for_project "${group_name}" "${project_ref}"
}

find_available_supervisor_url() {
  local base_url="$1"
  local base_prefix="${base_url%:*}"
  local port="${base_url##*:}"

  while true; do
    if ! port_is_listening "${port}"; then
      echo "${base_prefix}:${port}"
      return
    fi
    port=$((port + 1))
  done
}

find_available_port() {
  local start_port="$1"
  local port="${start_port}"

  while true; do
    if ! port_is_listening "${port}"; then
      echo "${port}"
      return
    fi
    port=$((port + 1))
  done
}

port_is_listening() {
  local port="$1"

  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"${port}" -sTCP:LISTEN -n -P >/dev/null 2>&1
    return
  fi

  if command -v nc >/dev/null 2>&1; then
    nc -z 127.0.0.1 "${port}" >/dev/null 2>&1
    return
  fi

  return 1
}

start_managed_supervisor_if_needed() {
  if [[ "${start_supervisor}" != "1" ]]; then
    return
  fi

  local requested_supervisor_url="${supervisor_url}"
  if curl -fsS "${supervisor_url}/health" >/dev/null 2>&1; then
    if [[ "${reuse_supervisor}" == "1" ]]; then
      echo "Supervisor already reachable at ${supervisor_url}; using existing instance"
      return
    fi

    supervisor_url="$(find_available_supervisor_url "${requested_supervisor_url}")"
    echo "Supervisor already running at ${requested_supervisor_url}; starting dedicated integration supervisor at ${supervisor_url}"
  fi

  discover_api_key_if_needed
  ensure_valid_wait_api_key_for_project_env

  local worker_token
  if ! worker_token="$(create_worker_token)"; then
    exit 1
  fi

  local supervisor_port="${supervisor_url##*:}"
  local supervisor_metrics_port="${requested_supervisor_metrics_port}"
  if [[ -z "${supervisor_metrics_port}" ]]; then
    supervisor_metrics_port="$(find_available_port 9090)"
  fi

  echo "Starting managed supervisor for integration suite (host process)"
  local effective_runner_env_vars="${runner_additional_env_vars}"
  if [[ -n "${effective_runner_env_vars}" ]]; then
    effective_runner_env_vars="${effective_runner_env_vars},DOCKER_WAIT_TASK_API_URL=${runner_api_url}"
  else
    effective_runner_env_vars="DOCKER_WAIT_TASK_API_URL=${runner_api_url}"
  fi

  (
    export PORT="${supervisor_port}"
    export TRIGGER_WORKLOAD_API_PORT_INTERNAL="${supervisor_port}"
    export TRIGGER_WORKLOAD_API_PORT_EXTERNAL="${supervisor_port}"
    export METRICS_PORT="${supervisor_metrics_port}"
    export TRIGGER_API_URL="${webapp_url}"
    export RUNNER_ADDITIONAL_ENV_VARS="${effective_runner_env_vars}"
    export TRIGGER_WORKER_TOKEN="${worker_token}"
    export MANAGED_WORKER_SECRET="${managed_worker_secret}"
    export OTEL_EXPORTER_OTLP_ENDPOINT="${otel_endpoint}"
    export DOCKER_RUNNER_NETWORKS="${runner_networks}"
    pnpm run --filter supervisor dev >"${supervisor_log}" 2>&1
  ) &
  supervisor_pid=$!

  if ! wait_for_url \
    "supervisor" \
    "${supervisor_url}/health" \
    120 \
    "Managed supervisor failed to start. Check logs at ${supervisor_log}."; then
    echo "----- supervisor log tail -----"
    tail -n 200 "${supervisor_log}" || true
    echo "----- end supervisor log tail -----"
    exit 1
  fi

  echo "Managed supervisor is healthy at ${supervisor_url}"
  echo "Managed supervisor metrics listening on 127.0.0.1:${supervisor_metrics_port}"
}

ensure_external_supervisor_if_needed() {
  if [[ "${start_supervisor}" == "1" ]]; then
    return
  fi

  if ! wait_for_url \
    "supervisor" \
    "${supervisor_url}/health" \
    30 \
    "Supervisor is not reachable. Start it first or set DOCKER_INTEGRATION_START_SUPERVISOR=1."; then
    exit 1
  fi
}

deploy_wait_fixture_if_needed() {
  if [[ "${wait_fixture_deploy}" != "1" ]]; then
    return
  fi

  if [[ "${wait_fixture_force_deploy}" != "1" && -n "${DOCKER_WAIT_TASK_ID:-}" ]]; then
    return
  fi

  if [[ ! -d "${wait_fixture_dir}" ]]; then
    echo "Waitpoint fixture directory not found: ${wait_fixture_dir}" >&2
    return 1
  fi

  discover_api_key_if_needed
  discover_project_ref_if_needed

  local api_key="${DOCKER_WAIT_API_KEY:-}"
  local project_ref="${DOCKER_WAIT_PROJECT_REF:-}"
  if [[ -z "${api_key}" || -z "${project_ref}" ]]; then
    echo "Cannot auto-deploy waitpoint fixture; missing DOCKER_WAIT_API_KEY or DOCKER_WAIT_PROJECT_REF." >&2
    return 1
  fi

  if [[ -z "${wait_fixture_access_token}" ]]; then
    wait_fixture_access_token="$(create_personal_access_token_in_webapp || true)"
  fi
  if [[ -z "${wait_fixture_access_token}" ]]; then
    echo "Cannot auto-deploy waitpoint fixture; failed to create a personal access token." >&2
    return 1
  fi

  refresh_wait_api_key_from_project_env "${project_ref}"
  ensure_wait_runtime_api_env_vars "${project_ref}"

  echo "Deploying waitpoint fixture task from ${wait_fixture_dir} to project ${project_ref}" >&2
  if ! (
    cd "${wait_fixture_dir}"
    deploy_cmd=(
      pnpm exec trigger deploy
      --project-ref "${project_ref}"
      --api-url "${webapp_url}"
    )
    if [[ "${wait_fixture_local_build}" == "1" ]]; then
      deploy_cmd+=(--local-build)
    fi
    if [[ -n "${wait_fixture_builder}" ]]; then
      deploy_cmd+=(--builder "${wait_fixture_builder}")
    fi
    if [[ "${wait_fixture_load_local}" == "1" ]]; then
      deploy_cmd+=(--load --no-push)
    else
      deploy_cmd+=(--push)
    fi
    CI=1 \
    TRIGGER_ACCESS_TOKEN="${wait_fixture_access_token}" \
    TRIGGER_API_URL="${webapp_url}" \
    TRIGGER_PROJECT_REF="${project_ref}" \
    "${deploy_cmd[@]}"
  ); then
    echo "Waitpoint fixture deploy failed." >&2
    return 1
  else
    refresh_wait_api_key_from_project_env "${project_ref}"
  fi

  return 0
}

discover_wait_inputs() {
  if [[ "${wait_test_enabled}" == "0" ]]; then
    export DOCKER_WAIT_TEST_ENABLED=0
    return
  fi

  discover_api_key_if_needed
  ensure_valid_wait_api_key_for_project_env
  discover_wait_task_id_if_needed
  if ! deploy_wait_fixture_if_needed; then
    echo "Failed to prepare waitpoint fixture deployment." >&2
    exit 1
  fi
  discover_wait_task_id_if_needed

  if [[ -z "${DOCKER_WAIT_API_URL:-}" ]]; then
    export DOCKER_WAIT_API_URL="${webapp_url}"
  fi

  if [[ "${wait_test_enabled}" == "auto" ]]; then
    if [[ -n "${DOCKER_WAIT_API_URL:-}" && -n "${DOCKER_WAIT_API_KEY:-}" && -n "${DOCKER_WAIT_TASK_ID:-}" ]]; then
      export DOCKER_WAIT_TEST_ENABLED=1
    else
      echo "Waitpoint test prerequisites were not auto-discovered; skipping waitpoint test."
      echo "Missing: $( [[ -z "${DOCKER_WAIT_API_URL:-}" ]] && echo -n 'DOCKER_WAIT_API_URL ' )$( [[ -z "${DOCKER_WAIT_API_KEY:-}" ]] && echo -n 'DOCKER_WAIT_API_KEY ' )$( [[ -z "${DOCKER_WAIT_TASK_ID:-}" ]] && echo -n 'DOCKER_WAIT_TASK_ID' )"
      export DOCKER_WAIT_TEST_ENABLED=0
    fi
  elif [[ "${wait_test_enabled}" == "1" ]]; then
    if [[ -z "${DOCKER_WAIT_API_URL:-}" || -z "${DOCKER_WAIT_API_KEY:-}" || -z "${DOCKER_WAIT_TASK_ID:-}" ]]; then
      echo "DOCKER_WAIT_TEST_ENABLED=1 but required inputs are missing."
      echo "Missing: $( [[ -z "${DOCKER_WAIT_API_URL:-}" ]] && echo -n 'DOCKER_WAIT_API_URL ' )$( [[ -z "${DOCKER_WAIT_API_KEY:-}" ]] && echo -n 'DOCKER_WAIT_API_KEY ' )$( [[ -z "${DOCKER_WAIT_TASK_ID:-}" ]] && echo -n 'DOCKER_WAIT_TASK_ID' )"
      exit 1
    fi
    export DOCKER_WAIT_TEST_ENABLED=1
  else
    echo "Unsupported DOCKER_WAIT_TEST_ENABLED=${wait_test_enabled}. Use 0, 1, or auto."
    exit 1
  fi
}

ensure_prereq pnpm
ensure_prereq curl
ensure_prereq node

if ! wait_for_url \
  "webapp" \
  "${webapp_url}/healthcheck" \
  180 \
  "Webapp did not become healthy. Ensure your local dev environment is already running."; then
  exit 1
fi

start_managed_supervisor_if_needed
ensure_external_supervisor_if_needed
discover_wait_inputs

export DOCKER_INTEGRATION_WEBAPP_URL="${webapp_url}"
export DOCKER_INTEGRATION_SUPERVISOR_URL="${supervisor_url}"
export DOCKER_INTEGRATION_COMPOSE_PROJECT="${compose_project}"
export DOCKER_INTEGRATION_COMPOSE_FILE="${compose_file_abs}"

pnpm run --filter supervisor test:docker
