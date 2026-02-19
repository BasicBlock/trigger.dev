#!/usr/bin/env bash
set -euo pipefail

cluster_name="${K8S_TEST_CLUSTER:-trigger-test}"
namespace="${K8S_TEST_NAMESPACE:-trigger-k8s-integration}"
stack_mode="${K8S_STACK_MODE:-}"
release_name="${K8S_STACK_RELEASE_NAME:-trigger-k8s-int}"
stack_namespace="${K8S_STACK_NAMESPACE:-trigger}"
stack_chart_dir="${K8S_STACK_CHART_DIR:-hosting/k8s/helm}"
wait_api_url="${K8S_WAIT_API_URL:-}"
wait_port="${K8S_WAIT_API_PORT:-3030}"
PORT_FORWARD_PID=""
AUTO_ENV_CREATED=0

cleanup() {
  if [[ -n "${PORT_FORWARD_PID}" ]]; then
    kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
  fi

  if [[ "${AUTO_ENV_CREATED}" == "1" ]]; then
    rm -f .env
  fi
}

trap cleanup EXIT

./scripts/k8s-integration/setup-kind.sh "${cluster_name}" "${namespace}"

if [[ -z "${stack_mode}" ]]; then
  if [[ -n "${K8S_INTEGRATION_DEPLOY_HELM:-}" ]]; then
    if [[ "${K8S_INTEGRATION_DEPLOY_HELM}" == "1" ]]; then
      stack_mode="helm"
    else
      stack_mode="none"
    fi
  else
    stack_mode="docker"
  fi
fi

if [[ "${stack_mode}" == "helm" ]]; then
  export K8S_INTEGRATION_DEPLOY_HELM=1
elif [[ "${stack_mode}" == "none" ]]; then
  export K8S_INTEGRATION_DEPLOY_HELM=0
else
  export K8S_INTEGRATION_DEPLOY_HELM=0
fi

# auto: enable only if prerequisites can be discovered
export K8S_WAIT_TEST_ENABLED="${K8S_WAIT_TEST_ENABLED:-auto}"

ensure_prereq() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "${cmd} is required but was not found in PATH"
    exit 1
  fi
}

deploy_stack_if_requested() {
  case "${stack_mode}" in
    helm)
      deploy_helm_stack
      ;;
    docker)
      deploy_docker_stack
      ;;
    none)
      ;;
    *)
      echo "Unsupported K8S_STACK_MODE=${stack_mode}. Expected one of: docker, helm, none"
      exit 1
      ;;
  esac
}

deploy_helm_stack() {
  if [[ "${K8S_INTEGRATION_DEPLOY_HELM}" != "1" ]]; then
    return
  fi

  ensure_prereq helm
  ensure_prereq kubectl

  echo "Deploying Trigger stack to kind via Helm (release: ${release_name}, namespace: ${stack_namespace})"

  (
    cd "${stack_chart_dir}"
    # Use --dependency-update so helm resolves chart dependencies in the same operation.
    # This avoids local charts/ drift causing "missing in charts/ directory" failures.
    # Disable chart hook test templates in this integration workflow.
    helm upgrade --install "${release_name}" . -n "${stack_namespace}" --create-namespace --dependency-update \
      --set tests.enabled=false \
      >/dev/null
  )

  kubectl wait --for=condition=available "deployment/${release_name}-webapp" -n "${stack_namespace}" --timeout=10m >/dev/null
}

deploy_docker_stack() {
  ensure_prereq docker
  ensure_prereq pnpm

  if [[ ! -f .env ]]; then
    if [[ ! -f .env.example ]]; then
      echo "Docker stack mode requires .env or .env.example at repository root."
      exit 1
    fi

    cp .env.example .env
    AUTO_ENV_CREATED=1
    echo "Created temporary .env from .env.example for integration run"
  fi

  echo "Starting Trigger stack via docker/dev-compose.yml"
  pnpm run dev:docker >/dev/null
}

wait_for_webapp() {
  local url="$1"
  local max_attempts=90
  local attempt=1

  while [[ $attempt -le $max_attempts ]]; do
    if curl -fsS "${url}/healthcheck" >/dev/null 2>&1 || curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    attempt=$((attempt + 1))
  done

  return 1
}

start_port_forward_if_needed() {
  if [[ "${K8S_WAIT_TEST_ENABLED}" == "0" ]]; then
    return
  fi

  if [[ -n "${wait_api_url}" ]]; then
    export K8S_WAIT_API_URL="${wait_api_url}"
    return
  fi

  ensure_prereq curl

  if [[ "${stack_mode}" == "docker" ]]; then
    export K8S_WAIT_API_URL="http://127.0.0.1:${wait_port}"
    if ! wait_for_webapp "${K8S_WAIT_API_URL}"; then
      echo "Timed out waiting for webapp at ${K8S_WAIT_API_URL} in docker stack"
      exit 1
    fi
    return
  fi

  if [[ "${stack_mode}" != "helm" ]]; then
    return
  fi

  ensure_prereq kubectl

  local service_name="${K8S_STACK_WEBAPP_SERVICE:-${release_name}-webapp}"
  if ! kubectl get svc "${service_name}" -n "${stack_namespace}" >/dev/null 2>&1; then
    cat <<EOF
K8S_WAIT_TEST_ENABLED=1 but no webapp service was found in kind.
Expected service: ${service_name} (namespace: ${stack_namespace})

Options:
1) Set K8S_INTEGRATION_DEPLOY_HELM=1 to deploy the stack in kind automatically.
2) Set K8S_WAIT_API_URL to an already-running webapp endpoint.
EOF
    exit 1
  fi

  echo "Starting port-forward for ${service_name} on localhost:${wait_port}"
  kubectl port-forward "svc/${service_name}" "${wait_port}:3030" -n "${stack_namespace}" >/tmp/k8s-integration-port-forward.log 2>&1 &
  PORT_FORWARD_PID=$!

  export K8S_WAIT_API_URL="http://127.0.0.1:${wait_port}"

  if ! wait_for_webapp "${K8S_WAIT_API_URL}"; then
    echo "Timed out waiting for webapp via port-forward at ${K8S_WAIT_API_URL}"
    echo "Port-forward logs:"
    cat /tmp/k8s-integration-port-forward.log || true
    exit 1
  fi
}

postgres_exec() {
  local sql="$1"
  case "${stack_mode}" in
    helm)
      postgres_exec_k8s "${sql}"
      ;;
    docker)
      postgres_exec_docker "${sql}"
      ;;
  esac
}

postgres_exec_k8s() {
  local sql="$1"
  local escaped_sql
  escaped_sql="$(printf '%q' "${sql}")"
  local postgres_pod="${K8S_STACK_POSTGRES_POD:-}"
  if [[ -z "${postgres_pod}" ]]; then
    postgres_pod="$(kubectl get pods -n "${stack_namespace}" -l app.kubernetes.io/name=postgresql -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  fi

  if [[ -z "${postgres_pod}" ]]; then
    postgres_pod="${release_name}-postgresql-0"
  fi

  kubectl exec -n "${stack_namespace}" "${postgres_pod}" -- sh -lc "PGPASSWORD=postgres psql -U postgres -d main -tAc ${escaped_sql}" 2>/dev/null || true
}

postgres_exec_docker() {
  local sql="$1"
  local escaped_sql
  escaped_sql="$(printf '%q' "${sql}")"
  local postgres_container="${K8S_DOCKER_POSTGRES_CONTAINER:-}"

  if [[ -z "${postgres_container}" ]]; then
    if docker ps --format '{{.Names}}' | grep -qx 'db-dev'; then
      postgres_container="db-dev"
    elif docker ps --format '{{.Names}}' | grep -qx 'database'; then
      postgres_container="database"
    else
      return
    fi
  fi

  local db
  for db in main postgres; do
    local result
    result="$(docker exec "${postgres_container}" sh -lc "PGPASSWORD=postgres psql -U postgres -d ${db} -tAc ${escaped_sql}" 2>/dev/null || true)"
    if [[ -n "${result}" ]]; then
      echo "${result}"
      return
    fi
  done
}

discover_wait_test_inputs_if_needed() {
  if [[ "${K8S_WAIT_TEST_ENABLED}" == "0" ]]; then
    return
  fi

  if [[ "${stack_mode}" == "helm" ]]; then
    ensure_prereq kubectl
  elif [[ "${stack_mode}" == "docker" ]]; then
    ensure_prereq docker
  fi

  if [[ -z "${K8S_WAIT_API_KEY:-}" ]]; then
    local discovered_key
    discovered_key="$(postgres_exec "SELECT \"apiKey\" FROM \"RuntimeEnvironment\" WHERE \"apiKey\" IS NOT NULL ORDER BY \"createdAt\" DESC LIMIT 1;")"
    discovered_key="$(echo "${discovered_key}" | tr -d '[:space:]')"
    if [[ -n "${discovered_key}" ]]; then
      export K8S_WAIT_API_KEY="${discovered_key}"
      echo "Discovered K8S_WAIT_API_KEY from RuntimeEnvironment"
    fi
  fi

  if [[ -z "${K8S_WAIT_TASK_ID:-}" ]]; then
    local discovered_task_id
    discovered_task_id="$(postgres_exec "SELECT slug FROM \"BackgroundWorkerTask\" WHERE slug = 'test.wait' LIMIT 1;")"
    discovered_task_id="$(echo "${discovered_task_id}" | tr -d '[:space:]')"

    if [[ -z "${discovered_task_id}" ]]; then
      discovered_task_id="$(postgres_exec "SELECT slug FROM \"BackgroundWorkerTask\" WHERE slug ILIKE '%wait%' ORDER BY \"createdAt\" DESC LIMIT 1;")"
      discovered_task_id="$(echo "${discovered_task_id}" | tr -d '[:space:]')"
    fi

    if [[ -n "${discovered_task_id}" ]]; then
      export K8S_WAIT_TASK_ID="${discovered_task_id}"
      echo "Discovered K8S_WAIT_TASK_ID=${K8S_WAIT_TASK_ID}"
    fi
  fi

  if [[ "${K8S_WAIT_TEST_ENABLED}" == "auto" ]]; then
    if [[ -n "${K8S_WAIT_API_URL:-}" && -n "${K8S_WAIT_API_KEY:-}" && -n "${K8S_WAIT_TASK_ID:-}" ]]; then
      export K8S_WAIT_TEST_ENABLED=1
    else
      echo "Borderline waitpoint test prerequisites were not auto-discovered; skipping waitpoint test."
      echo "Missing: $( [[ -z "${K8S_WAIT_API_URL:-}" ]] && echo -n 'K8S_WAIT_API_URL ' )$( [[ -z "${K8S_WAIT_API_KEY:-}" ]] && echo -n 'K8S_WAIT_API_KEY ' )$( [[ -z "${K8S_WAIT_TASK_ID:-}" ]] && echo -n 'K8S_WAIT_TASK_ID' )"
      export K8S_WAIT_TEST_ENABLED=0
    fi
  elif [[ "${K8S_WAIT_TEST_ENABLED}" == "1" ]]; then
    if [[ -z "${K8S_WAIT_API_URL:-}" || -z "${K8S_WAIT_API_KEY:-}" || -z "${K8S_WAIT_TASK_ID:-}" ]]; then
      echo "K8S_WAIT_TEST_ENABLED=1 but required inputs are missing."
      echo "Missing: $( [[ -z "${K8S_WAIT_API_URL:-}" ]] && echo -n 'K8S_WAIT_API_URL ' )$( [[ -z "${K8S_WAIT_API_KEY:-}" ]] && echo -n 'K8S_WAIT_API_KEY ' )$( [[ -z "${K8S_WAIT_TASK_ID:-}" ]] && echo -n 'K8S_WAIT_TASK_ID' )"
      exit 1
    fi
  fi
}

deploy_stack_if_requested
start_port_forward_if_needed
discover_wait_test_inputs_if_needed

K8S_TEST_NAMESPACE="${namespace}" pnpm run --filter supervisor test:k8s
