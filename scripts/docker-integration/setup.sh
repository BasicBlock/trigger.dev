#!/usr/bin/env bash
set -euo pipefail

start_stack="${DOCKER_INTEGRATION_START_STACK:-1}"
compose_project="${DOCKER_INTEGRATION_COMPOSE_PROJECT:-triggerdotdev-dev-docker}"
compose_file="${DOCKER_INTEGRATION_COMPOSE_FILE:-docker/dev-compose.yml}"

ensure_prereq() {
  local cmd="$1"
  if ! command -v "${cmd}" >/dev/null 2>&1; then
    echo "${cmd} is required but was not found in PATH"
    exit 1
  fi
}

ensure_docker_daemon() {
  if ! docker info >/dev/null 2>&1; then
    cat <<EOF
Docker CLI is installed, but the Docker daemon is not reachable.
Start Docker Desktop (or dockerd) and rerun this command.
EOF
    exit 1
  fi
}

ensure_prereq docker
ensure_prereq pnpm
ensure_docker_daemon

if [[ "${start_stack}" == "1" ]]; then
  if [[ ! -f .env ]]; then
    if [[ ! -f .env.example ]]; then
      echo "DOCKER_INTEGRATION_START_STACK=1 requires .env or .env.example at repository root."
      exit 1
    fi

    cp .env.example .env
    echo "Created .env from .env.example"
  fi

  export DOCKER_BUILDKIT="${DOCKER_BUILDKIT:-1}"
  export BUILDKIT_PROGRESS="${BUILDKIT_PROGRESS:-plain}"

  echo "Building Docker integration images (${compose_project})"
  docker compose --progress=plain -p "${compose_project}" -f "${compose_file}" build

  echo "Starting Docker integration stack (${compose_project})"
  docker compose -p "${compose_project}" -f "${compose_file}" up -d --remove-orphans --no-build
fi

echo "Docker integration setup complete"
