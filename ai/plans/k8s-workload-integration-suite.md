# Docker Workload Integration Suite Plan

## Goals

- Build a local integration test suite that runs real workloads in Docker.
- Exercise warm start and suspend/restore pathways end-to-end, with strong failure diagnostics.
- Reduce iteration time for issues like:
  - run logs not surfacing in UI/event pipeline
  - short waits suspending and never restoring
  - suspend/restore breaking node IPC channels

## Constraints and assumptions

- Prioritize lightweight local iteration over production-scale realism.
- Reuse existing repository primitives:
  - existing Docker and webapp development stack (`docker/dev-compose.yml`)
  - `apps/supervisor` integration tests and clients
- Keep this suite opt-in (`pnpm test:docker`) so normal `pnpm test` stays fast.

## Architecture decision

- Use Docker Compose as the default and only local runtime for this suite.
- Run workloads as Docker containers and validate container lifecycle behavior directly.
- Keep dependencies and test services in one Docker-based topology to simplify setup and debugging.
- Add a dedicated Docker integration harness under `apps/supervisor/src/dockerIntegration`.
- Add bootstrap scripts under `scripts/docker-integration`.

## Phased implementation

### Phase 1: Foundation (implemented)

- Add Docker bootstrap script for deterministic local setup.
- Add `pnpm test:docker` scripts (root + supervisor).
- Add initial supervisor Docker integration harness utilities:
  - network/container setup and cleanup
  - container state/start/stop waiters
  - log collection helper
- Add initial smoke tests that run real containers:
  - run container to completion and assert logs are retrievable
  - replace a running container and assert replacement workload succeeds

### Phase 2: Trigger workflow harness

- Add an integration fixture that can boot stack modes:
  - `docker` (default): webapp/API/supervisor via `docker/dev-compose.yml` plus test overrides
  - `none`: external stack managed by caller
- Add test driver helpers for:
  - creating/running test workloads via API
  - polling run/snapshot/checkpoint states
  - collecting logs/events and warm-start evidence

### Phase 3: Warm start + suspend/restore scenarios

- Implement high-value scenarios:
  - log propagation survives suspend/restore
  - short waits suspend then restore and complete
  - restore preserves or re-establishes IPC behavior
  - duplicate restore requests remain idempotent
- Add fault injections:
  - kill/restart supervisor mid-restore
  - stop/remove/replace restore container during recovery

### Phase 4: Developer ergonomics and CI

- Add failure artifact capture:
  - `docker ps`, `docker inspect`, container logs, compose service logs
- Add test tagging:
  - `smoke` (quick local checks)
  - `full` (broader workflow coverage)
- Add optional CI job behind manual trigger or nightly schedule.

## Execution entrypoints

- `./scripts/docker-integration/setup.sh`
- `pnpm test:docker`

## Deliverables checklist

- [x] Plan document in `ai/plans`
- [x] Docker harness and smoke tests merged
- [x] workflow-level waitpoint suspend/restore test scaffolded
- [x] self-contained task fixture for waitpoint test (no manual task registration)
- [ ] artifact capture and CI integration merged
- [x] smoke/full test tagging added (`DOCKER_TEST_LEVEL`)
