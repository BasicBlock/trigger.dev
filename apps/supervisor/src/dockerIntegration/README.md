# Supervisor Docker Integration Tests

These tests run against a local stack with real `webapp`, `supervisor`, and Docker runner workloads.

## Run

From repository root:

```bash
pnpm test:docker
```

This command will:

1. Start local Docker stack (`docker/dev-compose.yml`) by default
2. Wait for webapp health, create a worker token, and start a managed supervisor host process (not a Docker image)
3. Auto-wire waitpoint test API URL and auto-discover API key/task ID when possible
4. Auto-deploy a `test.wait` fixture task when missing (from `references/test-tasks`)
4. Run `apps/supervisor` Docker integration tests
5. Tear down supervisor and Docker stack at the end (success or failure)

## Environment

Core:

- `DOCKER_INTEGRATION_START_STACK=1|0` (default `1`)
- `DOCKER_INTEGRATION_CLEANUP_STACK=1|0` (default `1`)
- `DOCKER_INTEGRATION_START_SUPERVISOR=1|0` (default `1`)
- `DOCKER_INTEGRATION_CLEANUP_SUPERVISOR=1|0` (default `1`)
- `DOCKER_INTEGRATION_WEBAPP_URL` (default `http://127.0.0.1:3030`)
- `DOCKER_INTEGRATION_SUPERVISOR_URL` (default `http://127.0.0.1:8020`)
- `DOCKER_INTEGRATION_DEPLOY_REGISTRY_HOST` (default `127.0.0.1:5001`) registry host injected into webapp deploy image refs
- `DOCKER_INTEGRATION_POSTGRES_CONTAINER` (optional explicit Postgres container)
- `DOCKER_INTEGRATION_WORKER_TOKEN` (optional; skips auto worker token creation)
- `DOCKER_INTEGRATION_MANAGED_WORKER_SECRET` (default `managed-secret`)
- `DOCKER_INTEGRATION_OTEL_EXPORTER_OTLP_ENDPOINT` (default `${DOCKER_INTEGRATION_WEBAPP_URL}/otel`)
- `DOCKER_INTEGRATION_DOCKER_RUNNER_NETWORKS` (default `bridge`)
- `DOCKER_INTEGRATION_RUNNER_API_URL` (default rewrites localhost/127.0.0.1 webapp URL to `host.docker.internal`) API URL injected into runner containers as `DOCKER_WAIT_TASK_API_URL`
- `DOCKER_INTEGRATION_RUNNER_ADDITIONAL_ENV_VARS` (optional csv `KEY=value,KEY2=value2`) appended to runner env vars
- `DOCKER_TEST_ARTIFACTS_DIR` diagnostics output directory on failure

When auto-deploying the wait fixture, the runner API URL is also synced into project env vars (`TRIGGER_API_URL` and `TRIGGER_STREAM_URL`) for the target environment.

Waitpoint suspend/restore test:

- `DOCKER_WAIT_TEST_ENABLED=auto|1|0` (default `auto`)
- `DOCKER_WAIT_API_URL` (default from `DOCKER_INTEGRATION_WEBAPP_URL`)
- `DOCKER_WAIT_API_KEY` (environment API key with trigger/read scopes)
- `DOCKER_WAIT_TASK_ID` (example `test.wait`)
- `DOCKER_WAIT_FIXTURE_DEPLOY=1|0` (default `1`) to auto-deploy the `test.wait` fixture
- `DOCKER_WAIT_FIXTURE_FORCE_DEPLOY=1|0` (default `1`) to always redeploy `test.wait` even if already present
- `DOCKER_WAIT_FIXTURE_DIR` (default `references/test-tasks`)
- `DOCKER_WAIT_FIXTURE_LOCAL_BUILD=1|0` (default `1`) pass `--local-build` to `trigger deploy` so fixture images are built by the local Docker daemon
- `DOCKER_WAIT_FIXTURE_BUILDER` (default `desktop-linux`) builder name passed to `trigger deploy --builder`
- `DOCKER_WAIT_FIXTURE_LOAD_LOCAL=1|0` (default `1`) use `--load --no-push` instead of pushing to the registry
- `DOCKER_WAIT_PROJECT_REF` (optional override; auto-discovered from `DOCKER_WAIT_API_KEY` when possible)
- `DOCKER_WAIT_SECONDS` (default `10`)
- `DOCKER_WAIT_RUN_TIMEOUT_MS` (default `180000`)
- `DOCKER_WAIT_EVENT_TIMEOUT_MS` (default `60000`)
- `DOCKER_WAIT_MIN_WAITING_MS` (default `1000`)
- `DOCKER_WAIT_ASSERT_RUNNER_CONTAINER=0` to disable runner container evidence assertion
- `DOCKER_WAIT_RUNNER_EVIDENCE_TIMEOUT_MS` (default `120000`)

Optional warm-start assertion:

- `DOCKER_WAIT_ASSERT_WARM_START=1`
- `DOCKER_WAIT_SUPERVISOR_CONTAINER` (required if warm-start assertion is enabled)
- `DOCKER_WAIT_WARM_START_LOG_SINCE` (default `30m`)
- `DOCKER_WAIT_WARM_START_EVIDENCE_TIMEOUT_MS` (default `120000`)

Test levels:

- `DOCKER_TEST_LEVEL=all|smoke|full` (default `all`)
- `smoke`: quick stack health checks
- `full`: broader integration scenarios (waitpoint flow, recovery/fault checks)

## Notes

- These tests are opt-in and not part of the standard `pnpm test` flow.
- Stack health checks always run and fail fast if webapp/supervisor are unavailable.
- You can run only smoke checks with `DOCKER_TEST_LEVEL=smoke pnpm test:docker`.
- Default behavior is one command up/down lifecycle for local integration execution.
- On failure, diagnostics include `docker ps`, `docker ps -a`, and compose logs.
