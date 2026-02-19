# Supervisor Kubernetes Integration Tests

These tests run real workload pods against your active kube context.

## Run

From repository root:

```bash
pnpm test:k8s
```

This command will:

1. Ensure a `kind` cluster exists (`trigger-test` by default)
2. Ensure a test namespace exists (`trigger-k8s-integration` by default)
3. Start Trigger stack for API/webapp (default: Docker dev stack)
4. Auto-wire waitpoint test API URL and auto-discover API key/task ID when possible
5. Run `apps/supervisor` k8s integration tests

## Environment

- `K8S_TEST_CLUSTER`: kind cluster name (default `trigger-test`)
- `K8S_TEST_NAMESPACE`: test namespace (default `trigger-k8s-integration`)
- `K8S_TEST_ARTIFACTS_DIR`: diagnostics output directory on failure (default `apps/supervisor/k8s-test-artifacts`)

Borderline waitpoint suspend/restore test (opt-in):

- `K8S_WAIT_TEST_ENABLED=auto` (default; runs only when all prerequisites are discoverable)
- `K8S_WAIT_TEST_ENABLED=1` (strict mode; fail fast if any prerequisite is missing)
- `K8S_WAIT_API_URL` (example `http://localhost:3030`)
- `K8S_WAIT_API_KEY` (environment API key with trigger/read scopes)
- `K8S_WAIT_TASK_ID` (example `test.wait`)
- `K8S_WAIT_SECONDS` (default `10`)
- `K8S_WAIT_RUN_TIMEOUT_MS` (default `180000`)
- `K8S_WAIT_EVENT_TIMEOUT_MS` (default `60000`)
- `K8S_WAIT_MIN_WAITING_MS` (default `1000`)
- `K8S_WAIT_ASSERT_WARM_START=1` to require suspend+restore evidence from warm-start logs
- `K8S_WAIT_WARM_START_NAMESPACE` (default `trigger`)
- `K8S_WAIT_WARM_START_LABEL_SELECTOR` (optional, recommended)
- `K8S_WAIT_WARM_START_LOG_SINCE_SECONDS` (default `1800`)
- `K8S_WAIT_WARM_START_EVIDENCE_TIMEOUT_MS` (default `120000`)
- `K8S_WAIT_API_URL` can be omitted for default Docker mode (`http://127.0.0.1:3030`) and for Helm mode (port-forward is started automatically)

Optional webapp delay knobs (to emulate slow snapshot/checkpoint completion):

- `CHECKPOINT_TEST_DELAY_MS` (default `0`)
- `CHECKPOINT_TEST_DELAY_JITTER_MS` (default `0`)

Kind stack orchestration via `scripts/k8s-integration/run.sh`:

- `K8S_STACK_MODE=docker|helm|none` (default `docker`)
- `K8S_INTEGRATION_DEPLOY_HELM=1` is still supported as a compatibility switch to force `K8S_STACK_MODE=helm`
- `K8S_STACK_RELEASE_NAME` (default `trigger-k8s-int`)
- `K8S_STACK_NAMESPACE` (default `trigger`)
- `K8S_STACK_WEBAPP_SERVICE` (default `${K8S_STACK_RELEASE_NAME}-webapp`)
- `K8S_WAIT_API_PORT` local port for webapp port-forward (default `3030`)

## Notes

- These tests are opt-in and not part of the standard `pnpm test` flow.
- Initial scenarios cover workload pod lifecycle and replacement behavior.
- On failure, the harness writes namespace diagnostics (`pods`, `events`, and container logs) to the artifacts directory.
- The borderline waitpoint test is skipped in `auto` mode when API URL / API key / task ID cannot be discovered.
- When `K8S_WAIT_ASSERT_WARM_START=1`, the test fails unless both `[suspend]` and `[restore]` log evidence appears for the exact run ID.
