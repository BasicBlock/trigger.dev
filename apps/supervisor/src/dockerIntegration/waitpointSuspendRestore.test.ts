import { setTimeout as sleep } from "timers/promises";
import { describe, expect, it } from "vitest";
import { resolveWaitpointConfig, shell, shellWithStderr, withDiagnostics } from "./harness.js";
import { isFullEnabled } from "./testMode.js";

const enabled = process.env.DOCKER_WAIT_TEST_ENABLED === "1";

const assertRunnerContainer = process.env.DOCKER_WAIT_ASSERT_RUNNER_CONTAINER !== "0";
const assertWarmStart = process.env.DOCKER_WAIT_ASSERT_WARM_START === "1";
const supervisorContainer = process.env.DOCKER_WAIT_SUPERVISOR_CONTAINER;

const minWaitingMs = Number(process.env.DOCKER_WAIT_MIN_WAITING_MS ?? "1000");
const waitSeconds = Number(process.env.DOCKER_WAIT_SECONDS ?? "10");
const pollIntervalMs = Number(process.env.DOCKER_WAIT_POLL_INTERVAL_MS ?? "1000");
const runTimeoutMs = Number(process.env.DOCKER_WAIT_RUN_TIMEOUT_MS ?? "180000");
const eventTimeoutMs = Number(process.env.DOCKER_WAIT_EVENT_TIMEOUT_MS ?? "60000");
const runnerEvidenceTimeoutMs = Number(process.env.DOCKER_WAIT_RUNNER_EVIDENCE_TIMEOUT_MS ?? "120000");
const warmStartLogSince = process.env.DOCKER_WAIT_WARM_START_LOG_SINCE ?? "30m";
const warmStartEvidenceTimeoutMs = Number(
  process.env.DOCKER_WAIT_WARM_START_EVIDENCE_TIMEOUT_MS ?? "120000"
);
const queuedTimeoutMs = Number(process.env.DOCKER_WAIT_QUEUED_TIMEOUT_MS ?? "30000");

const terminalStatuses = new Set([
  "COMPLETED",
  "FAILED",
  "CANCELED",
  "INTERRUPTED",
  "CRASHED",
  "SYSTEM_FAILURE",
  "TIMED_OUT",
  "EXPIRED",
]);

const suite = enabled ? describe : describe;
const itFull = isFullEnabled() ? it : it;

suite("docker waitpoint suspend/restore integration", () => {
  itFull(
    "completes a short wait run and emits markers across webapp/supervisor/runner",
    async () => {
      const { apiUrl, apiKey, taskId } = await resolveWaitpointConfig();

      await withDiagnostics(
        {
          testName: "waitpoint suspend restore integration",
        },
        async () => {
          const runId = await triggerWaitTask({
            apiUrl,
            apiKey,
            taskId,
            waitSeconds,
          });

          const runnerEvidencePromise = assertRunnerContainer
            ? waitForRunnerContainerEvidence({
                runId,
                timeoutMs: runnerEvidenceTimeoutMs,
                pollIntervalMs,
              })
            : Promise.resolve(true);

          const runPoll = await waitForRunCompletion({
            apiUrl,
            apiKey,
            runId,
            timeoutMs: runTimeoutMs,
            pollIntervalMs,
            queuedTimeoutMs,
          });

          expect(runPoll.finalStatus).toBe("COMPLETED");
          expect(runPoll.statusTransitions).toContain("WAITING");
          expect(runPoll.waitingDurationMs).toBeGreaterThanOrEqual(minWaitingMs);

          const runnerSeen = await runnerEvidencePromise;
          if (assertRunnerContainer) {
            expect(runnerSeen).toBe(true);
          }

          if (assertWarmStart) {
            if (!supervisorContainer) {
              throw new Error(
                "DOCKER_WAIT_ASSERT_WARM_START=1 requires DOCKER_WAIT_SUPERVISOR_CONTAINER to be set"
              );
            }

            const warmStartEvidence = await waitForWarmStartEvidence({
              runId,
              supervisorContainer,
              timeoutMs: warmStartEvidenceTimeoutMs,
              pollIntervalMs,
              since: warmStartLogSince,
            });

            expect(warmStartEvidence.suspendSeen).toBe(true);
            expect(warmStartEvidence.restoreSeen).toBe(true);
          }

          const markers = await waitForRunMarkers({
            apiUrl,
            apiKey,
            runId,
            timeoutMs: eventTimeoutMs,
            pollIntervalMs,
          });

          expect(markers.startSeen).toBe(true);
          expect(markers.completeSeen).toBe(true);
        }
      );
    },
    runTimeoutMs + eventTimeoutMs + 60_000
  );
});

async function triggerWaitTask(params: {
  apiUrl: string;
  apiKey: string;
  taskId: string;
  waitSeconds: number;
}): Promise<string> {
  let response: Response;
  try {
    response = await fetch(`${params.apiUrl}/api/v1/tasks/${params.taskId}/trigger`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${params.apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        payload: {
          waitTimeSec: params.waitSeconds,
        },
        context: {},
      }),
    });
  } catch (error) {
    throw new Error(`Unable to reach webapp API at ${params.apiUrl}: ${String(error)}`);
  }

  if (!response.ok) {
    throw new Error(`Failed to trigger task (${response.status}): ${await safeResponseText(response)}`);
  }

  const body = (await response.json()) as { id?: string };
  if (!body.id) {
    throw new Error(`Trigger response did not include run id: ${JSON.stringify(body)}`);
  }

  return body.id;
}

async function waitForRunCompletion(params: {
  apiUrl: string;
  apiKey: string;
  runId: string;
  timeoutMs: number;
  pollIntervalMs: number;
  queuedTimeoutMs: number;
}): Promise<{ finalStatus: string; statusTransitions: string[]; waitingDurationMs: number }> {
  const deadline = Date.now() + params.timeoutMs;
  const statusTransitions: string[] = [];
  let finalStatus = "UNKNOWN";
  let waitingStartedAt: number | undefined;
  let waitingDurationMs = 0;
  let firstQueuedAt: number | undefined;
  let sawExecutionEvidence = false;

  while (Date.now() < deadline) {
    const now = Date.now();
    const run = await getRun(params.apiUrl, params.apiKey, params.runId);
    const currentStatus = String(run.status ?? "UNKNOWN");
    const attemptCount = Number(run.attemptCount ?? run.attemptNumber ?? 0);

    if (statusTransitions.at(-1) !== currentStatus) {
      statusTransitions.push(currentStatus);
    }

    if (currentStatus === "WAITING" && waitingStartedAt === undefined) {
      waitingStartedAt = now;
    }

    if (currentStatus === "QUEUED" && firstQueuedAt === undefined) {
      firstQueuedAt = now;
    } else if (currentStatus !== "QUEUED") {
      firstQueuedAt = undefined;
    }

    if (currentStatus !== "QUEUED" || attemptCount > 0) {
      sawExecutionEvidence = true;
    }

    if (
      !sawExecutionEvidence &&
      firstQueuedAt !== undefined &&
      now - firstQueuedAt >= params.queuedTimeoutMs
    ) {
      throw new Error(
        `Run ${params.runId} stayed QUEUED for ${params.queuedTimeoutMs}ms without execution evidence (attemptCount=${attemptCount}). This usually means no worker could execute it (e.g. runner image unavailable or supervisor/worker misconfiguration). Status transitions: ${statusTransitions.join(
          " -> "
        )}`
      );
    }

    if (currentStatus !== "WAITING" && waitingStartedAt !== undefined) {
      waitingDurationMs += now - waitingStartedAt;
      waitingStartedAt = undefined;
    }

    if (terminalStatuses.has(currentStatus)) {
      finalStatus = currentStatus;
      if (waitingStartedAt !== undefined) {
        waitingDurationMs += now - waitingStartedAt;
      }
      return { finalStatus, statusTransitions, waitingDurationMs };
    }

    await sleep(params.pollIntervalMs);
  }

  throw new Error(
    `Timed out waiting for run ${params.runId} completion. Status transitions: ${statusTransitions.join(
      " -> "
    )}`
  );
}

async function waitForRunMarkers(params: {
  apiUrl: string;
  apiKey: string;
  runId: string;
  timeoutMs: number;
  pollIntervalMs: number;
}): Promise<{ startSeen: boolean; completeSeen: boolean }> {
  const deadline = Date.now() + params.timeoutMs;

  while (Date.now() < deadline) {
    const eventsResponse = await fetch(`${params.apiUrl}/api/v1/runs/${params.runId}/events`, {
      headers: {
        Authorization: `Bearer ${params.apiKey}`,
      },
    });

    if (!eventsResponse.ok) {
      throw new Error(
        `Failed to fetch run events (${eventsResponse.status}): ${await safeResponseText(eventsResponse)}`
      );
    }

    const eventsBody = (await eventsResponse.json()) as { events?: unknown[] };
    const eventText = JSON.stringify(eventsBody.events ?? []);

    const startSeen = eventText.includes("WAIT_START_MARKER");
    const completeSeen = eventText.includes("WAIT_COMPLETE_MARKER");

    if (startSeen && completeSeen) {
      return { startSeen, completeSeen };
    }

    await sleep(params.pollIntervalMs);
  }

  return { startSeen: false, completeSeen: false };
}

async function waitForRunnerContainerEvidence(params: {
  runId: string;
  timeoutMs: number;
  pollIntervalMs: number;
}): Promise<boolean> {
  const deadline = Date.now() + params.timeoutMs;
  const runSuffix = params.runId.replace(/^run_/, "");

  while (Date.now() < deadline) {
    const containerNames = await shell("docker ps -a --format '{{.Names}}'");
    const lines = containerNames
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);

    if (
      lines.some(
        (name) =>
          name === `runner-${runSuffix}` ||
          name.startsWith(`runner-${runSuffix}-attempt-`) ||
          name.includes(`runner-${runSuffix}`)
      )
    ) {
      return true;
    }

    await sleep(params.pollIntervalMs);
  }

  return false;
}

async function waitForWarmStartEvidence(params: {
  runId: string;
  supervisorContainer: string;
  timeoutMs: number;
  pollIntervalMs: number;
  since: string;
}): Promise<{ suspendSeen: boolean; restoreSeen: boolean }> {
  const deadline = Date.now() + params.timeoutMs;

  while (Date.now() < deadline) {
    const logs = await shellWithStderr(
      `docker logs --since ${params.since} ${params.supervisorContainer} 2>&1`
    );

    const suspendSeen = logs.includes("[suspend]") && logs.includes(`run ${params.runId}`);
    const restoreSeen = logs.includes("[restore]") && logs.includes(`run ${params.runId}`);

    if (suspendSeen && restoreSeen) {
      return { suspendSeen, restoreSeen };
    }

    await sleep(params.pollIntervalMs);
  }

  return { suspendSeen: false, restoreSeen: false };
}

async function getRun(apiUrl: string, apiKey: string, runId: string): Promise<any> {
  const response = await fetch(`${apiUrl}/api/v3/runs/${runId}`, {
    headers: {
      Authorization: `Bearer ${apiKey}`,
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch run ${runId} (${response.status}): ${await safeResponseText(response)}`);
  }

  return await response.json();
}

async function safeResponseText(response: Response): Promise<string> {
  try {
    return await response.text();
  } catch {
    return "<failed to read response body>";
  }
}
