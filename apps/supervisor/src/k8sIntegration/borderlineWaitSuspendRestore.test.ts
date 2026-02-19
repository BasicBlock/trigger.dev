import { describe, expect, it } from "vitest";
import { setTimeout as sleep } from "timers/promises";
import { createK8sApi } from "../clients/kubernetes.js";

const enabled = process.env.K8S_WAIT_TEST_ENABLED === "1";
const apiUrl = process.env.K8S_WAIT_API_URL;
const apiKey = process.env.K8S_WAIT_API_KEY;
const taskId = process.env.K8S_WAIT_TASK_ID;
const assertWarmStart = process.env.K8S_WAIT_ASSERT_WARM_START === "1";
const minWaitingMs = Number(process.env.K8S_WAIT_MIN_WAITING_MS ?? "1000");

const warmStartNamespace = process.env.K8S_WAIT_WARM_START_NAMESPACE ?? "trigger";
const warmStartLabelSelector = process.env.K8S_WAIT_WARM_START_LABEL_SELECTOR;
const warmStartLogSinceSeconds = Number(process.env.K8S_WAIT_WARM_START_LOG_SINCE_SECONDS ?? "1800");
const warmStartEvidenceTimeoutMs = Number(
  process.env.K8S_WAIT_WARM_START_EVIDENCE_TIMEOUT_MS ?? "120000"
);

const waitSeconds = Number(process.env.K8S_WAIT_SECONDS ?? "10");
const pollIntervalMs = Number(process.env.K8S_WAIT_POLL_INTERVAL_MS ?? "1000");
const runTimeoutMs = Number(process.env.K8S_WAIT_RUN_TIMEOUT_MS ?? "180000");
const eventTimeoutMs = Number(process.env.K8S_WAIT_EVENT_TIMEOUT_MS ?? "60000");

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

const suite = enabled ? describe : describe.skip;

suite("borderline waitpoint suspend/restore integration", () => {
  it(
    "completes a 10s waitpoint run and emits wait markers",
    async () => {
      if (!apiUrl || !apiKey || !taskId) {
        throw new Error(
          "Missing required env vars. Expected K8S_WAIT_API_URL, K8S_WAIT_API_KEY, K8S_WAIT_TASK_ID."
        );
      }

      const runId = await triggerWaitTask({
        apiUrl,
        apiKey,
        taskId,
        waitSeconds,
      });

      const runPoll = await waitForRunCompletion({
        apiUrl,
        apiKey,
        runId,
        timeoutMs: runTimeoutMs,
        pollIntervalMs,
      });

      expect(runPoll.finalStatus).toBe("COMPLETED");
      expect(runPoll.statusTransitions).toContain("WAITING");
      expect(runPoll.waitingDurationMs).toBeGreaterThanOrEqual(minWaitingMs);

      if (assertWarmStart) {
        const warmStartEvidence = await waitForWarmStartEvidence({
          runId,
          timeoutMs: warmStartEvidenceTimeoutMs,
          pollIntervalMs,
          namespace: warmStartNamespace,
          labelSelector: warmStartLabelSelector,
          sinceSeconds: warmStartLogSinceSeconds,
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
    },
    runTimeoutMs + eventTimeoutMs + 30_000
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
    throw new Error(
      `Unable to reach webapp API at ${params.apiUrl}. Ensure the stack is running (default: docker via scripts/k8s-integration/run.sh, or K8S_STACK_MODE=helm). Original error: ${String(
        error
      )}`
    );
  }

  if (!response.ok) {
    throw new Error(
      `Failed to trigger task (${response.status}): ${await safeResponseText(response)}`
    );
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
}): Promise<{ finalStatus: string; statusTransitions: string[]; waitingDurationMs: number }> {
  const deadline = Date.now() + params.timeoutMs;
  const statusTransitions: string[] = [];
  let finalStatus = "UNKNOWN";
  let waitingStartedAt: number | undefined;
  let waitingDurationMs = 0;

  while (Date.now() < deadline) {
    const now = Date.now();
    const run = await getRun(params.apiUrl, params.apiKey, params.runId);
    const currentStatus = String(run.status ?? "UNKNOWN");

    if (statusTransitions.at(-1) !== currentStatus) {
      statusTransitions.push(currentStatus);
    }

    if (currentStatus === "WAITING" && waitingStartedAt === undefined) {
      waitingStartedAt = now;
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

async function waitForWarmStartEvidence(params: {
  runId: string;
  timeoutMs: number;
  pollIntervalMs: number;
  namespace: string;
  labelSelector?: string;
  sinceSeconds: number;
}): Promise<{ suspendSeen: boolean; restoreSeen: boolean }> {
  const deadline = Date.now() + params.timeoutMs;
  const k8s = createK8sApi();

  while (Date.now() < deadline) {
    const logs = await readWarmStartLogs({
      k8s,
      namespace: params.namespace,
      labelSelector: params.labelSelector,
      sinceSeconds: params.sinceSeconds,
    });

    const suspendSeen = logs.includes("[suspend]") && logs.includes(`run ${params.runId}`);
    const restoreSeen = logs.includes("[restore]") && logs.includes(`run ${params.runId}`);

    if (suspendSeen && restoreSeen) {
      return { suspendSeen, restoreSeen };
    }

    await sleep(params.pollIntervalMs);
  }

  return { suspendSeen: false, restoreSeen: false };
}

async function readWarmStartLogs(params: {
  k8s: ReturnType<typeof createK8sApi>;
  namespace: string;
  labelSelector?: string;
  sinceSeconds: number;
}): Promise<string> {
  const podList = await params.k8s.core.listNamespacedPod({
    namespace: params.namespace,
    labelSelector: params.labelSelector,
  });

  const chunks: string[] = [];
  for (const pod of podList.items) {
    const podName = pod.metadata?.name;
    if (!podName) {
      continue;
    }

    for (const container of pod.spec?.containers ?? []) {
      try {
        const logs = await params.k8s.core.readNamespacedPodLog({
          namespace: params.namespace,
          name: podName,
          container: container.name,
          sinceSeconds: params.sinceSeconds,
        });
        chunks.push(logs);
      } catch {
        // Keep polling other containers and pods.
      }
    }
  }

  return chunks.join("\n");
}

async function safeResponseText(response: Response): Promise<string> {
  try {
    return await response.text();
  } catch {
    return "<failed to read response body>";
  }
}
