import { setTimeout as sleep } from "timers/promises";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { K8sApi, createK8sApi } from "../clients/kubernetes.js";

export const DEFAULT_TEST_NAMESPACE = process.env.K8S_TEST_NAMESPACE ?? "trigger-k8s-integration";
const DEFAULT_ARTIFACTS_DIR =
  process.env.K8S_TEST_ARTIFACTS_DIR ?? join(process.cwd(), "k8s-test-artifacts");
const DEFAULT_TIMEOUT_MS = 60_000;
const POLL_INTERVAL_MS = 1_000;

export function createHarness() {
  const k8s = createK8sApi();

  return {
    k8s,
    namespace: DEFAULT_TEST_NAMESPACE,
  };
}

export async function ensureNamespace(k8s: K8sApi, namespace: string) {
  try {
    await k8s.core.readNamespace({ name: namespace });
  } catch {
    await k8s.core.createNamespace({
      body: {
        metadata: {
          name: namespace,
        },
      },
    });
  }

  await ensureDefaultServiceAccount(k8s, namespace);
}

async function ensureDefaultServiceAccount(k8s: K8sApi, namespace: string): Promise<void> {
  const timeoutMs = 30_000;
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    try {
      await k8s.core.readNamespacedServiceAccount({
        namespace,
        name: "default",
      });
      return;
    } catch {
      try {
        await k8s.core.createNamespacedServiceAccount({
          namespace,
          body: {
            metadata: {
              name: "default",
              namespace,
            },
          },
        });
        return;
      } catch {
        // Service account may be created concurrently; keep polling.
      }
    }

    await sleep(500);
  }

  throw new Error(
    `Timed out waiting for default service account in namespace ${namespace} after ${timeoutMs}ms`
  );
}

export async function cleanupPodsByLabel(
  k8s: K8sApi,
  namespace: string,
  labelSelector: string
): Promise<void> {
  await k8s.core.deleteCollectionNamespacedPod({
    namespace,
    labelSelector,
    gracePeriodSeconds: 0,
  });
}

export async function waitForPodPhase(params: {
  k8s: K8sApi;
  namespace: string;
  podName: string;
  phase: "Pending" | "Running" | "Succeeded" | "Failed" | "Unknown";
  timeoutMs?: number;
}) {
  const timeoutMs = params.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    try {
      const pod = await params.k8s.core.readNamespacedPod({
        namespace: params.namespace,
        name: params.podName,
      });

      if (pod.status?.phase === params.phase) {
        return;
      }
    } catch {
      // Keep polling while pod is starting/deleting.
    }

    await sleep(POLL_INTERVAL_MS);
  }

  throw new Error(
    `Timed out waiting for pod ${params.podName} to reach phase ${params.phase} after ${timeoutMs}ms`
  );
}

export async function waitForPodDeletion(params: {
  k8s: K8sApi;
  namespace: string;
  podName: string;
  timeoutMs?: number;
}) {
  const timeoutMs = params.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    try {
      await params.k8s.core.readNamespacedPod({
        namespace: params.namespace,
        name: params.podName,
      });
    } catch {
      return;
    }

    await sleep(POLL_INTERVAL_MS);
  }

  throw new Error(`Timed out waiting for pod ${params.podName} deletion after ${timeoutMs}ms`);
}

export async function podLogs(params: {
  k8s: K8sApi;
  namespace: string;
  podName: string;
  container?: string;
}): Promise<string> {
  return params.k8s.core.readNamespacedPodLog({
    namespace: params.namespace,
    name: params.podName,
    container: params.container,
  });
}

export async function withDiagnostics<T>(
  params: {
    k8s: K8sApi;
    namespace: string;
    labelSelector: string;
    testName: string;
  },
  fn: () => Promise<T>
): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    const artifactDir = await collectDiagnostics({
      k8s: params.k8s,
      namespace: params.namespace,
      labelSelector: params.labelSelector,
      testName: params.testName,
    });

    // Keep this as console.error so artifact location is obvious in test output.
    console.error(`k8s diagnostics captured at ${artifactDir}`);
    throw error;
  }
}

async function collectDiagnostics(params: {
  k8s: K8sApi;
  namespace: string;
  labelSelector: string;
  testName: string;
}): Promise<string> {
  const timestamp = new Date().toISOString().replaceAll(":", "-");
  const artifactDir = join(DEFAULT_ARTIFACTS_DIR, `${sanitizeFileName(params.testName)}-${timestamp}`);
  await mkdir(artifactDir, { recursive: true });

  try {
    const namespaceObj = await params.k8s.core.readNamespace({ name: params.namespace });
    await writeJson(join(artifactDir, "namespace.json"), namespaceObj);
  } catch (error) {
    await writeText(join(artifactDir, "namespace-error.txt"), stringifyError(error));
  }

  let pods: Awaited<ReturnType<K8sApi["core"]["listNamespacedPod"]>>["items"] = [];
  try {
    const podList = await params.k8s.core.listNamespacedPod({
      namespace: params.namespace,
      labelSelector: params.labelSelector,
    });
    pods = podList.items;
    await writeJson(join(artifactDir, "pods.json"), podList);
  } catch (error) {
    await writeText(join(artifactDir, "pods-error.txt"), stringifyError(error));
  }

  try {
    const events = await params.k8s.core.listNamespacedEvent({
      namespace: params.namespace,
    });
    await writeJson(join(artifactDir, "events.json"), events);
  } catch (error) {
    await writeText(join(artifactDir, "events-error.txt"), stringifyError(error));
  }

  for (const pod of pods) {
    const podName = pod.metadata?.name;
    if (!podName) {
      continue;
    }

    const containers = pod.spec?.containers ?? [];
    for (const container of containers) {
      try {
        const logs = await podLogs({
          k8s: params.k8s,
          namespace: params.namespace,
          podName,
          container: container.name,
        });
        await writeText(join(artifactDir, `${podName}-${container.name}.log`), logs);
      } catch (error) {
        await writeText(
          join(artifactDir, `${podName}-${container.name}.log.error.txt`),
          stringifyError(error)
        );
      }
    }
  }

  return artifactDir;
}

async function writeJson(path: string, value: unknown): Promise<void> {
  await writeFile(path, JSON.stringify(value, null, 2), "utf8");
}

async function writeText(path: string, value: string): Promise<void> {
  await writeFile(path, value, "utf8");
}

function sanitizeFileName(input: string): string {
  return input.toLowerCase().replace(/[^a-z0-9]+/g, "-");
}

function stringifyError(error: unknown): string {
  if (error instanceof Error) {
    return `${error.name}: ${error.message}\n${error.stack ?? ""}`;
  }

  return String(error);
}
