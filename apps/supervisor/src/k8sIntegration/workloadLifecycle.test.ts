import { randomUUID } from "node:crypto";
import { beforeAll, afterEach, describe, expect, it } from "vitest";
import {
  cleanupPodsByLabel,
  createHarness,
  ensureNamespace,
  podLogs,
  waitForPodDeletion,
  waitForPodPhase,
  withDiagnostics,
} from "./harness.js";

const TEST_LABEL_KEY = "trigger.dev/test-suite";
const TEST_LABEL_VALUE = "k8s-workload-lifecycle";
const TEST_LABEL_SELECTOR = `${TEST_LABEL_KEY}=${TEST_LABEL_VALUE}`;

describe("k8s workload lifecycle integration", () => {
  const { k8s, namespace } = createHarness();

  beforeAll(async () => {
    await ensureNamespace(k8s, namespace);
  });

  afterEach(async () => {
    await cleanupPodsByLabel(k8s, namespace, TEST_LABEL_SELECTOR);
  });

  it(
    "runs a real workload pod and exposes logs",
    async () => {
      await withDiagnostics(
        {
          k8s,
          namespace,
          labelSelector: TEST_LABEL_SELECTOR,
          testName: "runs a real workload pod and exposes logs",
        },
        async () => {
          const podName = `k8s-int-${randomUUID().slice(0, 8)}`;
          const marker = `trigger-k8s-log-marker-${Date.now()}`;

          await k8s.core.createNamespacedPod({
            namespace,
            body: {
              metadata: {
                name: podName,
                labels: {
                  app: "task-run",
                  [TEST_LABEL_KEY]: TEST_LABEL_VALUE,
                },
              },
              spec: {
                restartPolicy: "Never",
                containers: [
                  {
                    name: "runner",
                    image: "busybox:1.36",
                    command: ["/bin/sh", "-c", `echo ${marker}; sleep 1; echo done`],
                  },
                ],
              },
            },
          });

          await waitForPodPhase({
            k8s,
            namespace,
            podName,
            phase: "Succeeded",
            timeoutMs: 120_000,
          });

          const logs = await podLogs({ k8s, namespace, podName, container: "runner" });
          expect(logs).toContain(marker);
        }
      );
    },
    150_000
  );

  it(
    "replaces a running pod with the same name and runs replacement workload",
    async () => {
      await withDiagnostics(
        {
          k8s,
          namespace,
          labelSelector: TEST_LABEL_SELECTOR,
          testName: "replaces a running pod with the same name and runs replacement workload",
        },
        async () => {
          const podName = `k8s-int-replace-${randomUUID().slice(0, 8)}`;
          const restoreMarker = `trigger-k8s-restore-marker-${Date.now()}`;

          await k8s.core.createNamespacedPod({
            namespace,
            body: {
              metadata: {
                name: podName,
                labels: {
                  app: "task-run",
                  [TEST_LABEL_KEY]: TEST_LABEL_VALUE,
                },
              },
              spec: {
                restartPolicy: "Never",
                containers: [
                  {
                    name: "runner",
                    image: "busybox:1.36",
                    command: ["/bin/sh", "-c", "sleep 300"],
                  },
                ],
              },
            },
          });

          await waitForPodPhase({
            k8s,
            namespace,
            podName,
            phase: "Running",
            timeoutMs: 120_000,
          });

          await k8s.core.deleteNamespacedPod({
            namespace,
            name: podName,
            gracePeriodSeconds: 0,
          });

          await waitForPodDeletion({
            k8s,
            namespace,
            podName,
            timeoutMs: 120_000,
          });

          await k8s.core.createNamespacedPod({
            namespace,
            body: {
              metadata: {
                name: podName,
                labels: {
                  app: "task-run",
                  [TEST_LABEL_KEY]: TEST_LABEL_VALUE,
                },
              },
              spec: {
                restartPolicy: "Never",
                containers: [
                  {
                    name: "runner",
                    image: "busybox:1.36",
                    command: ["/bin/sh", "-c", `echo ${restoreMarker}; sleep 1; echo restored`],
                  },
                ],
              },
            },
          });

          await waitForPodPhase({
            k8s,
            namespace,
            podName,
            phase: "Succeeded",
            timeoutMs: 120_000,
          });

          const logs = await podLogs({ k8s, namespace, podName, container: "runner" });
          expect(logs).toContain(restoreMarker);
        }
      );
    },
    180_000
  );

  it(
    "completes a short wait workload without getting stuck",
    async () => {
      await withDiagnostics(
        {
          k8s,
          namespace,
          labelSelector: TEST_LABEL_SELECTOR,
          testName: "completes a short wait workload without getting stuck",
        },
        async () => {
          const podName = `k8s-int-shortwait-${randomUUID().slice(0, 8)}`;
          const marker = `trigger-k8s-short-wait-${Date.now()}`;

          await k8s.core.createNamespacedPod({
            namespace,
            body: {
              metadata: {
                name: podName,
                labels: {
                  app: "task-run",
                  [TEST_LABEL_KEY]: TEST_LABEL_VALUE,
                },
              },
              spec: {
                restartPolicy: "Never",
                containers: [
                  {
                    name: "runner",
                    image: "busybox:1.36",
                    command: ["/bin/sh", "-c", `echo begin; sleep 2; echo ${marker}`],
                  },
                ],
              },
            },
          });

          await waitForPodPhase({
            k8s,
            namespace,
            podName,
            phase: "Succeeded",
            timeoutMs: 120_000,
          });

          const logs = await podLogs({ k8s, namespace, podName, container: "runner" });
          expect(logs).toContain(marker);
        }
      );
    },
    150_000
  );
});
