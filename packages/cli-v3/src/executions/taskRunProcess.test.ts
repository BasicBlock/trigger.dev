import { TaskRunProcess, type TaskRunProcessOptions } from "./taskRunProcess.js";
import { describe, it, expect, vi } from "vitest";
import { UnexpectedExitError } from "@basicblock/trigger-core/v3/errors";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import type {
  TaskRunExecution,
  TaskRunExecutionPayload,
  WorkerManifest,
  ServerBackgroundWorker,
  MachinePresetResources,
} from "@basicblock/trigger-core/v3";

function createTaskRunProcessOptions(
  overrides: Partial<TaskRunProcessOptions> = {}
): TaskRunProcessOptions {
  return {
    workerManifest: {
      runtime: "node",
      workerEntryPoint: "/dev/null",
      configEntryPoint: "/dev/null",
      otelImportHook: {},
    } as unknown as WorkerManifest,
    serverWorker: {
      id: "worker-1",
      version: "20260217.1",
      contentHash: "worker-hash-1",
    } as unknown as ServerBackgroundWorker,
    env: {},
    machineResources: { cpu: 1, memory: 1 } as MachinePresetResources,
    ...overrides,
  };
}

function createExecution(runId: string, attemptNumber: number): TaskRunExecution {
  return {
    run: {
      id: runId,
      payload: "{}",
      payloadType: "application/json",
      tags: [],
      isTest: false,
      createdAt: new Date(),
      startedAt: new Date(),
      maxAttempts: 3,
      version: "1",
      durationMs: 0,
      costInCents: 0,
      baseCostInCents: 0,
    },
    metadata: {
      id: "metadata-1",
      version: "20260217.1",
      contentHash: "hash-1",
    },
    attempt: {
      number: attemptNumber,
      startedAt: new Date(),
      id: "deprecated",
      backgroundWorkerId: "deprecated",
      backgroundWorkerTaskId: "deprecated",
      status: "deprecated" as any,
    },
    task: { id: "test-task", filePath: "test.ts" },
    queue: { id: "queue-1", name: "test-queue" },
    environment: { id: "env-1", slug: "test", type: "DEVELOPMENT" },
    organization: { id: "org-1", slug: "test-org", name: "Test Org" },
    project: { id: "proj-1", ref: "proj_test", slug: "test", name: "Test" },
    machine: { name: "small-1x", cpu: 0.5, memory: 0.5, centsPerMs: 0 },
  } as unknown as TaskRunExecution;
}

async function createSocketWorkerEntryPoint() {
  const dir = await mkdtemp(path.join(os.tmpdir(), "trigger-socket-worker-"));
  const workerPath = path.join(dir, "worker.mjs");

  const source = `
import net from "node:net";

const socketPath = process.env.TRIGGER_IPC_SOCKET_PATH;
if (!socketPath) {
  throw new Error("TRIGGER_IPC_SOCKET_PATH is required");
}

let buffer = "";
const socket = net.createConnection(socketPath);
socket.setEncoding("utf8");

function sendPacket(packet) {
  socket.write(JSON.stringify(packet) + "\\n");
}

function handlePacket(packet) {
  if (!packet || packet.type !== "EVENT" || !packet.message) {
    return;
  }

  if (packet.message.type === "EXECUTE_TASK_RUN") {
    const execution = packet.message.payload.execution;

    sendPacket({
      type: "EVENT",
      message: {
        version: "v1",
        type: "TASK_RUN_COMPLETED",
        payload: {
          execution,
          result: {
            ok: true,
            id: execution.run.id,
            output: "{\\"ok\\":true}",
            outputType: "application/json",
            usage: {
              durationMs: 1
            }
          }
        }
      }
    });
  } else if (typeof packet.id === "number") {
    sendPacket({
      type: "ACK",
      id: packet.id,
      message: {
        status: "ok"
      }
    });
  }
}

socket.on("data", (chunk) => {
  buffer += chunk;
  let index = buffer.indexOf("\\n");

  while (index >= 0) {
    const line = buffer.slice(0, index).trim();
    buffer = buffer.slice(index + 1);

    if (line.length > 0) {
      try {
        handlePacket(JSON.parse(line));
      } catch {}
    }

    index = buffer.indexOf("\\n");
  }
});
`;

  await writeFile(workerPath, source, "utf8");

  return {
    workerPath,
    cleanup: () => rm(dir, { recursive: true, force: true }),
  };
}

describe("TaskRunProcess", () => {
  describe("execute() on a dead child process", () => {
    it("should reject when child process has already exited and IPC send is skipped", async () => {
      const proc = new TaskRunProcess(createTaskRunProcessOptions());

      // Simulate a child process that has exited: _child exists but is not connected
      const fakeChild = {
        connected: false,
        killed: false,
        pid: 12345,
        kill: vi.fn(),
        on: vi.fn(),
        stdout: { on: vi.fn() },
        stderr: { on: vi.fn() },
      };

      // Set internal state to mimic a process whose child has crashed
      (proc as any)._child = fakeChild;
      (proc as any)._childPid = 12345;
      (proc as any)._isBeingKilled = false;

      const execution = createExecution("run-1", 2);

      // This should NOT hang forever - it should reject promptly.
      //
      // BUG: Currently execute() creates a promise, skips the IPC send because
      // _child.connected is false, then awaits the promise which will never
      // resolve because the child is dead and #handleExit already ran.
      //
      // The Promise.race with a timeout detects the hang.
      const result = await Promise.race([
        proc
          .execute(
            {
              payload: { execution, traceContext: {}, metrics: [] },
              messageId: "run_run-1",
              env: {},
            },
            true
          )
          .then(
            (v) => ({ type: "resolved" as const, value: v }),
            (e) => ({ type: "rejected" as const, error: e })
          ),
        new Promise<{ type: "hung" }>((resolve) =>
          setTimeout(() => resolve({ type: "hung" as const }), 2000)
        ),
      ]);

      // The test fails (proving the bug) if execute() hangs
      expect(result.type).not.toBe("hung");
      expect(result.type).toBe("rejected");

      if (result.type === "rejected") {
        expect(result.error).toBeInstanceOf(UnexpectedExitError);
        expect(result.error.stderr).toContain("not connected");
      }
    });
  });

  describe("socket IPC transport", () => {
    it("should execute successfully when TRIGGER_IPC_TRANSPORT=socket", async () => {
      const fixture = await createSocketWorkerEntryPoint();

      const proc = new TaskRunProcess(
        createTaskRunProcessOptions({
          workerManifest: {
            runtime: "node",
            workerEntryPoint: fixture.workerPath,
            configEntryPoint: fixture.workerPath,
            otelImportHook: {},
          } as unknown as WorkerManifest,
          env: {
            TRIGGER_IPC_TRANSPORT: "socket",
          },
        })
      ).initialize();

      const execution = createExecution("run-socket-1", 1);
      const payload: TaskRunExecutionPayload = {
        execution,
        traceContext: {},
        metrics: [],
      } as TaskRunExecutionPayload;

      try {
        const result = await proc.execute({
          payload,
          messageId: "run_run-socket-1",
          env: {},
        });

        expect(result.ok).toBe(true);
        if (result.ok) {
          expect(result.id).toBe("run-socket-1");
          expect(result.outputType).toBe("application/json");
        }
      } finally {
        await proc.kill("SIGKILL", 2_000);
        await fixture.cleanup();
      }
    });
  });
});
