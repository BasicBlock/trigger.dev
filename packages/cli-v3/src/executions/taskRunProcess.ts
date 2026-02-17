import {
  attemptKey,
  CompletedWaitpoint,
  ExecutorToWorkerMessageCatalog,
  MachinePresetResources,
  ServerBackgroundWorker,
  TaskRunErrorCodes,
  TaskRunExecution,
  TaskRunExecutionPayload,
  TaskRunExecutionResult,
  type TaskRunInternalError,
  tryCatch,
  WorkerManifest,
  WorkerToExecutorMessageCatalog,
} from "@basicblock/trigger-core/v3";
import {
  type WorkerToExecutorProcessConnection,
  ZodIpcConnection,
} from "@basicblock/trigger-core/v3/zodIpc";
import { Evt } from "evt";
import { ChildProcess, fork } from "node:child_process";
import { randomUUID } from "node:crypto";
import { chalkError, chalkGrey, chalkRun, prettyPrintDate } from "../utilities/cliOutput.js";

import { execOptionsForRuntime, execPathForRuntime } from "@basicblock/trigger-core/v3/build";
import { nodeOptionsWithMaxOldSpaceSize } from "@basicblock/trigger-core/v3/machines";
import { InferSocketMessageSchema } from "@basicblock/trigger-core/v3/zodSocket";
import { logger } from "../utilities/logger.js";
import {
  createParentSocketIpcProcess,
  type IpcProcessLike,
} from "./localSocketIpc.js";
import {
  CancelledProcessError,
  CleanupProcessError,
  internalErrorFromUnexpectedExit,
  GracefulExitTimeoutError,
  MaxDurationExceededError,
  UnexpectedExitError,
  SuspendedProcessError,
} from "@basicblock/trigger-core/v3/errors";

export type OnSendDebugLogMessage = InferSocketMessageSchema<
  typeof ExecutorToWorkerMessageCatalog,
  "SEND_DEBUG_LOG"
>;

export type OnSetSuspendableMessage = InferSocketMessageSchema<
  typeof ExecutorToWorkerMessageCatalog,
  "SET_SUSPENDABLE"
>;

export type TaskRunProcessOptions = {
  workerManifest: WorkerManifest;
  serverWorker: ServerBackgroundWorker;
  env: Record<string, string>;
  machineResources: MachinePresetResources;
  isWarmStart?: boolean;
  cwd?: string;
  gracefulTerminationTimeoutInMs?: number;
};

export type TaskRunProcessExecuteParams = {
  payload: TaskRunExecutionPayload;
  messageId: string;
  env?: Record<string, string>;
};

type EndIpcQuiesceResult = {
  ok: boolean;
  attempts: number;
  timeoutInMs: number;
  retryDelayMs: number;
  socketPath?: string;
  lastError?: string;
};

export class TaskRunProcess {
  private _ipc?: WorkerToExecutorProcessConnection;
  private _ipcProcess?: IpcProcessLike;
  private _child: ChildProcess | undefined;
  private _childPid?: number;
  private _attemptPromises: Map<
    string,
    { resolver: (value: TaskRunExecutionResult) => void; rejecter: (err?: any) => void }
  > = new Map();
  private _attemptStatuses: Map<string, "PENDING" | "REJECTED" | "RESOLVED"> = new Map();
  private _currentExecution: TaskRunExecution | undefined;
  private _gracefulExitTimeoutElapsed: boolean = false;
  private _isBeingKilled: boolean = false;
  private _isBeingCancelled: boolean = false;
  private _isBeingSuspended: boolean = false;
  private _isMaxDurationExceeded: boolean = false;
  private _maxDurationInfo?: { maxDurationInSeconds: number; elapsedTimeInSeconds: number };
  private _stderr: Array<string> = [];
  private _ipcPingSeq: number = 0;
  private _ipcPingSent: number = 0;
  private _ipcPingAcked: number = 0;
  private _ipcPingFailed: number = 0;
  private _isIpcQuiescing: boolean = false;
  private _inFlightAckSends: number = 0;
  private _ipcSocketPath?: string;

  public onTaskRunHeartbeat: Evt<string> = new Evt();
  public onExit: Evt<{ code: number | null; signal: NodeJS.Signals | null; pid?: number }> =
    new Evt();
  public onSendDebugLog: Evt<OnSendDebugLogMessage> = new Evt();
  public onSetSuspendable: Evt<OnSetSuspendableMessage> = new Evt();

  private _isPreparedForNextRun: boolean = false;
  private _isPreparedForNextAttempt: boolean = false;

  constructor(public readonly options: TaskRunProcessOptions) {
    this._isPreparedForNextRun = true;
    this._isPreparedForNextAttempt = true;
  }

  get isPreparedForNextRun() {
    return this._isPreparedForNextRun;
  }

  get isPreparedForNextAttempt() {
    return this._isPreparedForNextAttempt;
  }

  private logRestoreTimeline(stage: string, properties?: Record<string, unknown>) {
    logger.debug("restore_timeline", {
      stage,
      runId: this.options.env.TRIGGER_RUN_ID,
      runnerId: this.options.env.TRIGGER_RUNNER_ID,
      snapshotId: this.options.env.TRIGGER_SNAPSHOT_ID,
      ...properties,
    });
  }

  unsafeDetachEvtHandlers() {
    this.onExit.detach();
    this.onSendDebugLog.detach();
    this.onSetSuspendable.detach();
    this.onTaskRunHeartbeat.detach();
  }

  async cancel() {
    this._isPreparedForNextRun = false;
    this._isBeingCancelled = true;

    try {
      await this.#cancel();
    } catch (err) {}

    await this.#gracefullyTerminate(this.options.gracefulTerminationTimeoutInMs);
  }

  async cleanup(kill = true) {
    this._isPreparedForNextRun = false;

    if (this._isBeingCancelled) {
      return;
    }

    await tryCatch(this.#flush());

    if (kill) {
      await this.#gracefullyTerminate(this.options.gracefulTerminationTimeoutInMs);
    }
  }

  initialize() {
    const { env: $env, workerManifest, cwd, machineResources: machine } = this.options;

    const maxOldSpaceSize = nodeOptionsWithMaxOldSpaceSize(undefined, machine);

    const ipcTransport = $env.TRIGGER_IPC_TRANSPORT ?? process.env.TRIGGER_IPC_TRANSPORT ?? "pipe";
    const useSocketIpc = ipcTransport === "socket";
    const socketConnectTimeoutInMs = Number.parseInt(
      $env.TRIGGER_IPC_SOCKET_CONNECT_TIMEOUT_MS ??
        process.env.TRIGGER_IPC_SOCKET_CONNECT_TIMEOUT_MS ??
        "10000",
      10
    );
    const socketPath = useSocketIpc
      ? $env.TRIGGER_IPC_SOCKET_PATH ??
        process.env.TRIGGER_IPC_SOCKET_PATH ??
        `/tmp/trigger-ipc-${process.pid}-${randomUUID()}.sock`
      : undefined;

    this._ipcSocketPath = socketPath;

    const fullEnv = {
      ...$env,
      OTEL_IMPORT_HOOK_INCLUDES: workerManifest.otelImportHook?.include?.join(","),
      // TODO: this will probably need to use something different for bun (maybe --preload?)
      NODE_OPTIONS: execOptionsForRuntime(workerManifest.runtime, workerManifest, maxOldSpaceSize),
      PATH: process.env.PATH,
      TRIGGER_PROCESS_FORK_START_TIME: String(Date.now()),
      TRIGGER_WARM_START: this.options.isWarmStart ? "true" : "false",
      TRIGGERDOTDEV: "1",
      TRIGGER_IPC_TRANSPORT: ipcTransport,
      ...(socketPath ? { TRIGGER_IPC_SOCKET_PATH: socketPath } : {}),
    };

    logger.debug(`initializing task run process`, {
      env: fullEnv,
      path: workerManifest.workerEntryPoint,
      cwd,
    });

    this._child = fork(workerManifest.workerEntryPoint, executorArgs(workerManifest), {
      stdio: [/*stdin*/ "ignore", /*stdout*/ "pipe", /*stderr*/ "pipe", "ipc"],
      cwd,
      env: fullEnv,
      execArgv: ["--trace-uncaught", "--no-warnings=ExperimentalWarning"],
      execPath: execPathForRuntime(workerManifest.runtime),
      serialization: "json",
    });

    this._childPid = this._child?.pid;

    logger.debug("initialized task run process", {
      path: workerManifest.workerEntryPoint,
      cwd,
      pid: this._childPid,
    });

    let ipcProcess: IpcProcessLike | ChildProcess = this._child;

    if (useSocketIpc && socketPath) {
      this._ipcProcess = createParentSocketIpcProcess(socketPath, {
        connectTimeoutInMs:
          Number.isFinite(socketConnectTimeoutInMs) && socketConnectTimeoutInMs > 0
            ? socketConnectTimeoutInMs
            : 10_000,
      });
      ipcProcess = this._ipcProcess;
    }

    this._ipc = new ZodIpcConnection({
      listenSchema: ExecutorToWorkerMessageCatalog,
      emitSchema: WorkerToExecutorMessageCatalog,
      process: ipcProcess,
      handlers: {
        TASK_RUN_COMPLETED: async (message) => {
          const { result, execution } = message;

          const key = attemptKey(execution);

          const promiseStatus = this._attemptStatuses.get(key);

          if (promiseStatus !== "PENDING") {
            return;
          }

          this._attemptStatuses.set(key, "RESOLVED");

          const attemptPromise = this._attemptPromises.get(key);

          if (!attemptPromise) {
            return;
          }

          const { resolver } = attemptPromise;

          resolver(result);
        },
        TASK_HEARTBEAT: async (message) => {
          this.onTaskRunHeartbeat.post(message.id);
        },
        UNCAUGHT_EXCEPTION: async (message) => {
          logger.debug("uncaught exception in task run process", { ...message });
        },
        SEND_DEBUG_LOG: async (message) => {
          this.onSendDebugLog.post(message);
        },
        SET_SUSPENDABLE: async (message) => {
          this.onSetSuspendable.post(message);
        },
        MAX_DURATION_EXCEEDED: async (message) => {
          logger.debug("max duration exceeded, gracefully terminating child process", {
            maxDurationInSeconds: message.maxDurationInSeconds,
            elapsedTimeInSeconds: message.elapsedTimeInSeconds,
            pid: this.pid,
          });

          // Set flag and store duration info for error reporting in #handleExit
          this._isMaxDurationExceeded = true;
          this._maxDurationInfo = {
            maxDurationInSeconds: message.maxDurationInSeconds,
            elapsedTimeInSeconds: message.elapsedTimeInSeconds,
          };

          // Use the same graceful termination approach as cancel
          await this.#gracefullyTerminate(this.options.gracefulTerminationTimeoutInMs);
        },
      },
    });

    this._child.on("exit", this.#handleExit.bind(this));
    this._child.on("error", this.#handleError.bind(this));
    this._child.stdout?.on("data", this.#handleLog.bind(this));
    this._child.stderr?.on("data", this.#handleStdErr.bind(this));

    return this;
  }

  async #flush(timeoutInMs: number = 5_000) {
    logger.debug("flushing task run process", { pid: this.pid });

    this.#beginTrackedAckSend({ allowDuringQuiesce: true, messageType: "FLUSH" });
    try {
      await this._ipc?.sendWithAck("FLUSH", { timeoutInMs }, timeoutInMs + 1_000);
    } finally {
      this.#endTrackedAckSend();
    }
  }

  async #cancel(timeoutInMs: number = 30_000) {
    logger.debug("sending cancel message to task run process", { pid: this.pid, timeoutInMs });

    this.#beginTrackedAckSend({ allowDuringQuiesce: true, messageType: "CANCEL" });
    try {
      await this._ipc?.sendWithAck("CANCEL", { timeoutInMs }, timeoutInMs + 1_000);
    } finally {
      this.#endTrackedAckSend();
    }
  }

  async execute(
    params: TaskRunProcessExecuteParams,
    isWarmStart?: boolean
  ): Promise<TaskRunExecutionResult> {
    this._isBeingCancelled = false;
    this._isPreparedForNextRun = false;
    this._isPreparedForNextAttempt = false;

    let resolver: (value: TaskRunExecutionResult) => void;
    let rejecter: (err?: any) => void;

    const promise = new Promise<TaskRunExecutionResult>((resolve, reject) => {
      resolver = resolve;
      rejecter = reject;
    });

    const key = attemptKey(params.payload.execution);

    this._attemptStatuses.set(key, "PENDING");

    // @ts-expect-error - We know that the resolver and rejecter are defined
    this._attemptPromises.set(key, { resolver, rejecter });

    const { execution, traceContext, metrics } = params.payload;

    this._currentExecution = execution;

    if (this._child?.connected && !this._isBeingKilled && !this._child.killed) {
      logger.debug(
        `[${new Date().toISOString()}][${
          params.payload.execution.run.id
        }] sending EXECUTE_TASK_RUN message to task run process`,
        {
          pid: this.pid,
        }
      );

      await this._ipc?.send("EXECUTE_TASK_RUN", {
        execution,
        traceContext,
        metadata: this.options.serverWorker,
        metrics,
        env: params.env,
        isWarmStart: isWarmStart ?? this.options.isWarmStart,
      });
    } else {
      // Child process is dead or disconnected — the IPC send was skipped so the attempt
      // promise would hang forever. Reject it immediately to let the caller handle it.
      this._attemptStatuses.set(key, "REJECTED");

      // @ts-expect-error - rejecter is assigned in the promise constructor above
      rejecter(
        new UnexpectedExitError(
          -1,
          null,
          "Child process is not connected, cannot execute task run"
        )
      );
    }

    const result = await promise;

    this._currentExecution = undefined;
    this._isPreparedForNextAttempt = true;

    return result;
  }

  isExecuting() {
    return this._currentExecution !== undefined;
  }

  async waitpointCompleted(waitpoint: CompletedWaitpoint): Promise<void> {
    if (this._isIpcQuiescing) {
      logger.debug("waitpointCompleted: skipping while IPC is quiescing", {
        pid: this.pid,
        waitpointId: waitpoint.friendlyId,
        inFlightAckSends: this._inFlightAckSends,
      });
      return;
    }

    if (!this._child?.connected || this._isBeingKilled || this._child.killed) {
      console.error(
        "Child process not connected or being killed, can't send waitpoint completed notification"
      );
      return;
    }

    if (!this._ipc) {
      logger.debug("waitpointCompleted: missing IPC channel", { pid: this.pid, waitpoint });
      return;
    }

    const maxAttempts = 3;
    const ackTimeoutMs = 2_000;
    const retryDelayMs = 250;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      let error: unknown;
      this.#beginTrackedAckSend({ messageType: "RESOLVE_WAITPOINT" });
      try {
        [error] = await tryCatch(
          this._ipc.sendWithAck("RESOLVE_WAITPOINT", { waitpoint }, ackTimeoutMs)
        );
      } finally {
        this.#endTrackedAckSend();
      }

      if (!error) {
        return;
      }

      const isFinalAttempt = attempt === maxAttempts;
      logger.debug("waitpointCompleted: RESOLVE_WAITPOINT ack failed", {
        pid: this.pid,
        attempt,
        maxAttempts,
        error: String(error),
        waitpointId: waitpoint.friendlyId,
      });

      if (isFinalAttempt) {
        throw new Error(
          `Failed to deliver RESOLVE_WAITPOINT after ${maxAttempts} attempts: ${String(error)}`
        );
      }

      await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
    }
  }

  async probeIpcHealth(context: string, timeoutInMs: number = 2_000): Promise<{
    ok: boolean;
    seq: number;
    rttMs?: number;
    error?: string;
    parentStats: { sent: number; acked: number; failed: number };
    workerStats?: { pingReceivedCount: number; workerTimestamp: string };
  }> {
    const seq = this._ipcPingSeq++;
    this._ipcPingSent++;
    const startedAt = Date.now();
    const isRestoreProbe = context.includes("post-restore");

    if (isRestoreProbe) {
      this.logRestoreTimeline("ipc_probe_start", {
        context,
        seq,
        timeoutInMs,
      });
    }

    if (!this._ipc || !this._child?.connected || this._child.killed) {
      this._ipcPingFailed++;
      const error = "IPC channel unavailable for probe";
      logger.debug("probeIpcHealth: IPC unavailable", {
        pid: this.pid,
        seq,
        context,
        error,
      });

      return {
        ok: false,
        seq,
        error,
        parentStats: this.ipcProbeStats,
      };
    }

    let probeError: unknown;
    let result: any;
    this.#beginTrackedAckSend({ messageType: "IPC_PING" });
    try {
      [probeError, result] = await tryCatch(
        this._ipc.sendWithAck("IPC_PING", { version: "v1", seq }, timeoutInMs)
      );
    } finally {
      this.#endTrackedAckSend();
    }

    if (probeError) {
      this._ipcPingFailed++;
      const error = String(probeError);
      logger.debug("probeIpcHealth: IPC ping failed", {
        pid: this.pid,
        seq,
        context,
        timeoutInMs,
        error,
        parentStats: this.ipcProbeStats,
      });
      if (isRestoreProbe) {
        this.logRestoreTimeline("ipc_probe_error", {
          context,
          seq,
          timeoutInMs,
          elapsedMs: Date.now() - startedAt,
          error,
        });
      }

      return {
        ok: false,
        seq,
        error,
        parentStats: this.ipcProbeStats,
      };
    }

    this._ipcPingAcked++;
    const rttMs = Date.now() - startedAt;
    logger.debug("probeIpcHealth: IPC ping acked", {
      pid: this.pid,
      seq,
      context,
      rttMs,
      parentStats: this.ipcProbeStats,
      workerStats: {
        pingReceivedCount: result.pingReceivedCount,
        workerTimestamp: result.workerTimestamp,
      },
    });
    if (isRestoreProbe) {
      this.logRestoreTimeline("ipc_probe_ok", {
        context,
        seq,
        rttMs,
        elapsedMs: Date.now() - startedAt,
      });
    }

    return {
      ok: true,
      seq,
      rttMs,
      parentStats: this.ipcProbeStats,
      workerStats: {
        pingReceivedCount: result.pingReceivedCount,
        workerTimestamp: result.workerTimestamp,
      },
    };
  }

  private get ipcProbeStats() {
    return {
      sent: this._ipcPingSent,
      acked: this._ipcPingAcked,
      failed: this._ipcPingFailed,
    };
  }

  async resetIpcConnection({
    reason,
    timeoutInMs = 2_000,
  }: {
    reason: string;
    timeoutInMs?: number;
  }): Promise<boolean> {
    const ipcProcess = this._ipcProcess as IpcProcessLike | undefined;
    if (!ipcProcess?.resetConnection) {
      logger.debug("resetIpcConnection: unavailable", {
        pid: this.pid,
        reason,
      });
      return false;
    }

    try {
      await ipcProcess.resetConnection(timeoutInMs);
      logger.debug("resetIpcConnection: success", {
        pid: this.pid,
        reason,
        timeoutInMs,
      });
      return true;
    } catch (error) {
      logger.debug("resetIpcConnection: failed", {
        pid: this.pid,
        reason,
        timeoutInMs,
        error: String(error),
      });
      return false;
    }
  }

  async beginIpcQuiesce(timeoutInMs: number = 2_000): Promise<{
    ok: boolean;
    timedOut: boolean;
    pendingCount: number;
    durationMs: number;
    childAcked: boolean;
    childInFlightHandlers?: number;
    childInFlightSends?: number;
    childQuietForMs?: number;
    childError?: string;
  }> {
    this._isIpcQuiescing = true;
    const startedAt = Date.now();
    const deadline = startedAt + timeoutInMs;

    while (this._inFlightAckSends > 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 25));
    }

    const pendingCount = this._inFlightAckSends;
    const timedOut = pendingCount > 0;
    let childAcked = false;
    let childInFlightHandlers: number | undefined;
    let childInFlightSends: number | undefined;
    let childQuietForMs: number | undefined;
    let childError: string | undefined;

    if (this._ipc && this._child?.connected && !this._child.killed) {
      this.#beginTrackedAckSend({ allowDuringQuiesce: true, messageType: "IPC_QUIESCE_BEGIN" });
      try {
        const remainingMs = Math.max(100, deadline - Date.now());
        const result = await this._ipc.sendWithAck(
          "IPC_QUIESCE_BEGIN",
          {
            version: "v1",
            timeoutInMs: remainingMs,
            quietPeriodInMs: 100,
          },
          remainingMs
        );
        childAcked = true;
        childInFlightHandlers = result.workerInFlightHandlers;
        childInFlightSends = result.workerInFlightSends;
        childQuietForMs = result.workerQuietForMs;
      } catch (error) {
        childError = String(error);
      } finally {
        this.#endTrackedAckSend();
      }
    }

    const durationMs = Date.now() - startedAt;

    logger.debug("beginIpcQuiesce", {
      pid: this.pid,
      timeoutInMs,
      pendingCount,
      timedOut,
      durationMs,
      childAcked,
      childInFlightHandlers,
      childInFlightSends,
      childQuietForMs,
      childError,
    });

    return {
      ok: !timedOut && childAcked,
      timedOut,
      pendingCount,
      durationMs,
      childAcked,
      childInFlightHandlers,
      childInFlightSends,
      childQuietForMs,
      childError,
    };
  }

  async endIpcQuiesce(): Promise<EndIpcQuiesceResult> {
    if (!this._isIpcQuiescing) {
      return {
        ok: true,
        attempts: 0,
        timeoutInMs: 2_000,
        retryDelayMs: 250,
        socketPath: this._ipcSocketPath,
      };
    }

    const maxAttempts = Math.max(
      1,
      Number.parseInt(
        this.options.env.TRIGGER_IPC_QUIESCE_END_MAX_ATTEMPTS ??
          process.env.TRIGGER_IPC_QUIESCE_END_MAX_ATTEMPTS ??
          "3",
        10
      )
    );
    const timeoutInMs = Math.max(
      250,
      Number.parseInt(
        this.options.env.TRIGGER_IPC_QUIESCE_END_TIMEOUT_MS ??
          process.env.TRIGGER_IPC_QUIESCE_END_TIMEOUT_MS ??
          "2000",
        10
      )
    );
    const retryDelayMs = Math.max(
      50,
      Number.parseInt(
        this.options.env.TRIGGER_IPC_QUIESCE_END_RETRY_DELAY_MS ??
          process.env.TRIGGER_IPC_QUIESCE_END_RETRY_DELAY_MS ??
          "250",
        10
      )
    );

    let lastError: string | undefined;
    let attempts = 0;
    let ok = false;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      attempts = attempt;
      const attemptStartedAt = Date.now();
      let attemptError: string | undefined;

      if (!this._ipc || !this._child?.connected || this._child.killed) {
        attemptError = "IPC channel unavailable while ending quiesce";
      } else {
        this.#beginTrackedAckSend({ allowDuringQuiesce: true, messageType: "IPC_QUIESCE_END" });
        try {
          await this._ipc.sendWithAck("IPC_QUIESCE_END", { version: "v1" }, timeoutInMs);
        } catch (error) {
          attemptError = String(error);
        } finally {
          this.#endTrackedAckSend();
        }
      }

      const probeSeq = this._ipcPingSeq++;
      this._ipcPingSent++;
      const probeStartedAt = Date.now();
      let probeOk = false;
      let probeError: string | undefined;

      if (!this._ipc || !this._child?.connected || this._child.killed) {
        this._ipcPingFailed++;
        probeError = "IPC channel unavailable for post-quiesce probe";
      } else {
        this.#beginTrackedAckSend({ allowDuringQuiesce: true, messageType: "IPC_PING" });
        try {
          await this._ipc.sendWithAck("IPC_PING", { version: "v1", seq: probeSeq }, timeoutInMs);
          this._ipcPingAcked++;
          probeOk = true;
        } catch (error) {
          this._ipcPingFailed++;
          probeError = String(error);
        } finally {
          this.#endTrackedAckSend();
        }
      }

      const elapsedMs = Date.now() - attemptStartedAt;
      const probeElapsedMs = Date.now() - probeStartedAt;

      this.logRestoreTimeline("ipc_quiesce_end_attempt", {
        pid: this.pid,
        attempt,
        maxAttempts,
        timeoutInMs,
        retryDelayMs,
        elapsedMs,
        socketPath: this._ipcSocketPath,
        ackOk: !attemptError,
        ackError: attemptError,
        probeOk,
        probeSeq,
        probeElapsedMs,
        probeError,
      });

      if (!attemptError && probeOk) {
        ok = true;
        break;
      }

      lastError = attemptError ?? probeError ?? "unknown IPC quiesce-end failure";

      if (attempt < maxAttempts) {
        await new Promise((resolve) => setTimeout(resolve, retryDelayMs));
      }
    }

    this._isIpcQuiescing = false;
    logger.debug("endIpcQuiesce", {
      pid: this.pid,
      ok,
      attempts,
      timeoutInMs,
      retryDelayMs,
      socketPath: this._ipcSocketPath,
      lastError,
      inFlightAckSends: this._inFlightAckSends,
    });

    return {
      ok,
      attempts,
      timeoutInMs,
      retryDelayMs,
      socketPath: this._ipcSocketPath,
      lastError,
    };
  }

  #beginTrackedAckSend(opts?: { allowDuringQuiesce?: boolean; messageType?: string }) {
    if (this._isIpcQuiescing && !opts?.allowDuringQuiesce) {
      throw new Error(
        `IPC quiescing: refusing sendWithAck for ${opts?.messageType ?? "unknown-message"}`
      );
    }

    this._inFlightAckSends++;
  }

  #endTrackedAckSend() {
    this._inFlightAckSends = Math.max(0, this._inFlightAckSends - 1);
  }

  #handleError(error: Error) {
    logger.debug("child process error", { error, pid: this.pid });
  }

  async #handleExit(code: number | null, signal: NodeJS.Signals | null) {
    logger.debug("handling child exit", { code, signal, pid: this.pid });

    await this._ipcProcess?.close();
    this._ipcProcess = undefined;

    // Go through all the attempts currently pending and reject them
    for (const [id, status] of this._attemptStatuses.entries()) {
      if (status === "PENDING") {
        logger.debug("found pending attempt", { id });

        this._attemptStatuses.set(id, "REJECTED");

        const attemptPromise = this._attemptPromises.get(id);

        if (!attemptPromise) {
          continue;
        }

        const { rejecter } = attemptPromise;

        if (this._isMaxDurationExceeded) {
          if (!this._maxDurationInfo) {
            rejecter(
              new UnexpectedExitError(
                code ?? -1,
                signal,
                "MaxDuration flag set but duration info missing"
              )
            );
            continue;
          }

          rejecter(
            new MaxDurationExceededError(
              this._maxDurationInfo.maxDurationInSeconds,
              this._maxDurationInfo.elapsedTimeInSeconds
            )
          );
        } else if (this._isBeingCancelled) {
          rejecter(new CancelledProcessError());
        } else if (this._gracefulExitTimeoutElapsed) {
          // Order matters, this has to be before the graceful exit timeout
          rejecter(new GracefulExitTimeoutError());
        } else if (this._isBeingKilled) {
          if (this._isBeingSuspended) {
            rejecter(new SuspendedProcessError());
          } else {
            rejecter(new CleanupProcessError());
          }
        } else {
          rejecter(
            new UnexpectedExitError(
              code ?? -1,
              signal,
              this._stderr.length ? this._stderr.join("\n") : undefined
            )
          );
        }
      }
    }

    logger.debug("Task run process exited, posting onExit", { code, signal, pid: this.pid });

    this.onExit.post({ code, signal, pid: this.pid });
  }

  #handleLog(data: Buffer) {
    if (!this._currentExecution) {
      logger.log(`${chalkGrey("○")} ${chalkGrey(prettyPrintDate(new Date()))} ${data.toString()}`);

      return;
    }

    const runId = chalkRun(
      `${this._currentExecution.run.id}.${this._currentExecution.attempt.number}`
    );

    logger.log(
      `${chalkGrey("○")} ${chalkGrey(prettyPrintDate(new Date()))} ${runId} ${data.toString()}`
    );
  }

  #handleStdErr(data: Buffer) {
    if (this._isBeingKilled) {
      return;
    }

    if (!this._currentExecution) {
      logger.log(`${chalkError("○")} ${chalkGrey(prettyPrintDate(new Date()))} ${data.toString()}`);

      return;
    }

    const runId = chalkRun(
      `${this._currentExecution.run.id}.${this._currentExecution.attempt.number}`
    );

    const errorLine = data.toString();

    logger.log(
      `${chalkError("○")} ${chalkGrey(prettyPrintDate(new Date()))} ${runId} ${errorLine}`
    );

    if (this._stderr.length > 100) {
      this._stderr.shift();
    }
    this._stderr.push(errorLine);
  }

  async #gracefullyTerminate(timeoutInMs: number = 1_000) {
    logger.debug("gracefully terminating task run process", { pid: this.pid, timeoutInMs });

    await this.kill("SIGTERM", timeoutInMs);

    if (this._child?.connected) {
      logger.debug("child process is still connected, sending SIGKILL", { pid: this.pid });

      await this.kill("SIGKILL");
    }
  }

  /** This will never throw. */
  async kill(signal?: number | NodeJS.Signals, timeoutInMs?: number) {
    logger.debug(`killing task run process`, {
      signal,
      timeoutInMs,
      pid: this.pid,
    });

    this._isBeingKilled = true;

    const killTimeout = this.onExit.waitFor(timeoutInMs);

    try {
      this._child?.kill(signal);
    } catch (error) {
      logger.debug("kill: failed to kill child process", { error });
    }

    if (!timeoutInMs) {
      return;
    }

    const [error] = await tryCatch(killTimeout);

    if (error) {
      logger.debug("kill: failed to wait for child process to exit", {
        timeoutInMs,
        signal,
        pid: this.pid,
      });
    }
  }

  async suspend({ flush }: { flush: boolean }) {
    this._isBeingSuspended = true;

    if (flush) {
      await tryCatch(this.#flush());
    }

    await this.kill("SIGKILL");
  }

  get isBeingKilled() {
    return this._isBeingKilled || this._child?.killed;
  }

  get isBeingSuspended() {
    return this._isBeingSuspended;
  }

  get pid() {
    return this._childPid;
  }

  get isHealthy() {
    if (!this._child) {
      return false;
    }

    if (this.isBeingKilled || this.isBeingSuspended) {
      return false;
    }

    return this._child.connected;
  }

  static parseExecuteError(error: unknown, dockerMode = true): TaskRunInternalError {
    if (error instanceof CancelledProcessError) {
      return {
        type: "INTERNAL_ERROR",
        code: TaskRunErrorCodes.TASK_RUN_CANCELLED,
      };
    }

    if (error instanceof MaxDurationExceededError) {
      return {
        type: "INTERNAL_ERROR",
        code: TaskRunErrorCodes.MAX_DURATION_EXCEEDED,
        message: error.message,
      };
    }

    if (error instanceof CleanupProcessError) {
      return {
        type: "INTERNAL_ERROR",
        code: TaskRunErrorCodes.TASK_EXECUTION_ABORTED,
      };
    }

    if (error instanceof UnexpectedExitError) {
      return internalErrorFromUnexpectedExit(error, dockerMode);
    }

    if (error instanceof GracefulExitTimeoutError) {
      return {
        type: "INTERNAL_ERROR",
        code: TaskRunErrorCodes.GRACEFUL_EXIT_TIMEOUT,
      };
    }

    return {
      type: "INTERNAL_ERROR",
      code: TaskRunErrorCodes.TASK_EXECUTION_FAILED,
      message: String(error),
    };
  }
}

function executorArgs(workerManifest: WorkerManifest): string[] {
  return [];
}
