import {
  type CompleteRunAttemptResult,
  type RunExecutionData,
  SuspendedProcessError,
  type TaskRunExecutionMetrics,
  type TaskRunExecutionResult,
  TaskRunExecutionRetry,
  TaskRunExecutionStatus,
  type TaskRunFailedExecutionResult,
  WorkerManifest,
} from "@basicblock/trigger-core/v3";
import { type WorkloadRunAttemptStartResponseBody } from "@basicblock/trigger-core/v3/workers";
import { TaskRunProcess } from "../../executions/taskRunProcess.js";
import { RunLogger, SendDebugLogOptions } from "./logger.js";
import { RunnerEnv } from "./env.js";
import { WorkloadHttpClient } from "@basicblock/trigger-core/v3/workers";
import { setTimeout as sleep } from "timers/promises";
import { RunExecutionSnapshotPoller } from "./poller.js";
import { assertExhaustive, tryCatch } from "@basicblock/trigger-core/utils";
import { Metadata, MetadataClient } from "./overrides.js";
import { randomBytes } from "node:crypto";
import { SnapshotManager, SnapshotState } from "./snapshot.js";
import type { SupervisorSocket } from "./controller.js";
import { RunNotifier } from "./notifier.js";
import { TaskRunProcessProvider } from "./taskRunProcessProvider.js";

class ExecutionAbortError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ExecutionAbortError";
  }
}

type RunExecutionOptions = {
  workerManifest: WorkerManifest;
  env: RunnerEnv;
  httpClient: WorkloadHttpClient;
  logger: RunLogger;
  supervisorSocket: SupervisorSocket;
  taskRunProcessProvider: TaskRunProcessProvider;
};

type RunExecutionPrepareOptions = {
  taskRunEnv: Record<string, string>;
};

type RunExecutionRunOptions = {
  runFriendlyId: string;
  snapshotFriendlyId: string;
  dequeuedAt?: Date;
  podScheduledAt?: Date;
  isWarmStart?: boolean;
};

const SNAPSHOTS_SINCE_RETRY_DELAYS_MS = [250, 1000, 2000] as const;
const COMPLETE_RUN_ATTEMPT_RETRY_DELAYS_MS = [250, 1000, 2000] as const;
const RESTORE_NETWORK_READY_TIMEOUT_MS = 45_000;
const RESTORE_NETWORK_READY_POLL_MS = 250;

export class RunExecution {
  private id: string;
  private executionAbortController: AbortController;

  private _runFriendlyId?: string;
  private currentAttemptNumber?: number;
  private currentTaskRunEnv?: Record<string, string>;
  private snapshotManager?: SnapshotManager;

  private dequeuedAt?: Date;
  private podScheduledAt?: Date;
  private readonly workerManifest: WorkerManifest;
  private readonly env: RunnerEnv;
  private readonly httpClient: WorkloadHttpClient;
  private readonly logger: RunLogger;
  private restoreCount: number;

  private taskRunProcess?: TaskRunProcess;
  private snapshotPoller?: RunExecutionSnapshotPoller;

  private lastHeartbeat?: Date;
  private isShuttingDown = false;
  private shutdownReason?: string;

  private isCompletingRun = false;
  private ignoreSnapshotChanges = false;

  private supervisorSocket: SupervisorSocket;
  private notifier?: RunNotifier;
  private metadataClient?: MetadataClient;
  private taskRunProcessProvider: TaskRunProcessProvider;

  constructor(opts: RunExecutionOptions) {
    this.id = randomBytes(4).toString("hex");
    this.workerManifest = opts.workerManifest;
    this.env = opts.env;
    this.httpClient = opts.httpClient;
    this.logger = opts.logger;
    this.supervisorSocket = opts.supervisorSocket;
    this.taskRunProcessProvider = opts.taskRunProcessProvider;

    this.restoreCount = 0;
    this.executionAbortController = new AbortController();

    if (this.env.TRIGGER_METADATA_URL) {
      this.metadataClient = new MetadataClient(this.env.TRIGGER_METADATA_URL);
    }
  }

  /**
   * Cancels the current execution.
   */
  public async cancel(): Promise<void> {
    if (this.isShuttingDown) {
      throw new Error("cancel called after execution shut down");
    }

    this.sendDebugLog("cancelling attempt", { runId: this.runFriendlyId });

    await this.taskRunProcess?.cancel();
  }

  /**
   * Kills the current execution.
   */
  public async kill({ exitExecution = true }: { exitExecution?: boolean } = {}) {
    if (this.taskRunProcess) {
      await this.taskRunProcessProvider.handleProcessAbort(this.taskRunProcess);
    }

    if (exitExecution) {
      this.shutdownExecution("kill");
    }
  }

  public async shutdown() {
    if (this.taskRunProcess) {
      await this.taskRunProcessProvider.handleProcessAbort(this.taskRunProcess);
    }

    this.shutdownExecution("shutdown");
  }

  /**
   * Prepares the execution with task run environment variables.
   * This should be called before executing, typically after a successful run to prepare for the next one.
   */
  public async prepareForExecution(opts: RunExecutionPrepareOptions) {
    if (this.isShuttingDown) {
      throw new Error("prepareForExecution called after execution shut down");
    }

    if (this.taskRunProcess) {
      throw new Error("prepareForExecution called after process was already created");
    }

    // Set the task run environment so canExecute returns true
    this.currentTaskRunEnv = opts.taskRunEnv;

    this.taskRunProcess = await this.taskRunProcessProvider.getProcess({
      taskRunEnv: opts.taskRunEnv,
      isWarmStart: true,
    });
  }

  private attachTaskRunProcessHandlers(taskRunProcess: TaskRunProcess): void {
    taskRunProcess.unsafeDetachEvtHandlers();

    taskRunProcess.onTaskRunHeartbeat.attach(async (runId) => {
      if (!this.runFriendlyId) {
        this.sendDebugLog("onTaskRunHeartbeat: missing run ID", { heartbeatRunId: runId });
        return;
      }

      if (runId !== this.runFriendlyId) {
        this.sendDebugLog("onTaskRunHeartbeat: mismatched run ID", {
          heartbeatRunId: runId,
          expectedRunId: this.runFriendlyId,
        });
        return;
      }

      const [error] = await tryCatch(this.onHeartbeat());

      if (error) {
        this.sendDebugLog("onTaskRunHeartbeat: failed", { error: error.message });
      }
    });

    taskRunProcess.onSendDebugLog.attach(async (debugLog) => {
      this.sendRuntimeDebugLog(debugLog.message, debugLog.properties);
    });

    taskRunProcess.onSetSuspendable.attach(async ({ suspendable }) => {
      this.suspendable = suspendable;
    });
  }

  /**
   * Returns true if no run has been started yet and we're prepared for the next run.
   */
  get canExecute(): boolean {
    if (this.taskRunProcessProvider.hasPersistentProcess) {
      return true;
    }

    // If we've ever had a run ID, this execution can't be reused
    if (this._runFriendlyId) {
      return false;
    }

    // We can execute if we have the task run environment ready
    return !!this.currentTaskRunEnv;
  }

  /**
   * Called by the RunController when it receives a websocket notification
   * or when the snapshot poller detects a change.
   *
   * This is the main entry point for snapshot changes, but processing is deferred to the snapshot manager.
   */
  private async enqueueSnapshotChangesAndWait(snapshots: RunExecutionData[]): Promise<void> {
    if (this.isShuttingDown) {
      this.sendDebugLog("enqueueSnapshotChangeAndWait: shutting down, skipping");
      return;
    }

    if (!this.snapshotManager) {
      this.sendDebugLog("enqueueSnapshotChangeAndWait: missing snapshot manager");
      return;
    }

    await this.snapshotManager.handleSnapshotChanges(snapshots);
  }

  private async processSnapshotChange(
    runData: RunExecutionData,
    deprecated: boolean
  ): Promise<void> {
    const { run, snapshot, completedWaitpoints } = runData;

    const snapshotMetadata = {
      incomingSnapshotId: snapshot.friendlyId,
      completedWaitpoints: completedWaitpoints.length,
    };

    if (this.ignoreSnapshotChanges) {
      this.sendDebugLog("processSnapshotChange: ignoring snapshot change", {
        incomingSnapshotId: snapshot.friendlyId,
        completedWaitpoints: completedWaitpoints.length,
        currentAttemptNumber: this.currentAttemptNumber,
        newAttemptNumber: run.attemptNumber,
      });
      return;
    }

    if (!this.snapshotManager) {
      this.sendDebugLog("handleSnapshotChange: missing snapshot manager", snapshotMetadata);
      return;
    }

    if (this.currentAttemptNumber && this.currentAttemptNumber !== run.attemptNumber) {
      this.sendDebugLog("error: attempt number mismatch", snapshotMetadata);
      // This is a rogue execution, a new one will already have been created elsewhere
      await this.exitTaskRunProcessWithoutFailingRun({
        flush: false,
        reason: "attempt number mismatch",
      });
      return;
    }

    // DO NOT REMOVE (very noisy, but helpful for debugging)
    // this.sendDebugLog(`processing snapshot change: ${snapshot.executionStatus}`, snapshotMetadata);

    // Reset the snapshot poll interval so we don't do unnecessary work
    this.snapshotPoller?.updateSnapshotId(snapshot.friendlyId);
    this.snapshotPoller?.resetCurrentInterval();

    if (deprecated) {
      this.sendDebugLog("run execution is deprecated", { incomingSnapshot: snapshot });

      await this.exitTaskRunProcessWithoutFailingRun({
        flush: false,
        reason: "deprecated execution",
      });
      return;
    }

    switch (snapshot.executionStatus) {
      case "PENDING_CANCEL": {
        this.sendDebugLog("run was cancelled", snapshotMetadata);

        const [error] = await tryCatch(this.cancel());

        if (error) {
          this.sendDebugLog("snapshot change: failed to cancel attempt", {
            ...snapshotMetadata,
            error: error.message,
          });
        }

        this.abortExecution();
        return;
      }
      case "QUEUED": {
        this.sendDebugLog("run was re-queued", snapshotMetadata);

        await this.exitTaskRunProcessWithoutFailingRun({ flush: true, reason: "re-queued" });
        return;
      }
      case "FINISHED": {
        this.sendDebugLog("run is finished", snapshotMetadata);

        // We are finishing the run in handleCompletionResult, so we don't need to do anything here
        if (this.isCompletingRun) {
          this.sendDebugLog("run is finished but we're completing it, skipping", snapshotMetadata);
          return;
        }

        await this.exitTaskRunProcessWithoutFailingRun({ flush: true, reason: "already-finished" });
        return;
      }
      case "QUEUED_EXECUTING":
      case "EXECUTING_WITH_WAITPOINTS": {
        this.sendDebugLog("run is executing with waitpoints", snapshotMetadata);

        // Wait for next status change - suspension is handled by the snapshot manager
        return;
      }
      case "SUSPENDED": {
        this.sendDebugLog("run was suspended", snapshotMetadata);

        // This will kill the process and fail the execution with a SuspendedProcessError
        // We don't flush because we already did before suspending
        await this.exitTaskRunProcessWithoutFailingRun({ flush: false, reason: "suspended" });
        return;
      }
      case "PENDING_EXECUTING": {
        this.sendDebugLog("run is pending execution", snapshotMetadata);
        this.logRestoreFlow("pending_execution_snapshot", snapshotMetadata);

        if (completedWaitpoints.length === 0) {
          this.sendDebugLog("no waitpoints to complete, nothing to do", snapshotMetadata);
          return;
        }

        const [error] = await tryCatch(this.restore());

        if (error) {
          this.sendDebugLog("failed to restore execution", {
            ...snapshotMetadata,
            error: error.message,
          });

          const completion = {
            id: run.id,
            ok: false,
            retry: undefined,
            error: TaskRunProcess.parseExecuteError(error),
          } satisfies TaskRunFailedExecutionResult;

          const [completeError] = await tryCatch(this.complete({ completion }));

          if (completeError) {
            this.sendDebugLog("failed to complete run after restore failure", {
              ...snapshotMetadata,
              error: completeError.message,
            });
          }

          this.abortExecution();
          return;
        }

        return;
      }
      case "EXECUTING": {
        if (completedWaitpoints.length === 0) {
          this.sendDebugLog("run is executing without completed waitpoints", snapshotMetadata);
          return;
        }

        this.sendDebugLog("run is executing with completed waitpoints", snapshotMetadata);

        if (!this.taskRunProcess) {
          this.sendDebugLog("no task run process, ignoring completed waitpoints", snapshotMetadata);

          this.abortExecution();
          return;
        }

        const probe = await this.taskRunProcess.probeIpcHealth("before-waitpoint-replay");
        this.sendDebugLog("[restore-flow] ipc_probe_before_waitpoint_replay", {
          probeOk: probe.ok,
          probeSeq: probe.seq,
          probeRttMs: probe.rttMs,
          probeError: probe.error,
          parentProbeSent: probe.parentStats.sent,
          parentProbeAcked: probe.parentStats.acked,
          parentProbeFailed: probe.parentStats.failed,
          workerPingReceivedCount: probe.workerStats?.pingReceivedCount,
          workerTimestamp: probe.workerStats?.workerTimestamp,
          ...snapshotMetadata,
        });

        let healthyProbe = probe;
        if (!healthyProbe.ok) {
          const resetOk = await this.taskRunProcess.resetIpcConnection({
            reason: "before-waitpoint-replay",
            timeoutInMs: 2_000,
          });

          this.sendDebugLog("[restore-flow] ipc_reset_before_waitpoint_replay", {
            resetOk,
            ...snapshotMetadata,
          });

          if (resetOk) {
            healthyProbe = await this.taskRunProcess.probeIpcHealth(
              "before-waitpoint-replay-after-reset"
            );
            this.sendDebugLog("[restore-flow] ipc_probe_before_waitpoint_replay_after_reset", {
              probeOk: healthyProbe.ok,
              probeSeq: healthyProbe.seq,
              probeRttMs: healthyProbe.rttMs,
              probeError: healthyProbe.error,
              parentProbeSent: healthyProbe.parentStats.sent,
              parentProbeAcked: healthyProbe.parentStats.acked,
              parentProbeFailed: healthyProbe.parentStats.failed,
              workerPingReceivedCount: healthyProbe.workerStats?.pingReceivedCount,
              workerTimestamp: healthyProbe.workerStats?.workerTimestamp,
              ...snapshotMetadata,
            });
          }
        }

        if (!healthyProbe.ok) {
          this.sendDebugLog("[restore-flow] ipc_unhealthy_before_waitpoint_replay_abort", {
            probeError: healthyProbe.error,
            ...snapshotMetadata,
          });
          this.abortExecution();
          return;
        }

        for (const waitpoint of completedWaitpoints) {
          const [waitpointError] = await tryCatch(this.taskRunProcess.waitpointCompleted(waitpoint));
          if (!waitpointError) {
            continue;
          }

          this.sendDebugLog("[restore-flow] waitpoint_replay_failed", {
            waitpointId: waitpoint.friendlyId,
            error: String(waitpointError),
            ...snapshotMetadata,
          });

          const resetOk = await this.taskRunProcess.resetIpcConnection({
            reason: "waitpoint-replay-failed",
            timeoutInMs: 2_000,
          });
          this.sendDebugLog("[restore-flow] ipc_reset_after_waitpoint_replay_failure", {
            resetOk,
            waitpointId: waitpoint.friendlyId,
            ...snapshotMetadata,
          });

          if (!resetOk) {
            this.abortExecution();
            return;
          }

          const reprobe = await this.taskRunProcess.probeIpcHealth(
            "after-waitpoint-replay-failure-reset"
          );
          this.sendDebugLog("[restore-flow] ipc_probe_after_waitpoint_replay_failure_reset", {
            probeOk: reprobe.ok,
            probeSeq: reprobe.seq,
            probeRttMs: reprobe.rttMs,
            probeError: reprobe.error,
            parentProbeSent: reprobe.parentStats.sent,
            parentProbeAcked: reprobe.parentStats.acked,
            parentProbeFailed: reprobe.parentStats.failed,
            workerPingReceivedCount: reprobe.workerStats?.pingReceivedCount,
            workerTimestamp: reprobe.workerStats?.workerTimestamp,
            waitpointId: waitpoint.friendlyId,
            ...snapshotMetadata,
          });

          if (!reprobe.ok) {
            this.abortExecution();
            return;
          }

          const [retryError] = await tryCatch(this.taskRunProcess.waitpointCompleted(waitpoint));
          if (retryError) {
            this.sendDebugLog("[restore-flow] waitpoint_replay_failed_after_reset", {
              waitpointId: waitpoint.friendlyId,
              error: String(retryError),
              ...snapshotMetadata,
            });
            this.abortExecution();
            return;
          }
        }

        return;
      }
      case "RUN_CREATED":
      case "DELAYED": {
        this.sendDebugLog(
          "aborting execution: invalid status change: RUN_CREATED or DELAYED",
          snapshotMetadata
        );

        this.abortExecution();
        return;
      }
      default: {
        assertExhaustive(snapshot.executionStatus);
      }
    }
  }

  private async startAttempt({
    isWarmStart,
  }: {
    isWarmStart?: boolean;
  }): Promise<WorkloadRunAttemptStartResponseBody & { metrics: TaskRunExecutionMetrics }> {
    if (!this.runFriendlyId || !this.snapshotManager) {
      throw new Error("Cannot start attempt: missing run or snapshot manager");
    }

    // Reset this for the new attempt
    this.isCompletingRun = false;

    this.sendDebugLog("starting attempt", { isWarmStart: String(isWarmStart) });

    const attemptStartedAt = Date.now();

    // Check for abort before each major async operation
    if (this.executionAbortController.signal.aborted) {
      throw new ExecutionAbortError("Execution aborted before start");
    }

    const start = await this.httpClient.startRunAttempt(
      this.runFriendlyId,
      this.snapshotManager.snapshotId,
      { isWarmStart }
    );

    if (this.executionAbortController.signal.aborted) {
      throw new ExecutionAbortError("Execution aborted after start");
    }

    if (!start.success) {
      throw new Error(`Start API call failed: ${start.error}`);
    }

    // A snapshot was just created, so update the snapshot ID
    this.snapshotManager.updateSnapshot(
      start.data.snapshot.friendlyId,
      start.data.snapshot.executionStatus
    );

    // Also set or update the attempt number - we do this to detect illegal attempt number changes, e.g. from stalled runners coming back online
    const attemptNumber = start.data.run.attemptNumber;
    if (attemptNumber && attemptNumber > 0) {
      this.currentAttemptNumber = attemptNumber;
    } else {
      this.sendDebugLog("error: invalid attempt number returned from start attempt", {
        attemptNumber: String(attemptNumber),
      });
    }

    const metrics = this.measureExecutionMetrics({
      attemptCreatedAt: attemptStartedAt,
      dequeuedAt: this.dequeuedAt?.getTime(),
      podScheduledAt: this.podScheduledAt?.getTime(),
    });

    this.sendDebugLog("started attempt", { start: start.data });

    return { ...start.data, metrics };
  }

  /**
   * Executes the run. This will return when the execution is complete and we should warm start.
   * When this returns, the child process will have been cleaned up.
   */
  public async execute(runOpts: RunExecutionRunOptions): Promise<void> {
    if (this.isShuttingDown) {
      throw new Error("execute called after execution shut down");
    }

    // Setup initial state
    this.runFriendlyId = runOpts.runFriendlyId;

    // Create snapshot manager
    this.snapshotManager = new SnapshotManager({
      runFriendlyId: runOpts.runFriendlyId,
      runnerId: this.env.TRIGGER_RUNNER_ID,
      initialSnapshotId: runOpts.snapshotFriendlyId,
      // We're just guessing here, but "PENDING_EXECUTING" is probably fine
      initialStatus: "PENDING_EXECUTING",
      logger: this.logger,
      metadataClient: this.metadataClient,
      onSnapshotChange: this.processSnapshotChange.bind(this),
      onSuspendable: this.handleSuspendable.bind(this),
    });

    this.dequeuedAt = runOpts.dequeuedAt;
    this.podScheduledAt = runOpts.podScheduledAt;

    // Create and start services
    this.snapshotPoller = new RunExecutionSnapshotPoller({
      runFriendlyId: this.runFriendlyId,
      snapshotFriendlyId: this.snapshotManager.snapshotId,
      logger: this.logger,
      snapshotPollIntervalSeconds: this.env.TRIGGER_SNAPSHOT_POLL_INTERVAL_SECONDS,
      onPoll: this.fetchAndProcessSnapshotChanges.bind(this),
    }).start();

    this.notifier = new RunNotifier({
      runFriendlyId: this.runFriendlyId,
      supervisorSocket: this.supervisorSocket,
      onNotify: this.fetchAndProcessSnapshotChanges.bind(this),
      logger: this.logger,
    }).start();

    const [startError, start] = await tryCatch(
      this.startAttempt({ isWarmStart: runOpts.isWarmStart })
    );

    if (startError) {
      this.sendDebugLog("failed to start attempt", { error: startError.message });

      this.shutdownExecution("failed to start attempt");
      return;
    }

    const [executeError] = await tryCatch(
      this.executeRunWrapper({ ...start, isWarmStart: runOpts.isWarmStart })
    );

    if (executeError) {
      this.sendDebugLog("failed to execute run", { error: executeError.message });

      this.shutdownExecution("failed to execute run");
      return;
    }

    // This is here for safety, but it
    this.shutdownExecution("execute call finished");
  }

  private async executeRunWrapper({
    run,
    snapshot,
    envVars,
    execution,
    metrics,
    isWarmStart,
    isImmediateRetry,
  }: WorkloadRunAttemptStartResponseBody & {
    metrics: TaskRunExecutionMetrics;
    isWarmStart?: boolean;
    isImmediateRetry?: boolean;
  }) {
    this.currentTaskRunEnv = envVars;

    const [executeError] = await tryCatch(
      this.executeRun({
        run,
        snapshot,
        envVars,
        execution,
        metrics,
        isWarmStart,
        isImmediateRetry,
      })
    );

    if (!executeError) {
      return;
    }

    if (executeError instanceof SuspendedProcessError) {
      this.sendDebugLog("execution was suspended", {
        run: run.friendlyId,
        snapshot: snapshot.friendlyId,
        error: executeError.message,
      });

      return;
    }

    if (executeError instanceof ExecutionAbortError) {
      this.sendDebugLog("execution was aborted", {
        run: run.friendlyId,
        snapshot: snapshot.friendlyId,
        error: executeError.message,
      });

      return;
    }

    this.sendDebugLog("error while executing attempt", {
      error: executeError.message,
      runId: run.friendlyId,
      snapshotId: snapshot.friendlyId,
    });

    const completion = {
      id: execution.run.id,
      ok: false,
      retry: undefined,
      error: TaskRunProcess.parseExecuteError(executeError),
    } satisfies TaskRunFailedExecutionResult;

    const [completeError] = await tryCatch(this.complete({ completion }));

    if (completeError) {
      this.sendDebugLog("failed to complete run", { error: completeError.message });
    }
  }

  private async executeRun({
    run,
    snapshot,
    envVars,
    execution,
    metrics,
    isWarmStart,
    isImmediateRetry,
  }: WorkloadRunAttemptStartResponseBody & {
    metrics: TaskRunExecutionMetrics;
    isWarmStart?: boolean;
    isImmediateRetry?: boolean;
  }) {
    if (isImmediateRetry) {
      await this.taskRunProcessProvider.handleImmediateRetry();
    }

    const taskRunEnv = this.currentTaskRunEnv ?? envVars;

    if (!this.taskRunProcess || this.taskRunProcess.isBeingKilled) {
      this.sendDebugLog("getting new task run process", { runId: execution.run.id });
      this.taskRunProcess = await this.taskRunProcessProvider.getProcess({
        taskRunEnv: { ...taskRunEnv, TRIGGER_PROJECT_REF: execution.project.ref },
        isWarmStart,
      });
    } else {
      this.sendDebugLog("using prepared task run process", { runId: execution.run.id });
    }

    this.attachTaskRunProcessHandlers(this.taskRunProcess);

    this.sendDebugLog("executing task run process", { runId: execution.run.id });

    const abortHandler = async () => {
      this.sendDebugLog("execution aborted during task run, cleaning up process", {
        runId: execution.run.id,
      });

      if (this.taskRunProcess) {
        await this.taskRunProcessProvider.handleProcessAbort(this.taskRunProcess);
      }
    };

    // Set up an abort handler that will cleanup the task run process
    this.executionAbortController.signal.addEventListener("abort", abortHandler);

    const completion = await this.taskRunProcess.execute(
      {
        payload: {
          execution,
          traceContext: execution.run.traceContext ?? {},
          metrics,
        },
        messageId: run.friendlyId,
        env: envVars,
      },
      isWarmStart
    );

    this.executionAbortController.signal.removeEventListener("abort", abortHandler);

    // If we get here, the task completed normally
    this.sendDebugLog("completed run attempt", { attemptSuccess: completion.ok });

    // Return the process to the provider - this handles all cleanup logic
    const [returnError] = await tryCatch(
      this.taskRunProcessProvider.returnProcess(this.taskRunProcess)
    );

    if (returnError) {
      this.sendDebugLog("failed to return task run process, submitting completion anyway", {
        error: returnError.message,
      });
    }

    const [completionError] = await tryCatch(this.complete({ completion }));

    if (completionError) {
      this.sendDebugLog("failed to complete run", { error: completionError.message });
    }
  }

  private async complete({ completion }: { completion: TaskRunExecutionResult }): Promise<void> {
    if (!this.runFriendlyId || !this.snapshotManager) {
      throw new Error("cannot complete run: missing run or snapshot manager");
    }

    this.isCompletingRun = true;

    let completionResult:
      | Awaited<ReturnType<WorkloadHttpClient["completeRunAttempt"]>>
      | undefined;

    for (let attempt = 0; attempt <= COMPLETE_RUN_ATTEMPT_RETRY_DELAYS_MS.length; attempt++) {
      completionResult = await this.httpClient.completeRunAttempt(
        this.runFriendlyId,
        this.snapshotManager.snapshotId,
        { completion }
      );

      if (completionResult.success) {
        break;
      }

      const isStatus4xx = /^Status 4\d\d\b/.test(completionResult.error);
      const hasRetryLeft = attempt < COMPLETE_RUN_ATTEMPT_RETRY_DELAYS_MS.length;

      if (!hasRetryLeft || isStatus4xx) {
        throw new Error(`failed to submit completion: ${completionResult.error}`);
      }

      const retryDelayMs = COMPLETE_RUN_ATTEMPT_RETRY_DELAYS_MS[attempt] ?? 1000;

      this.sendDebugLog("complete run attempt failed, retrying", {
        attempt: attempt + 1,
        retryDelayMs,
        error: completionResult.error,
      });

      await sleep(retryDelayMs);
    }

    if (!completionResult?.success) {
      throw new Error("failed to submit completion: unknown error");
    }

    await this.handleCompletionResult({
      completion,
      result: completionResult.data.result,
    });
  }

  private async handleCompletionResult({
    completion,
    result,
  }: {
    completion: TaskRunExecutionResult;
    result: CompleteRunAttemptResult;
  }) {
    this.sendDebugLog(`completion result: ${result.attemptStatus}`, {
      attemptSuccess: completion.ok,
      attemptStatus: result.attemptStatus,
      snapshotId: result.snapshot.friendlyId,
      runId: result.run.friendlyId,
    });

    const snapshotStatus = this.convertAttemptStatusToSnapshotStatus(result.attemptStatus);

    // Update our snapshot ID to match the completion result to ensure any subsequent API calls use the correct snapshot
    this.updateSnapshotAfterCompletion(result.snapshot.friendlyId, snapshotStatus);

    const { attemptStatus } = result;

    switch (attemptStatus) {
      case "RUN_FINISHED":
      case "RUN_PENDING_CANCEL":
      case "RETRY_QUEUED": {
        return;
      }
      case "RETRY_IMMEDIATELY": {
        if (attemptStatus !== "RETRY_IMMEDIATELY") {
          return;
        }

        if (completion.ok) {
          throw new Error("Should retry but completion OK.");
        }

        if (!completion.retry) {
          throw new Error("Should retry but missing retry params.");
        }

        await this.retryImmediately({ retryOpts: completion.retry });
        return;
      }
      default: {
        assertExhaustive(attemptStatus);
      }
    }
  }

  private updateSnapshotAfterCompletion(snapshotId: string, status: TaskRunExecutionStatus) {
    this.snapshotManager?.updateSnapshot(snapshotId, status);
    this.snapshotPoller?.updateSnapshotId(snapshotId);
  }

  private convertAttemptStatusToSnapshotStatus(
    attemptStatus: CompleteRunAttemptResult["attemptStatus"]
  ): TaskRunExecutionStatus {
    switch (attemptStatus) {
      case "RUN_FINISHED":
        return "FINISHED";
      case "RUN_PENDING_CANCEL":
        return "PENDING_CANCEL";
      case "RETRY_QUEUED":
        return "QUEUED";
      case "RETRY_IMMEDIATELY":
        return "EXECUTING";
      default:
        assertExhaustive(attemptStatus);
    }
  }

  private measureExecutionMetrics({
    attemptCreatedAt,
    dequeuedAt,
    podScheduledAt,
  }: {
    attemptCreatedAt: number;
    dequeuedAt?: number;
    podScheduledAt?: number;
  }): TaskRunExecutionMetrics {
    const metrics: TaskRunExecutionMetrics = [
      {
        name: "start",
        event: "create_attempt",
        timestamp: attemptCreatedAt,
        duration: Date.now() - attemptCreatedAt,
      },
    ];

    if (dequeuedAt) {
      metrics.push({
        name: "start",
        event: "dequeue",
        timestamp: dequeuedAt,
        duration: 0,
      });
    }

    if (podScheduledAt) {
      metrics.push({
        name: "start",
        event: "pod_scheduled",
        timestamp: podScheduledAt,
        duration: 0,
      });
    }

    return metrics;
  }

  private async retryImmediately({ retryOpts }: { retryOpts: TaskRunExecutionRetry }) {
    this.sendDebugLog("retrying run immediately", {
      timestamp: retryOpts.timestamp,
      delay: retryOpts.delay,
    });

    const delay = retryOpts.timestamp - Date.now();

    if (delay > 0) {
      // Wait for retry delay to pass
      await sleep(delay);
    }

    // Start and execute next attempt
    const [startError, start] = await tryCatch(
      this.enableIgnoreSnapshotChanges(() => this.startAttempt({ isWarmStart: true }))
    );

    if (startError) {
      this.sendDebugLog("failed to start attempt for retry", { error: startError.message });

      this.shutdownExecution("retryImmediately: failed to start attempt");
      return;
    }

    const [executeError] = await tryCatch(
      this.executeRunWrapper({ ...start, isWarmStart: true, isImmediateRetry: true })
    );

    if (executeError) {
      this.sendDebugLog("failed to execute run for retry", { error: executeError.message });

      this.shutdownExecution("retryImmediately: failed to execute run");
      return;
    }
  }

  private async enableIgnoreSnapshotChanges<T>(fn: () => Promise<T>): Promise<T> {
    this.ignoreSnapshotChanges = true;
    try {
      return await fn();
    } finally {
      this.ignoreSnapshotChanges = false;
    }
  }

  /**
   * Restores a suspended execution from PENDING_EXECUTING
   */
  private async restore(): Promise<void> {
    this.logRestoreFlow("restore_enter");
    this.logRestoreFlow("starting_restore");

    let continueAttempted = false;
    const restoreStartedAt = Date.now();

    if (!this.runFriendlyId || !this.snapshotManager) {
      this.logRestoreFlow("continue_skipped", {
        reason: "restore_state_invalid",
        hasRunFriendlyId: Boolean(this.runFriendlyId),
        hasSnapshotManager: Boolean(this.snapshotManager),
        elapsedMs: Date.now() - restoreStartedAt,
      });
      throw new Error("Cannot restore: missing run or snapshot manager");
    }

    // Short delay to give websocket time to reconnect
    await sleep(100);

    // Reset the supervisor socket after checkpoint restore.
    // Socket transports don't reliably survive process snapshot/restore boundaries.
    try {
      this.supervisorSocket.disconnect();
    } catch (_) {
      // noop
    }
    this.supervisorSocket.connect();

    // Process any env overrides
    const envOverrideResult = await this.processEnvOverrides("restore");
    this.logRestoreFlow("env_overrides_processed", {
      hasOverrides: Boolean(envOverrideResult),
      runnerIdChanged: envOverrideResult?.runnerIdChanged ?? false,
      supervisorChanged: envOverrideResult?.supervisorChanged ?? false,
    });

    this.logRestoreFlow("network_ready_gate_start", {
      snapshotId: this.snapshotManager.snapshotId,
    });

    const networkReady = await this.waitForRestoreNetworkReady({
      runFriendlyId: this.runFriendlyId,
      snapshotId: this.snapshotManager.snapshotId,
    });
    this.logRestoreFlow("network_ready_gate_done", networkReady);
    if (!networkReady.ready) {
      this.logRestoreFlow("network_ready_gate_error", {
        snapshotId: this.snapshotManager.snapshotId,
        ...networkReady,
      });
    }

    const restoreAliveBaselineSeq = this.taskRunProcess?.latestIpcRestoreAliveSeq ?? -1;

    this.logRestoreFlow("calling_continue", {
      snapshotId: this.snapshotManager.snapshotId,
    });
    continueAttempted = true;
    const continueStartedAt = Date.now();
    const continuationResult = await this.httpClient.continueRunExecution(
      this.runFriendlyId,
      this.snapshotManager.snapshotId
    );

    if (!continuationResult.success) {
      // Check if we need to refresh metadata due to connection error
      if (continuationResult.isConnectionError) {
        this.sendDebugLog("restore: connection error detected, refreshing metadata");
        await this.processEnvOverrides("restore connection error");
        this.logRestoreFlow("retrying_continue_after_connection_error", {
          snapshotId: this.snapshotManager.snapshotId,
        });

        // Retry the continuation after refreshing metadata
        const retryResult = await this.httpClient.continueRunExecution(
          this.runFriendlyId,
          this.snapshotManager.snapshotId
        );

        if (!retryResult.success) {
          this.logRestoreFlow("continue_error", {
            mode: "retry",
            elapsedMs: Date.now() - continueStartedAt,
            snapshotId: this.snapshotManager.snapshotId,
            error: retryResult.error,
          });
          throw new Error(retryResult.error);
        }

        this.logRestoreFlow("continue_ok", {
          mode: "retry",
          elapsedMs: Date.now() - continueStartedAt,
          snapshotId: this.snapshotManager.snapshotId,
        });
      } else {
        this.logRestoreFlow("continue_error", {
          mode: "initial",
          elapsedMs: Date.now() - continueStartedAt,
          snapshotId: this.snapshotManager.snapshotId,
          error: continuationResult.error,
        });
        throw new Error(continuationResult.error);
      }
    } else {
      this.logRestoreFlow("continue_ok", {
        mode: "initial",
        elapsedMs: Date.now() - continueStartedAt,
        snapshotId: this.snapshotManager.snapshotId,
      });
    }

    if (this.taskRunProcess) {
      const restoreAliveTimeoutInMs = Number.parseInt(
        this.env.TRIGGER_IPC_RESTORE_ALIVE_TIMEOUT_MS ??
          process.env.TRIGGER_IPC_RESTORE_ALIVE_TIMEOUT_MS ??
          "2000",
        10
      );
      const restoreDirectionalProbeDurationInMs = Number.parseInt(
        this.env.TRIGGER_IPC_DIRECTIONAL_PROBE_DURATION_MS ??
          process.env.TRIGGER_IPC_DIRECTIONAL_PROBE_DURATION_MS ??
          "4000",
        10
      );
      const restoreDirectionalProbeIntervalInMs = Number.parseInt(
        this.env.TRIGGER_IPC_DIRECTIONAL_PROBE_INTERVAL_MS ??
          process.env.TRIGGER_IPC_DIRECTIONAL_PROBE_INTERVAL_MS ??
          "250",
        10
      );
      const restoreDirectionalPingTimeoutInMs = Number.parseInt(
        this.env.TRIGGER_IPC_DIRECTIONAL_PING_TIMEOUT_MS ??
          process.env.TRIGGER_IPC_DIRECTIONAL_PING_TIMEOUT_MS ??
          "750",
        10
      );
      const postRestoreResetAttempts = Number.parseInt(
        this.env.TRIGGER_IPC_POST_RESTORE_RESET_ATTEMPTS ??
          process.env.TRIGGER_IPC_POST_RESTORE_RESET_ATTEMPTS ??
          "3",
        10
      );
      const postRestoreResetTimeoutInMs = Number.parseInt(
        this.env.TRIGGER_IPC_POST_RESTORE_RESET_TIMEOUT_MS ??
          process.env.TRIGGER_IPC_POST_RESTORE_RESET_TIMEOUT_MS ??
          "2000",
        10
      );
      const postRestoreResetRetryDelayMs = Number.parseInt(
        this.env.TRIGGER_IPC_POST_RESTORE_RESET_RETRY_DELAY_MS ??
          process.env.TRIGGER_IPC_POST_RESTORE_RESET_RETRY_DELAY_MS ??
          "250",
        10
      );

      const reconnectAttempts = Number.isFinite(postRestoreResetAttempts)
        ? Math.max(1, postRestoreResetAttempts)
        : 3;
      const reconnectTimeoutInMs = Number.isFinite(postRestoreResetTimeoutInMs)
        ? Math.max(250, postRestoreResetTimeoutInMs)
        : 2_000;
      const reconnectRetryDelayMs = Number.isFinite(postRestoreResetRetryDelayMs)
        ? Math.max(50, postRestoreResetRetryDelayMs)
        : 250;
      let reconnectOk = false;
      let reconnectProbeSeq: number | undefined;
      let reconnectProbeRttMs: number | undefined;
      let reconnectLastError: string | undefined;

      for (let attempt = 1; attempt <= reconnectAttempts; attempt++) {
        const resetOk = await this.taskRunProcess.resetIpcConnection({
          reason: "post-restore-initial-reconnect",
          timeoutInMs: reconnectTimeoutInMs,
        });

        let probeOk = false;
        let probeError: string | undefined;

        if (resetOk) {
          const reconnectProbe = await this.taskRunProcess.probeIpcHealth(
            "post-restore-initial-reconnect",
            Math.min(1_500, reconnectTimeoutInMs),
            { allowDuringQuiesce: true }
          );
          probeOk = reconnectProbe.ok;
          reconnectProbeSeq = reconnectProbe.seq;
          reconnectProbeRttMs = reconnectProbe.rttMs;
          probeError = reconnectProbe.error;
        } else {
          probeError = "resetIpcConnection returned false";
        }

        this.logRestoreFlow("ipc_post_restore_reconnect_attempt", {
          attempt,
          reconnectAttempts,
          reconnectTimeoutInMs,
          reconnectRetryDelayMs,
          resetOk,
          probeOk,
          probeError,
          probeSeq: reconnectProbeSeq,
          probeRttMs: reconnectProbeRttMs,
        });

        if (resetOk && probeOk) {
          reconnectOk = true;
          break;
        }

        reconnectLastError = probeError ?? reconnectLastError;

        if (attempt < reconnectAttempts) {
          await new Promise((resolve) => setTimeout(resolve, reconnectRetryDelayMs));
        }
      }

      this.logRestoreFlow("ipc_post_restore_reconnect_summary", {
        reconnectOk,
        reconnectAttempts,
        reconnectTimeoutInMs,
        reconnectRetryDelayMs,
        reconnectProbeSeq,
        reconnectProbeRttMs,
        reconnectLastError,
      });

      if (!reconnectOk) {
        throw new Error(
          `Post-restore IPC reconnect failed after ${reconnectAttempts} attempts: ${reconnectLastError ?? "unknown error"}`
        );
      }

      const restoreAliveMinSeq = restoreAliveBaselineSeq + 1;
      this.logRestoreFlow("ipc_restore_alive_wait_start", {
        restoreAliveMinSeq,
        restoreAliveTimeoutInMs,
        restoreDirectionalProbeDurationInMs,
        restoreDirectionalProbeIntervalInMs,
        restoreDirectionalPingTimeoutInMs,
      });
      const restoreAliveResult = await this.taskRunProcess.waitForIpcRestoreAlive({
        minSeq: restoreAliveMinSeq,
        timeoutInMs: Number.isFinite(restoreAliveTimeoutInMs) ? restoreAliveTimeoutInMs : 2_000,
      });
      this.logRestoreFlow(
        restoreAliveResult.ok ? "ipc_restore_alive_wait_ok" : "ipc_restore_alive_wait_timeout",
        {
          restoreAliveMinSeq: restoreAliveResult.minSeq,
          restoreAliveLatestSeq: restoreAliveResult.latestSeq,
          restoreAliveElapsedMs: restoreAliveResult.elapsedMs,
          restoreAliveTimeoutInMs: restoreAliveResult.timeoutInMs,
          restoreAliveReason: restoreAliveResult.reason,
          restoreAlivePauseMs: restoreAliveResult.pauseMs,
          restoreAliveWorkerTimestamp: restoreAliveResult.workerTimestamp,
          restoreAliveWorkerPid: restoreAliveResult.workerPid,
        }
      );

      const directionalStartedAt = Date.now();
      const directionalDeadline =
        directionalStartedAt +
        (Number.isFinite(restoreDirectionalProbeDurationInMs)
          ? Math.max(250, restoreDirectionalProbeDurationInMs)
          : 4_000);
      const directionalIntervalMs = Number.isFinite(restoreDirectionalProbeIntervalInMs)
        ? Math.max(50, restoreDirectionalProbeIntervalInMs)
        : 250;
      const directionalPingTimeoutMs = Number.isFinite(restoreDirectionalPingTimeoutInMs)
        ? Math.max(100, restoreDirectionalPingTimeoutInMs)
        : 750;
      let directionalAttempts = 0;
      let childToParentOk = restoreAliveResult.ok;
      let parentToChildOk = false;
      let childToParentFirstElapsedMs = restoreAliveResult.ok ? restoreAliveResult.elapsedMs : undefined;
      let parentToChildFirstElapsedMs: number | undefined;
      let parentToChildSeq: number | undefined;
      let parentToChildRttMs: number | undefined;
      let parentToChildLastError: string | undefined;

      while (Date.now() < directionalDeadline && (!childToParentOk || !parentToChildOk)) {
        directionalAttempts++;

        if (!childToParentOk) {
          const latestAlive = this.taskRunProcess.latestIpcRestoreAlive;
          if (latestAlive.seq >= restoreAliveMinSeq) {
            childToParentOk = true;
            childToParentFirstElapsedMs = Date.now() - directionalStartedAt;
            this.logRestoreFlow("ipc_directional_probe_child_to_parent_ok", {
              restoreAliveMinSeq,
              restoreAliveLatestSeq: latestAlive.seq,
              restoreAliveReason: latestAlive.reason,
              restoreAlivePauseMs: latestAlive.pauseMs,
              restoreAliveWorkerTimestamp: latestAlive.workerTimestamp,
              restoreAliveWorkerPid: latestAlive.workerPid,
              directionalAttempts,
              elapsedMs: childToParentFirstElapsedMs,
            });
          }
        }

        if (!parentToChildOk) {
          const probe = await this.taskRunProcess.probeIpcHealth(
            "post-restore-directional-probe",
            directionalPingTimeoutMs,
            { allowDuringQuiesce: true }
          );
          parentToChildLastError = probe.error;
          if (probe.ok) {
            parentToChildOk = true;
            parentToChildSeq = probe.seq;
            parentToChildRttMs = probe.rttMs;
            parentToChildFirstElapsedMs = Date.now() - directionalStartedAt;
            this.logRestoreFlow("ipc_directional_probe_parent_to_child_ok", {
              probeSeq: probe.seq,
              probeRttMs: probe.rttMs,
              directionalAttempts,
              elapsedMs: parentToChildFirstElapsedMs,
            });
          }
        }

        if (!childToParentOk || !parentToChildOk) {
          await new Promise((resolve) => setTimeout(resolve, directionalIntervalMs));
        }
      }

      const directionalElapsedMs = Date.now() - directionalStartedAt;
      this.logRestoreFlow("ipc_directional_probe_summary", {
        restoreAliveMinSeq,
        directionalAttempts,
        directionalElapsedMs,
        directionalDurationBudgetInMs: directionalDeadline - directionalStartedAt,
        childToParentOk,
        childToParentFirstElapsedMs,
        parentToChildOk,
        parentToChildFirstElapsedMs,
        parentToChildSeq,
        parentToChildRttMs,
        parentToChildLastError,
      });

      if (!childToParentOk || !parentToChildOk) {
        this.logRestoreFlow("ipc_directional_probe_incomplete", {
          restoreAliveMinSeq,
          childToParentOk,
          parentToChildOk,
          childToParentFirstElapsedMs,
          parentToChildFirstElapsedMs,
          parentToChildLastError,
        });

        const directionalResetOk = await this.taskRunProcess.resetIpcConnection({
          reason: "post-restore-directional-probe-incomplete",
          timeoutInMs: 2_000,
        });

        this.logRestoreFlow("ipc_directional_probe_incomplete_reset", {
          directionalResetOk,
          childToParentOk,
          parentToChildOk,
        });

        if (!directionalResetOk) {
          throw new Error(
            `Directional IPC probe incomplete and reset failed: childToParentOk=${childToParentOk} parentToChildOk=${parentToChildOk} lastError=${parentToChildLastError ?? "unknown"}`
          );
        }

        const postResetRestoreAliveMinSeq = this.taskRunProcess.latestIpcRestoreAliveSeq + 1;
        const postResetRestoreAlive = await this.taskRunProcess.waitForIpcRestoreAlive({
          minSeq: postResetRestoreAliveMinSeq,
          timeoutInMs: Number.isFinite(restoreAliveTimeoutInMs)
            ? Math.max(500, restoreAliveTimeoutInMs)
            : 2_000,
        });

        const postResetProbe = await this.taskRunProcess.probeIpcHealth(
          "post-restore-directional-probe-after-reset",
          directionalPingTimeoutMs,
          { allowDuringQuiesce: true }
        );

        this.logRestoreFlow("ipc_directional_probe_after_reset_summary", {
          postResetRestoreAliveMinSeq,
          postResetChildToParentOk: postResetRestoreAlive.ok,
          postResetChildToParentLatestSeq: postResetRestoreAlive.latestSeq,
          postResetChildToParentReason: postResetRestoreAlive.reason,
          postResetChildToParentPauseMs: postResetRestoreAlive.pauseMs,
          postResetChildToParentWorkerTimestamp: postResetRestoreAlive.workerTimestamp,
          postResetParentToChildOk: postResetProbe.ok,
          postResetParentToChildSeq: postResetProbe.seq,
          postResetParentToChildRttMs: postResetProbe.rttMs,
          postResetParentToChildError: postResetProbe.error,
        });

        if (!postResetRestoreAlive.ok || !postResetProbe.ok) {
          throw new Error(
            `Directional IPC probe incomplete after reset: childToParentOk=${postResetRestoreAlive.ok} parentToChildOk=${postResetProbe.ok} parentToChildError=${postResetProbe.error ?? "unknown"}`
          );
        }
      }

      this.logRestoreFlow("ipc_probe_start", {
        context: "pre-quiesce-release",
      });

      let preQuiesceReleaseProbe = await this.taskRunProcess.probeIpcHealth(
        "pre-quiesce-release",
        directionalPingTimeoutMs,
        { allowDuringQuiesce: true }
      );

      this.logRestoreFlow(
        preQuiesceReleaseProbe.ok ? "ipc_probe_ok" : "ipc_probe_error",
        {
          context: "pre-quiesce-release",
          elapsedMs: Date.now() - directionalStartedAt,
          probeSeq: preQuiesceReleaseProbe.seq,
          probeRttMs: preQuiesceReleaseProbe.rttMs,
          probeError: preQuiesceReleaseProbe.error,
        }
      );

      if (!preQuiesceReleaseProbe.ok) {
        const resetOk = await this.taskRunProcess.resetIpcConnection({
          reason: "pre-quiesce-release-probe-failed",
          timeoutInMs: 2_000,
        });

        this.logRestoreFlow("ipc_reset_pre_quiesce_release", {
          resetOk,
        });

        if (!resetOk) {
          throw new Error(
            `Pre-quiesce release IPC probe failed and reset failed: ${preQuiesceReleaseProbe.error}`
          );
        }

        this.logRestoreFlow("ipc_probe_start", {
          context: "pre-quiesce-release-after-reset",
        });

        preQuiesceReleaseProbe = await this.taskRunProcess.probeIpcHealth(
          "pre-quiesce-release-after-reset",
          directionalPingTimeoutMs,
          { allowDuringQuiesce: true }
        );

        this.logRestoreFlow(
          preQuiesceReleaseProbe.ok ? "ipc_probe_ok" : "ipc_probe_error",
          {
            context: "pre-quiesce-release-after-reset",
            elapsedMs: Date.now() - directionalStartedAt,
            probeSeq: preQuiesceReleaseProbe.seq,
            probeRttMs: preQuiesceReleaseProbe.rttMs,
            probeError: preQuiesceReleaseProbe.error,
          }
        );

        if (!preQuiesceReleaseProbe.ok) {
          throw new Error(
            `Pre-quiesce release IPC still unhealthy after reset: ${preQuiesceReleaseProbe.error}`
          );
        }
      }

      const quiesceResult = await this.taskRunProcess.endIpcQuiesce();
      const allowIpcQuiesceTimeout =
        this.env.ALLOW_IPC_QUIESCE_TIMEOUT === "1" ||
        this.env.ALLOW_IPC_QUIESCE_TIMEOUT?.toLowerCase() === "true";

      this.logRestoreFlow("ipc_quiesce_released", {
        quiesceOk: quiesceResult.ok,
        quiesceAttempts: quiesceResult.attempts,
        quiesceTimeoutInMs: quiesceResult.timeoutInMs,
        quiesceRetryDelayMs: quiesceResult.retryDelayMs,
        quiesceSocketPath: quiesceResult.socketPath,
        quiesceLastError: quiesceResult.lastError,
      });

      if (!quiesceResult.ok && !allowIpcQuiesceTimeout) {
        throw new Error(
          `Failed to end IPC quiesce after ${quiesceResult.attempts} attempts: ${quiesceResult.lastError ?? "unknown error"}`
        );
      }

      if (!quiesceResult.ok && allowIpcQuiesceTimeout) {
        this.logRestoreFlow("ipc_quiesce_timeout_fallback", {
          quiesceAttempts: quiesceResult.attempts,
          quiesceTimeoutInMs: quiesceResult.timeoutInMs,
          quiesceRetryDelayMs: quiesceResult.retryDelayMs,
          quiesceSocketPath: quiesceResult.socketPath,
          quiesceLastError: quiesceResult.lastError,
          allowIpcQuiesceTimeout,
        });
      }

      this.logRestoreFlow("ipc_probe_start", {
        context: "post-restore-after-continue",
      });
      let probe = await this.taskRunProcess.probeIpcHealth("post-restore-after-continue");
      this.logRestoreFlow("ipc_probe_post_restore", {
        probeOk: probe.ok,
        probeSeq: probe.seq,
        probeRttMs: probe.rttMs,
        probeError: probe.error,
        parentProbeSent: probe.parentStats.sent,
        parentProbeAcked: probe.parentStats.acked,
        parentProbeFailed: probe.parentStats.failed,
        workerPingReceivedCount: probe.workerStats?.pingReceivedCount,
        workerTimestamp: probe.workerStats?.workerTimestamp,
      });
      this.logRestoreFlow(probe.ok ? "ipc_probe_ok" : "ipc_probe_error", {
        context: "post-restore-after-continue",
        elapsedMs: Date.now() - continueStartedAt,
        probeSeq: probe.seq,
        probeRttMs: probe.rttMs,
        probeError: probe.error,
      });

      if (!probe.ok) {
        const resetOk = await this.taskRunProcess.resetIpcConnection({
          reason: "post-restore-after-continue-probe-failed",
          timeoutInMs: 2_000,
        });
        this.logRestoreFlow("ipc_reset_post_restore", { resetOk });

        if (!resetOk) {
          throw new Error(`Post-restore IPC probe failed and reset failed: ${probe.error}`);
        }

        this.logRestoreFlow("ipc_probe_start", {
          context: "post-restore-after-reset",
        });
        probe = await this.taskRunProcess.probeIpcHealth("post-restore-after-reset");
        this.logRestoreFlow("ipc_probe_post_restore_after_reset", {
          probeOk: probe.ok,
          probeSeq: probe.seq,
          probeRttMs: probe.rttMs,
          probeError: probe.error,
          parentProbeSent: probe.parentStats.sent,
          parentProbeAcked: probe.parentStats.acked,
          parentProbeFailed: probe.parentStats.failed,
          workerPingReceivedCount: probe.workerStats?.pingReceivedCount,
          workerTimestamp: probe.workerStats?.workerTimestamp,
        });
        this.logRestoreFlow(probe.ok ? "ipc_probe_ok" : "ipc_probe_error", {
          context: "post-restore-after-reset",
          elapsedMs: Date.now() - continueStartedAt,
          probeSeq: probe.seq,
          probeRttMs: probe.rttMs,
          probeError: probe.error,
        });

        if (!probe.ok) {
          throw new Error(`Post-restore IPC still unhealthy after reset: ${probe.error}`);
        }
      }
    }

    // Track restore count
    this.restoreCount++;
    this.logRestoreFlow("execution_loop_resumed", {
      restoreCount: this.restoreCount,
      snapshotId: this.snapshotManager.snapshotId,
    });

    if (!continueAttempted) {
      this.logRestoreFlow("continue_skipped", {
        reason: "unexpected_branch",
        elapsedMs: Date.now() - restoreStartedAt,
      });
    }
  }

  private getRestoreNetworkReadyTimeoutMs(): number {
    const value = Number.parseInt(process.env.TRIGGER_RESTORE_NETWORK_READY_TIMEOUT_MS ?? "", 10);
    return Number.isFinite(value) && value > 0 ? value : RESTORE_NETWORK_READY_TIMEOUT_MS;
  }

  private getRestoreNetworkReadyPollMs(): number {
    const value = Number.parseInt(process.env.TRIGGER_RESTORE_NETWORK_READY_POLL_MS ?? "", 10);
    return Number.isFinite(value) && value > 0 ? value : RESTORE_NETWORK_READY_POLL_MS;
  }

  private async waitForRestoreNetworkReady(opts: {
    runFriendlyId: string;
    snapshotId: string;
  }): Promise<{
    ready: boolean;
    attempts: number;
    durationMs: number;
    timeoutMs: number;
    pollMs: number;
    lastError?: string;
  }> {
    const timeoutMs = this.getRestoreNetworkReadyTimeoutMs();
    const pollMs = this.getRestoreNetworkReadyPollMs();
    const startedAt = Date.now();
    const deadline = startedAt + timeoutMs;
    let attempts = 0;
    let lastError: string | undefined;

    while (Date.now() < deadline) {
      attempts++;
      const response = await this.httpClient.getSnapshotsSince(opts.runFriendlyId, opts.snapshotId);

      if (response.success) {
        return {
          ready: true,
          attempts,
          durationMs: Date.now() - startedAt,
          timeoutMs,
          pollMs,
        };
      }

      lastError = response.error;

      // Non-connection errors mean we reached supervisor and should not block restore here.
      if (!response.isConnectionError) {
        return {
          ready: true,
          attempts,
          durationMs: Date.now() - startedAt,
          timeoutMs,
          pollMs,
          lastError,
        };
      }

      this.logRestoreFlow("network_ready_gate_retry", {
        attempts,
        timeoutMs,
        pollMs,
        error: response.error,
      });

      const remainingMs = deadline - Date.now();
      if (remainingMs <= 0) {
        break;
      }

      await sleep(Math.min(pollMs, remainingMs));
    }

    return {
      ready: false,
      attempts,
      durationMs: Date.now() - startedAt,
      timeoutMs,
      pollMs,
      lastError,
    };
  }

  private async exitTaskRunProcessWithoutFailingRun({
    flush,
    reason,
  }: {
    flush: boolean;
    reason: string;
  }) {
    await this.taskRunProcessProvider.suspendProcess(flush, this.taskRunProcess);

    // No services should be left running after this line - let's make sure of it
    this.shutdownExecution(`exitTaskRunProcessWithoutFailingRun: ${reason}`);
  }

  /**
   * Processes env overrides from the metadata service. Generally called when we're resuming from a suspended state.
   */
  public async processEnvOverrides(
    reason?: string,
    shouldPollForSnapshotChanges?: boolean
  ): Promise<{
    overrides: Metadata;
    runnerIdChanged?: boolean;
    supervisorChanged?: boolean;
  } | null> {
    if (!this.metadataClient) {
      return null;
    }

    const previousRunnerId = this.env.TRIGGER_RUNNER_ID;
    const previousSupervisorUrl = this.env.TRIGGER_SUPERVISOR_API_URL;

    const [error, overrides] = await this.metadataClient.getEnvOverrides();

    if (error) {
      this.sendDebugLog("[override] failed to fetch", {
        reason,
        error: error.message,
      });
      return null;
    }

    if (overrides.TRIGGER_RUN_ID && overrides.TRIGGER_RUN_ID !== this.runFriendlyId) {
      this.sendDebugLog("[override] run ID mismatch, ignoring overrides", {
        reason,
        currentRunId: this.runFriendlyId,
        incomingRunId: overrides.TRIGGER_RUN_ID,
      });
      return null;
    }

    this.sendDebugLog(`[override] processing: ${reason}`, {
      overrides,
      currentEnv: this.env.raw,
    });

    // Override the env with the new values
    this.env.override(overrides);

    // Check if runner ID changed
    const newRunnerId = this.env.TRIGGER_RUNNER_ID;
    const runnerIdChanged = previousRunnerId !== newRunnerId;

    // Check if supervisor URL changed
    const newSupervisorUrl = this.env.TRIGGER_SUPERVISOR_API_URL;
    const supervisorChanged = previousSupervisorUrl !== newSupervisorUrl;

    // Update services with new values
    if (overrides.TRIGGER_SNAPSHOT_POLL_INTERVAL_SECONDS) {
      this.snapshotPoller?.updateInterval(this.env.TRIGGER_SNAPSHOT_POLL_INTERVAL_SECONDS * 1000);
    }
    if (
      overrides.TRIGGER_SUPERVISOR_API_PROTOCOL ||
      overrides.TRIGGER_SUPERVISOR_API_DOMAIN ||
      overrides.TRIGGER_SUPERVISOR_API_PORT
    ) {
      this.httpClient.updateApiUrl(this.env.TRIGGER_SUPERVISOR_API_URL);
    }
    if (overrides.TRIGGER_RUNNER_ID) {
      this.httpClient.updateRunnerId(this.env.TRIGGER_RUNNER_ID);
    }

    // Poll for snapshot changes immediately
    if (shouldPollForSnapshotChanges) {
      this.sendDebugLog("[override] polling for snapshot changes", { reason });
      this.fetchAndProcessSnapshotChanges("restore").catch(() => {});
    }

    return {
      overrides,
      runnerIdChanged,
      supervisorChanged,
    };
  }

  private async onHeartbeat() {
    if (!this.runFriendlyId) {
      this.sendDebugLog("heartbeat: missing run ID");
      return;
    }

    if (!this.snapshotManager) {
      this.sendDebugLog("heartbeat: missing snapshot manager");
      return;
    }

    this.sendDebugLog("heartbeat");

    const response = await this.httpClient.heartbeatRun(
      this.runFriendlyId,
      this.snapshotManager.snapshotId
    );

    if (!response.success) {
      this.sendDebugLog("heartbeat: failed", { error: response.error });

      // Check if we need to refresh metadata due to connection error
      if (response.isConnectionError) {
        this.sendDebugLog("heartbeat: connection error detected, refreshing metadata");
        await this.processEnvOverrides("heartbeat connection error");
      }
    }

    this.lastHeartbeat = new Date();
  }

  private sendDebugLog(
    message: string,
    properties?: SendDebugLogOptions["properties"],
    runIdOverride?: string
  ) {
    this.logger.sendDebugLog({
      runId: runIdOverride ?? this.runFriendlyId,
      message: `[execution] ${message}`,
      properties: {
        ...properties,
        runId: this.runFriendlyId,
        snapshotId: this.currentSnapshotFriendlyId,
        executionId: this.id,
        executionRestoreCount: this.restoreCount,
        lastHeartbeat: this.lastHeartbeat?.toISOString(),
      },
    });
  }

  private sendRuntimeDebugLog(
    message: string,
    properties?: SendDebugLogOptions["properties"],
    runIdOverride?: string
  ) {
    this.logger.sendDebugLog({
      runId: runIdOverride ?? this.runFriendlyId,
      message: `[runtime] ${message}`,
      print: false,
      properties: {
        ...properties,
        runId: this.runFriendlyId,
        snapshotId: this.currentSnapshotFriendlyId,
        executionId: this.id,
        executionRestoreCount: this.restoreCount,
        lastHeartbeat: this.lastHeartbeat?.toISOString(),
      },
    });
  }

  private logRestoreFlow(message: string, properties?: SendDebugLogOptions["properties"]) {
    this.sendDebugLog(`[restore-flow] ${message}`, properties);
  }

  private set suspendable(suspendable: boolean) {
    this.snapshotManager?.setSuspendable(suspendable).catch((error) => {
      this.sendDebugLog("failed to set suspendable", { error: error.message });
    });
  }

  // Ensure we can only set this once
  private set runFriendlyId(id: string) {
    if (this._runFriendlyId) {
      throw new Error("Run ID already set");
    }

    this._runFriendlyId = id;
  }

  public get runFriendlyId(): string | undefined {
    return this._runFriendlyId;
  }

  public get currentSnapshotFriendlyId(): string | undefined {
    return this.snapshotManager?.snapshotId;
  }

  public get taskRunEnv(): Record<string, string> | undefined {
    return this.currentTaskRunEnv;
  }

  public get metrics() {
    return {
      execution: {
        restoreCount: this.restoreCount,
        lastHeartbeat: this.lastHeartbeat,
      },
      poller: this.snapshotPoller?.metrics,
      notifier: this.notifier?.metrics,
    };
  }

  get isAborted() {
    return this.executionAbortController.signal.aborted;
  }

  private abortExecution() {
    if (this.isAborted) {
      this.sendDebugLog("execution already aborted");
      return;
    }

    this.executionAbortController.abort();
    this.shutdownExecution("abortExecution");
  }

  private shutdownExecution(reason: string) {
    if (this.isShuttingDown) {
      this.sendDebugLog(`[shutdown] ${reason} (already shutting down)`, {
        firstShutdownReason: this.shutdownReason,
      });
      return;
    }

    this.sendDebugLog(`[shutdown] ${reason}`);

    this.isShuttingDown = true;
    this.shutdownReason = reason;

    this.snapshotPoller?.stop();
    this.snapshotManager?.stop();
    this.notifier?.stop();

    this.taskRunProcess?.unsafeDetachEvtHandlers();
  }

  private async handleSuspendable(suspendableSnapshot: SnapshotState) {
    this.sendDebugLog("handleSuspendable", { suspendableSnapshot });

    if (!this.snapshotManager) {
      this.sendDebugLog("handleSuspendable: missing snapshot manager", { suspendableSnapshot });
      return;
    }

    // Ensure this is the current snapshot
    if (suspendableSnapshot.id !== this.currentSnapshotFriendlyId) {
      this.sendDebugLog("snapshot changed before cleanup, abort", {
        suspendableSnapshot,
        currentSnapshotId: this.currentSnapshotFriendlyId,
      });
      this.abortExecution();
      return;
    }

    if (this.taskRunProcess) {
      const quiesce = await this.taskRunProcess.beginIpcQuiesce();
      this.sendDebugLog("handleSuspendable: ipc_quiesce_ready", {
        suspendableSnapshot,
        quiesceOk: quiesce.ok,
        quiesceTimedOut: quiesce.timedOut,
        quiescePendingCount: quiesce.pendingCount,
        quiesceDurationMs: quiesce.durationMs,
        quiesceChildAcked: quiesce.childAcked,
        quiesceChildInFlightHandlers: quiesce.childInFlightHandlers,
        quiesceChildInFlightSends: quiesce.childInFlightSends,
        quiesceChildQuietForMs: quiesce.childQuietForMs,
        quiesceChildError: quiesce.childError,
      });
    }

    // First cleanup the task run process
    const [error] = await tryCatch(this.taskRunProcess?.cleanup(false));

    if (error) {
      this.sendDebugLog("failed to cleanup task run process, carrying on", {
        suspendableSnapshot,
        error: error.message,
      });
    }

    // Double check snapshot hasn't changed after cleanup
    if (suspendableSnapshot.id !== this.currentSnapshotFriendlyId) {
      this.sendDebugLog("snapshot changed after cleanup, abort", {
        suspendableSnapshot,
        currentSnapshotId: this.currentSnapshotFriendlyId,
      });
      this.abortExecution();
      return;
    }

    if (!this.runFriendlyId) {
      this.sendDebugLog("missing run ID for suspension, abort", { suspendableSnapshot });
      this.abortExecution();
      return;
    }

    // Call the suspend API with the current snapshot ID
    const suspendResult = await this.httpClient.suspendRun(
      this.runFriendlyId,
      suspendableSnapshot.id
    );

    if (!suspendResult.success) {
      this.sendDebugLog("suspension request failed, staying alive 🎶", {
        suspendableSnapshot,
        error: suspendResult.error,
      });

      // This is fine, we'll wait for the next status change
      return;
    }

    if (!suspendResult.data.ok) {
      this.sendDebugLog("suspension request returned error, staying alive 🎶", {
        suspendableSnapshot,
        error: suspendResult.data.error,
      });

      // This is fine, we'll wait for the next status change
      return;
    }

    this.sendDebugLog("suspending, any day now 🚬", { suspendableSnapshot });

    // Disconnect before snapshotting so the restored process does a fresh socket handshake.
    // This avoids carrying a stale websocket transport through checkpoint/restore.
    this.supervisorSocket.disconnect();
  }

  /**
   * Fetches the latest execution data and enqueues snapshot changes. Used by both poller and notification handlers.
   * @param source string - where this call originated (e.g. 'poller', 'notification')
   */
  public async fetchAndProcessSnapshotChanges(source: string): Promise<void> {
    if (!this.runFriendlyId) {
      this.sendDebugLog(`fetchAndProcessSnapshotChanges: missing runFriendlyId`, { source });
      return;
    }

    // Use the last processed snapshot as the since parameter
    const sinceSnapshotId = this.currentSnapshotFriendlyId;

    if (!sinceSnapshotId) {
      this.sendDebugLog(`fetchAndProcessSnapshotChanges: missing sinceSnapshotId`, { source });
      return;
    }

    let response = await this.httpClient.getSnapshotsSince(this.runFriendlyId, sinceSnapshotId);

    // A failed first post-restore poll can block /continue until the next 30s tick.
    // Retry quickly for connection errors to keep restore flow moving.
    if (!response.success && response.isConnectionError) {
      for (let i = 0; i < SNAPSHOTS_SINCE_RETRY_DELAYS_MS.length; i++) {
        const delayMs = SNAPSHOTS_SINCE_RETRY_DELAYS_MS[i];
        this.sendDebugLog(`fetchAndProcessSnapshotChanges: retrying get snapshots since`, {
          source,
          attempt: i + 1,
          maxAttempts: SNAPSHOTS_SINCE_RETRY_DELAYS_MS.length,
          delayMs,
          sinceSnapshotId,
          error: response.error,
        });

        await sleep(delayMs);
        response = await this.httpClient.getSnapshotsSince(this.runFriendlyId, sinceSnapshotId);

        if (response.success || !response.isConnectionError) {
          break;
        }
      }
    }

    if (!response.success) {
      this.sendDebugLog(`fetchAndProcessSnapshotChanges: failed to get snapshots since`, {
        source,
        error: response.error,
      });
      if (source === "restore" || this.restoreCount > 0) {
        this.logRestoreFlow("snapshots_since_failed", {
          source,
          sinceSnapshotId,
          error: response.error,
        });
      }

      if (response.isConnectionError) {
        // Log this separately to make it more visible
        this.sendDebugLog(
          "fetchAndProcessSnapshotChanges: connection error detected, refreshing metadata"
        );
      }

      // Always trigger metadata refresh on snapshot fetch errors
      await this.processEnvOverrides("snapshots since error");
      return;
    }

    const { snapshots } = response.data;
    if (source === "restore" || this.restoreCount > 0) {
      this.logRestoreFlow("got_snapshots_since", {
        source,
        sinceSnapshotId,
        snapshotCount: snapshots.length,
      });
    }

    if (!snapshots.length) {
      return;
    }

    const [error] = await tryCatch(this.enqueueSnapshotChangesAndWait(snapshots));

    if (error) {
      this.sendDebugLog(
        `fetchAndProcessSnapshotChanges: failed to enqueue and process snapshot change`,
        {
          source,
          error: error.message,
        }
      );
      return;
    }
  }
}
