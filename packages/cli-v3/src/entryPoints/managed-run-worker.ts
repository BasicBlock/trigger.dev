import type { Tracer } from "@opentelemetry/api";
import type { Logger } from "@opentelemetry/api-logs";
import {
  AnyOnCatchErrorHookFunction,
  AnyOnFailureHookFunction,
  AnyOnInitHookFunction,
  AnyOnStartHookFunction,
  AnyOnSuccessHookFunction,
  apiClientManager,
  clock,
  ExecutorToWorkerMessageCatalog,
  type HandleErrorFunction,
  lifecycleHooks,
  localsAPI,
  logger,
  LogLevel,
  OTEL_LOG_ATTRIBUTE_COUNT_LIMIT,
  resourceCatalog,
  runMetadata,
  runtime,
  runTimelineMetrics,
  taskContext,
  TaskRunContext,
  TaskRunErrorCodes,
  TaskRunExecution,
  timeout,
  TriggerConfig,
  UsageMeasurement,
  waitUntil,
  WorkerManifest,
  WorkerToExecutorMessageCatalog,
  traceContext,
  heartbeats,
  realtimeStreams,
} from "@basicblock/trigger-core/v3";
import { TriggerTracer } from "@basicblock/trigger-core/v3/tracer";
import {
  ConsoleInterceptor,
  DevUsageManager,
  DurableClock,
  getEnvVar,
  getNumberEnvVar,
  logLevels,
  SharedRuntimeManager,
  OtelTaskLogger,
  populateEnv,
  ProdUsageManager,
  StandardLifecycleHooksManager,
  StandardLocalsManager,
  StandardMetadataManager,
  StandardResourceCatalog,
  StandardRunTimelineMetricsManager,
  StandardWaitUntilManager,
  TaskExecutor,
  TracingDiagnosticLogLevel,
  TracingSDK,
  usage,
  UsageTimeoutManager,
  StandardTraceContextManager,
  StandardHeartbeatsManager,
  StandardRealtimeStreamsManager,
} from "@basicblock/trigger-core/v3/workers";
import { ZodIpcConnection } from "@basicblock/trigger-core/v3/zodIpc";
import { readFile } from "node:fs/promises";
import { setInterval, setTimeout } from "node:timers/promises";
import sourceMapSupport from "source-map-support";
import { env } from "std-env";
import { normalizeImportPath } from "../utilities/normalizeImportPath.js";
import { VERSION } from "../version.js";
import { promiseWithResolvers } from "@basicblock/trigger-core/utils";
import { createChildSocketIpcProcess } from "../executions/localSocketIpc.js";

sourceMapSupport.install({
  handleUncaughtExceptions: false,
  environment: "node",
  hookRequire: false,
});

const ipcProcess =
  process.env.TRIGGER_IPC_TRANSPORT === "socket" && process.env.TRIGGER_IPC_SOCKET_PATH
    ? createChildSocketIpcProcess(process.env.TRIGGER_IPC_SOCKET_PATH)
    : process;

function hasResetConnection(
  value: typeof ipcProcess
): value is typeof ipcProcess & { resetConnection: (timeoutInMs?: number) => Promise<void> } {
  return "resetConnection" in value && typeof value.resetConnection === "function";
}

function parseEnvFile(contents: string): Record<string, string> {
  return contents.split(/\r?\n/).reduce(
    (acc, line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) {
        return acc;
      }

      const delimiterIndex = trimmed.indexOf("=");
      if (delimiterIndex === -1) {
        return acc;
      }

      const key = trimmed.slice(0, delimiterIndex).trim();
      const value = trimmed.slice(delimiterIndex + 1).trim();

      if (!key) {
        return acc;
      }

      const unquotedValue =
        (value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))
          ? value.slice(1, -1)
          : value;

      acc[key] = unquotedValue;
      return acc;
    },
    {} as Record<string, string>
  );
}

async function loadMountedEnvFile() {
  const mountedEnvPath = process.env.TRIGGER_MOUNTED_ENV_FILE ?? process.env.DOPPLER_SECRETS_FILE;
  if (!mountedEnvPath) {
    return;
  }

  try {
    const envContents = await readFile(mountedEnvPath, "utf8");
    const parsedEnv = parseEnvFile(envContents);

    for (const [key, value] of Object.entries(parsedEnv)) {
      // Keep explicit process env values intact (including Trigger-managed vars).
      if (process.env[key] === undefined) {
        process.env[key] = value;
      }
    }
  } catch (error) {
    console.error("Failed to load mounted env file", {
      mountedEnvPath,
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

process.on("uncaughtException", function (error, origin) {
  console.error("Uncaught exception", { error, origin });
  if (error instanceof Error) {
    ipcProcess.send &&
      ipcProcess.send({
        type: "EVENT",
        message: {
          type: "UNCAUGHT_EXCEPTION",
          payload: {
            error: { name: error.name, message: error.message, stack: error.stack },
            origin,
          },
          version: "v1",
        },
      });
  } else {
    ipcProcess.send &&
      ipcProcess.send({
        type: "EVENT",
        message: {
          type: "UNCAUGHT_EXCEPTION",
          payload: {
            error: {
              name: "Error",
              message: typeof error === "string" ? error : JSON.stringify(error),
            },
            origin,
          },
          version: "v1",
        },
      });
  }
});

await loadMountedEnvFile();

const heartbeatIntervalMs = getEnvVar("HEARTBEAT_INTERVAL_MS");

const standardLocalsManager = new StandardLocalsManager();
localsAPI.setGlobalLocalsManager(standardLocalsManager);

const standardLifecycleHooksManager = new StandardLifecycleHooksManager();
lifecycleHooks.setGlobalLifecycleHooksManager(standardLifecycleHooksManager);

const standardRunTimelineMetricsManager = new StandardRunTimelineMetricsManager();
runTimelineMetrics.setGlobalManager(standardRunTimelineMetricsManager);

const standardResourceCatalog = new StandardResourceCatalog();
resourceCatalog.setGlobalResourceCatalog(standardResourceCatalog);

const durableClock = new DurableClock();
clock.setGlobalClock(durableClock);

const standardTraceContextManager = new StandardTraceContextManager();
traceContext.setGlobalManager(standardTraceContextManager);

const runMetadataManager = new StandardMetadataManager(apiClientManager.clientOrThrow());
runMetadata.setGlobalManager(runMetadataManager);

const standardRealtimeStreamsManager = new StandardRealtimeStreamsManager(
  apiClientManager.clientOrThrow(),
  getEnvVar("TRIGGER_STREAM_URL", getEnvVar("TRIGGER_API_URL")) ?? "https://api.trigger.dev",
  (getEnvVar("TRIGGER_STREAMS_DEBUG") === "1" || getEnvVar("TRIGGER_STREAMS_DEBUG") === "true") ??
    false
);
realtimeStreams.setGlobalManager(standardRealtimeStreamsManager);

const waitUntilTimeoutInMs = getNumberEnvVar("TRIGGER_WAIT_UNTIL_TIMEOUT_MS", 60_000);
const waitUntilManager = new StandardWaitUntilManager(waitUntilTimeoutInMs);
waitUntil.setGlobalManager(waitUntilManager);

const standardHeartbeatsManager = new StandardHeartbeatsManager(
  parseInt(heartbeatIntervalMs ?? "30000", 10)
);
heartbeats.setGlobalManager(standardHeartbeatsManager);

const triggerLogLevel = getEnvVar("TRIGGER_LOG_LEVEL");

async function importConfig(
  configPath: string
): Promise<{ config: TriggerConfig; handleError?: HandleErrorFunction }> {
  const configModule = await import(configPath);

  const config = configModule?.default ?? configModule?.config;

  return {
    config,
    handleError: configModule?.handleError,
  };
}

async function loadWorkerManifest() {
  const manifestContents = await readFile("./index.json", "utf-8");
  const raw = JSON.parse(manifestContents);

  return WorkerManifest.parse(raw);
}

async function doBootstrap() {
  return await runTimelineMetrics.measureMetric("trigger.dev/start", "bootstrap", {}, async () => {
    const workerManifest = await loadWorkerManifest();

    resourceCatalog.registerWorkerManifest(workerManifest);

    const { config, handleError } = await importConfig(
      normalizeImportPath(workerManifest.configPath)
    );

    const tracingSDK = new TracingSDK({
      url: env.TRIGGER_OTEL_EXPORTER_OTLP_ENDPOINT ?? "http://0.0.0.0:4318",
      instrumentations: config.instrumentations ?? [],
      diagLogLevel: (env.TRIGGER_OTEL_LOG_LEVEL as TracingDiagnosticLogLevel) ?? "none",
      forceFlushTimeoutMillis: 30_000,
      exporters: config.telemetry?.exporters ?? [],
      logExporters: config.telemetry?.logExporters ?? [],
      resource: config.telemetry?.resource,
    });

    const otelTracer: Tracer = tracingSDK.getTracer("trigger-dev-worker", VERSION);
    const otelLogger: Logger = tracingSDK.getLogger("trigger-dev-worker", VERSION);

    const tracer = new TriggerTracer({ tracer: otelTracer, logger: otelLogger });
    const consoleInterceptor = new ConsoleInterceptor(
      otelLogger,
      typeof config.enableConsoleLogging === "boolean" ? config.enableConsoleLogging : true,
      typeof config.disableConsoleInterceptor === "boolean"
        ? config.disableConsoleInterceptor
        : false,
      OTEL_LOG_ATTRIBUTE_COUNT_LIMIT
    );

    const configLogLevel = triggerLogLevel ?? config.logLevel ?? "info";

    const otelTaskLogger = new OtelTaskLogger({
      logger: otelLogger,
      tracer: tracer,
      level: logLevels.includes(configLogLevel as any) ? (configLogLevel as LogLevel) : "info",
      maxAttributeCount: OTEL_LOG_ATTRIBUTE_COUNT_LIMIT,
    });

    logger.setGlobalTaskLogger(otelTaskLogger);

    if (config.init) {
      lifecycleHooks.registerGlobalInitHook({
        id: "config",
        fn: config.init as AnyOnInitHookFunction,
      });
    }

    if (config.onStart) {
      lifecycleHooks.registerGlobalStartHook({
        id: "config",
        fn: config.onStart as AnyOnStartHookFunction,
      });
    }

    if (config.onSuccess) {
      lifecycleHooks.registerGlobalSuccessHook({
        id: "config",
        fn: config.onSuccess as AnyOnSuccessHookFunction,
      });
    }

    if (config.onFailure) {
      lifecycleHooks.registerGlobalFailureHook({
        id: "config",
        fn: config.onFailure as AnyOnFailureHookFunction,
      });
    }

    if (handleError) {
      lifecycleHooks.registerGlobalCatchErrorHook({
        id: "config",
        fn: handleError as AnyOnCatchErrorHookFunction,
      });
    }

    return {
      tracer,
      tracingSDK,
      consoleInterceptor,
      config,
      workerManifest,
    };
  });
}

let bootstrapCache:
  | {
      tracer: TriggerTracer;
      tracingSDK: TracingSDK;
      consoleInterceptor: ConsoleInterceptor;
      config: TriggerConfig;
      workerManifest: WorkerManifest;
    }
  | undefined;

async function bootstrap() {
  if (!bootstrapCache) {
    bootstrapCache = await doBootstrap();
  }

  return bootstrapCache;
}

let _execution: TaskRunExecution | undefined;
let _isRunning = false;
let _isCancelled = false;
let _tracingSDK: TracingSDK | undefined;
let _executionMeasurement: UsageMeasurement | undefined;
let _cancelController = new AbortController();
let _lastFlushPromise: Promise<void> | undefined;
let _sharedWorkerRuntime: SharedRuntimeManager | undefined;
let _ipcPingReceivedCount = 0;
let _isIpcQuiescing = false;
let _ipcInFlightSends = 0;
let _ipcLastActivityAt = Date.now();
let _ipcRestoreAliveSeq = 0;
let _ipcRestoreAliveSentInCurrentQuiesce = false;
let _ipcRestoreReconnectAttemptedInCurrentQuiesce = false;
let _ipcRestoreAliveTickAt = Date.now();

function markIpcActivity() {
  _ipcLastActivityAt = Date.now();
}

async function waitForIpcQuietWindow(timeoutInMs: number, quietPeriodInMs: number): Promise<number> {
  const startedAt = Date.now();
  const deadline = startedAt + timeoutInMs;

  while (Date.now() < deadline) {
    const now = Date.now();
    const quietForMs = now - _ipcLastActivityAt;

    if (_ipcInFlightSends === 0 && quietForMs >= quietPeriodInMs) {
      return quietForMs;
    }

    await setTimeout(25);
  }

  return Math.max(0, Date.now() - _ipcLastActivityAt);
}

function resetExecutionEnvironment() {
  _execution = undefined;
  _isRunning = false;
  _isCancelled = false;
  _executionMeasurement = undefined;
  _cancelController = new AbortController();

  standardLocalsManager.reset();
  standardLifecycleHooksManager.reset();
  standardRunTimelineMetricsManager.reset();
  usage.reset();
  timeout.reset();
  runMetadataManager.reset();
  waitUntilManager.reset();
  standardRealtimeStreamsManager.reset();
  _sharedWorkerRuntime?.reset();
  durableClock.reset();
  taskContext.disable();
  standardTraceContextManager.reset();
  standardHeartbeatsManager.reset();

  // Wait for all streams to finish before completing the run
  waitUntil.register({
    requiresResolving: () => standardRealtimeStreamsManager.hasActiveStreams(),
    promise: (timeoutInMs) => standardRealtimeStreamsManager.waitForAllStreams(timeoutInMs),
  });

  console.log(`[${new Date().toISOString()}] Reset execution environment`);
}

let _lastEnv: Record<string, string> | undefined;
let _executionCount = 0;

const zodIpc = new ZodIpcConnection({
  listenSchema: WorkerToExecutorMessageCatalog,
  emitSchema: ExecutorToWorkerMessageCatalog,
  process: ipcProcess,
  handlers: {
    EXECUTE_TASK_RUN: async (
      { execution, traceContext, metadata, metrics, env, isWarmStart },
      sender
    ) => {
      markIpcActivity();
      if (env) {
        populateEnv(env, {
          override: true,
          previousEnv: _lastEnv,
        });

        _lastEnv = env;
      }

      console.log(
        `[${new Date().toISOString()}] Received EXECUTE_TASK_RUN isWarmStart ${String(isWarmStart)}`
      );

      if (_lastFlushPromise) {
        const now = performance.now();

        await _lastFlushPromise;

        const duration = performance.now() - now;

        console.log(`[${new Date().toISOString()}] Awaited last flush in ${duration}ms`);
      }

      resetExecutionEnvironment();

      standardTraceContextManager.traceContext = traceContext;

      const prodManager = initializeUsageManager({
        usageIntervalMs: getEnvVar("USAGE_HEARTBEAT_INTERVAL_MS"),
        usageEventUrl: getEnvVar("USAGE_EVENT_URL"),
        triggerJWT: getEnvVar("TRIGGER_JWT"),
      });

      prodManager.setInitialState({
        cpuTime: execution.run.durationMs ?? 0,
        costInCents: execution.run.costInCents ?? 0,
      });

      standardRunTimelineMetricsManager.registerMetricsFromExecution(metrics, isWarmStart);

      console.log(`[${new Date().toISOString()}] Received EXECUTE_TASK_RUN`, execution);

      if (_isRunning) {
        console.error("Worker is already running a task");

        await sender.send("TASK_RUN_COMPLETED", {
          execution,
          result: {
            ok: false,
            id: execution.run.id,
            error: {
              type: "INTERNAL_ERROR",
              code: TaskRunErrorCodes.TASK_ALREADY_RUNNING,
            },
            usage: {
              durationMs: 0,
            },
            flushedMetadata: await runMetadataManager.stopAndReturnLastFlush(),
          },
        });

        return;
      }

      const ctx = TaskRunContext.parse(execution);

      taskContext.setGlobalTaskContext({
        ctx,
        worker: metadata,
        isWarmStart: isWarmStart ?? false,
      });

      try {
        const { tracer, tracingSDK, consoleInterceptor, config, workerManifest } =
          await bootstrap();

        _tracingSDK = tracingSDK;

        const taskManifest = workerManifest.tasks.find((t) => t.id === execution.task.id);

        if (!taskManifest) {
          console.error(`Could not find task ${execution.task.id}`);

          await sender.send("TASK_RUN_COMPLETED", {
            execution,
            result: {
              ok: false,
              id: execution.run.id,
              error: {
                type: "INTERNAL_ERROR",
                code: TaskRunErrorCodes.COULD_NOT_FIND_TASK,
                message: `Could not find task ${execution.task.id}. Make sure the task is exported and the ID is correct.`,
              },
              usage: {
                durationMs: 0,
              },
              flushedMetadata: await runMetadataManager.stopAndReturnLastFlush(),
            },
          });

          return;
        }

        // Import the task module
        let task = resourceCatalog.getTask(execution.task.id);

        if (!task) {
          try {
            await runTimelineMetrics.measureMetric(
              "trigger.dev/start",
              "import",
              {
                entryPoint: taskManifest.entryPoint,
                file: taskManifest.filePath,
              },
              async () => {
                const beforeImport = performance.now();
                resourceCatalog.setCurrentFileContext(
                  taskManifest.entryPoint,
                  taskManifest.filePath
                );

                // Load init file if it exists
                if (workerManifest.initEntryPoint) {
                  try {
                    await import(normalizeImportPath(workerManifest.initEntryPoint));
                    console.log(`Loaded init file from ${workerManifest.initEntryPoint}`);
                  } catch (err) {
                    console.error(`Failed to load init file`, err);
                    throw err;
                  }
                }

                await import(normalizeImportPath(taskManifest.entryPoint));
                resourceCatalog.clearCurrentFileContext();
                const durationMs = performance.now() - beforeImport;

                console.log(
                  `Imported task ${execution.task.id} [${taskManifest.entryPoint}] in ${durationMs}ms`
                );
              }
            );
          } catch (err) {
            console.error(`Failed to import task ${execution.task.id}`, err);

            await sender.send("TASK_RUN_COMPLETED", {
              execution,
              result: {
                ok: false,
                id: execution.run.id,
                error: {
                  type: "INTERNAL_ERROR",
                  code: TaskRunErrorCodes.COULD_NOT_IMPORT_TASK,
                  message: err instanceof Error ? err.message : String(err),
                  stackTrace: err instanceof Error ? err.stack : undefined,
                },
                usage: {
                  durationMs: 0,
                },
                flushedMetadata: await runMetadataManager.stopAndReturnLastFlush(),
              },
            });

            return;
          }

          process.title = `trigger-dev-worker: ${execution.task.id} ${execution.run.id}`;

          // Now try and get the task again
          task = resourceCatalog.getTask(execution.task.id);
        }

        if (!task) {
          console.error(`Could not find task ${execution.task.id}`);

          await sender.send("TASK_RUN_COMPLETED", {
            execution,
            result: {
              ok: false,
              id: execution.run.id,
              error: {
                type: "INTERNAL_ERROR",
                code: TaskRunErrorCodes.COULD_NOT_FIND_EXECUTOR,
              },
              usage: {
                durationMs: 0,
              },
              flushedMetadata: await runMetadataManager.stopAndReturnLastFlush(),
            },
          });

          return;
        }

        runMetadataManager.runId = execution.run.id;
        runMetadataManager.runIdIsRoot = typeof execution.run.rootTaskRunId === "undefined";
        _executionCount++;

        const executor = new TaskExecutor(task, {
          tracer,
          tracingSDK,
          consoleInterceptor,
          retries: config.retries,
          isWarmStart,
          executionCount: _executionCount,
        });

        try {
          _execution = execution;
          _isRunning = true;

          standardHeartbeatsManager.startHeartbeat(_execution.run.id);

          runMetadataManager.startPeriodicFlush(
            getNumberEnvVar("TRIGGER_RUN_METADATA_FLUSH_INTERVAL", 1000)
          );

          _executionMeasurement = usage.start();

          const timeoutController = timeout.abortAfterTimeout(execution.run.maxDuration);

          const signal = AbortSignal.any([_cancelController.signal, timeoutController.signal]);

          const { result } = await executor.execute(execution, ctx, signal);

          if (_isRunning && !_isCancelled) {
            const usageSample = usage.stop(_executionMeasurement);

            return sender.send("TASK_RUN_COMPLETED", {
              execution,
              result: {
                ...result,
                usage: {
                  durationMs: usageSample.cpuTime,
                },
                flushedMetadata: await runMetadataManager.stopAndReturnLastFlush(),
              },
            });
          }
        } finally {
          standardHeartbeatsManager.stopHeartbeat();

          _execution = undefined;
          _isRunning = false;

          console.log(`[${new Date().toISOString()}] Task run completed`);
        }
      } catch (err) {
        console.error("Failed to execute task", err);

        await sender.send("TASK_RUN_COMPLETED", {
          execution,
          result: {
            ok: false,
            id: execution.run.id,
            error: {
              type: "INTERNAL_ERROR",
              code: TaskRunErrorCodes.CONFIGURED_INCORRECTLY,
              message: err instanceof Error ? err.message : String(err),
              stackTrace: err instanceof Error ? err.stack : undefined,
            },
            usage: {
              durationMs: 0,
            },
            flushedMetadata: await runMetadataManager.stopAndReturnLastFlush(),
          },
        });
      }
    },
    CANCEL: async ({ timeoutInMs }) => {
      markIpcActivity();
      _isCancelled = true;
      _cancelController.abort("run cancelled");
      await callCancelHooks(timeoutInMs);
      if (_executionMeasurement) {
        usage.stop(_executionMeasurement);
      }
      await flushAll(timeoutInMs);
    },
    FLUSH: async ({ timeoutInMs }) => {
      markIpcActivity();
      await flushAll(timeoutInMs);
    },
    RESOLVE_WAITPOINT: async ({ waitpoint }) => {
      markIpcActivity();
      if (_isIpcQuiescing) {
        return { status: "ok" as const };
      }
      _sharedWorkerRuntime?.resolveWaitpoints([waitpoint]);
      return { status: "ok" as const };
    },
    IPC_PING: async ({ seq }) => {
      markIpcActivity();
      _ipcPingReceivedCount++;
      return {
        status: "ok" as const,
        seq,
        workerTimestamp: new Date().toISOString(),
        pingReceivedCount: _ipcPingReceivedCount,
      };
    },
    IPC_QUIESCE_BEGIN: async ({ timeoutInMs, quietPeriodInMs }) => {
      markIpcActivity();
      _isIpcQuiescing = true;
      _ipcRestoreAliveSentInCurrentQuiesce = false;
      _ipcRestoreReconnectAttemptedInCurrentQuiesce = false;
      _ipcRestoreAliveTickAt = Date.now();

      const workerQuietForMs = await waitForIpcQuietWindow(timeoutInMs, quietPeriodInMs);

      return {
        status: "ok" as const,
        workerTimestamp: new Date().toISOString(),
        workerInFlightHandlers: 0,
        workerInFlightSends: _ipcInFlightSends,
        workerQuiescing: _isIpcQuiescing,
        workerQuietForMs: Math.trunc(workerQuietForMs),
      };
    },
    IPC_QUIESCE_END: async () => {
      _isIpcQuiescing = false;
      _ipcRestoreAliveSentInCurrentQuiesce = false;
      _ipcRestoreReconnectAttemptedInCurrentQuiesce = false;
      markIpcActivity();

      return {
        status: "ok" as const,
        workerTimestamp: new Date().toISOString(),
        workerQuiescing: _isIpcQuiescing,
      };
    },
  },
});

async function emitIpcRestoreAlive(reason: "sigcont" | "pause_detected", pauseMs?: number) {
  if (!_isIpcQuiescing) {
    return;
  }

  if (!_ipcRestoreReconnectAttemptedInCurrentQuiesce && hasResetConnection(ipcProcess)) {
    _ipcRestoreReconnectAttemptedInCurrentQuiesce = true;
    const reconnectTimeoutInMs = getNumberEnvVar("TRIGGER_IPC_RESTORE_RECONNECT_TIMEOUT_MS", 2000);
    try {
      await ipcProcess.resetConnection(
        reconnectTimeoutInMs && reconnectTimeoutInMs > 0 ? reconnectTimeoutInMs : 2_000
      );
    } catch (error) {
      console.error("Failed to reset child IPC connection after restore signal", {
        reason,
        pauseMs,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  try {
    await zodIpc.send("IPC_RESTORE_ALIVE", {
      version: "v1",
      seq: _ipcRestoreAliveSeq++,
      workerTimestamp: new Date().toISOString(),
      workerPid: process.pid,
      reason,
      pauseMs: pauseMs !== undefined ? Math.max(0, Math.trunc(pauseMs)) : undefined,
    });
    _ipcRestoreAliveSentInCurrentQuiesce = true;
    markIpcActivity();
  } catch (error) {
    console.error("Failed to emit IPC_RESTORE_ALIVE", {
      reason,
      pauseMs,
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

process.on("SIGCONT", () => {
  void emitIpcRestoreAlive("sigcont");
});

const restorePauseThresholdInMs =
  getNumberEnvVar("TRIGGER_IPC_RESTORE_PAUSE_THRESHOLD_MS", 1000) ?? 1000;
globalThis.setInterval(() => {
  const now = Date.now();
  const pauseMs = now - _ipcRestoreAliveTickAt;
  _ipcRestoreAliveTickAt = now;

  if (
    _isIpcQuiescing &&
    !_ipcRestoreAliveSentInCurrentQuiesce &&
    pauseMs >= restorePauseThresholdInMs
  ) {
    void emitIpcRestoreAlive("pause_detected", pauseMs);
  }
}, 250);

const originalIpcSend = zodIpc.send.bind(zodIpc);
zodIpc.send = (async (type: any, payload: any) => {
  _ipcInFlightSends++;
  markIpcActivity();
  try {
    await originalIpcSend(type, payload);
  } finally {
    _ipcInFlightSends = Math.max(0, _ipcInFlightSends - 1);
    markIpcActivity();
  }
}) as typeof zodIpc.send;

async function callCancelHooks(timeoutInMs: number = 10_000) {
  const now = performance.now();

  try {
    await Promise.race([lifecycleHooks.callOnCancelHookListeners(), setTimeout(timeoutInMs)]);
  } finally {
    const duration = performance.now() - now;

    console.log(`Called cancel hooks in ${duration}ms`);
  }
}

async function flushAll(timeoutInMs: number = 10_000) {
  const now = performance.now();

  const { promise, resolve } = promiseWithResolvers<void>();

  _lastFlushPromise = promise;

  const results = await Promise.allSettled([
    flushUsage(timeoutInMs),
    flushTracingSDK(timeoutInMs),
    flushMetadata(timeoutInMs),
  ]);

  const successfulFlushes = results
    .filter((result) => result.status === "fulfilled")
    .map((result) => result.value.flushed);
  const failedFlushes = ["usage", "tracingSDK", "runMetadata"].filter(
    (flushed) => !successfulFlushes.includes(flushed)
  );

  if (failedFlushes.length > 0) {
    console.error(`Failed to flush ${failedFlushes.join(", ")}`);
  }

  const errorMessages = results
    .filter((result) => result.status === "rejected")
    .map((result) => result.reason);

  if (errorMessages.length > 0) {
    console.error(errorMessages.join("\n"));
  }

  for (const flushed of successfulFlushes) {
    console.log(`Flushed ${flushed} successfully`);
  }

  const duration = performance.now() - now;

  console.log(`Flushed all in ${duration}ms`);

  // Resolve the last flush promise
  resolve();
}

async function flushUsage(timeoutInMs: number = 10_000) {
  const now = performance.now();

  await Promise.race([usage.flush(), setTimeout(timeoutInMs)]);

  const duration = performance.now() - now;

  console.log(`Flushed usage in ${duration}ms`);

  return {
    flushed: "usage",
    durationMs: duration,
  };
}

async function flushTracingSDK(timeoutInMs: number = 10_000) {
  const now = performance.now();

  await Promise.race([_tracingSDK?.flush(), setTimeout(timeoutInMs)]);

  const duration = performance.now() - now;

  console.log(`Flushed tracingSDK in ${duration}ms`);

  return {
    flushed: "tracingSDK",
    durationMs: duration,
  };
}

async function flushMetadata(timeoutInMs: number = 10_000) {
  const now = performance.now();

  await Promise.race([runMetadataManager.flush(), setTimeout(timeoutInMs)]);

  const duration = performance.now() - now;

  console.log(`Flushed runMetadata in ${duration}ms`);

  return {
    flushed: "runMetadata",
    durationMs: duration,
  };
}

function initializeUsageManager({
  usageIntervalMs,
  usageEventUrl,
  triggerJWT,
}: {
  usageIntervalMs?: string;
  usageEventUrl?: string;
  triggerJWT?: string;
}) {
  const devUsageManager = new DevUsageManager();
  const prodUsageManager = new ProdUsageManager(devUsageManager, {
    heartbeatIntervalMs: usageIntervalMs ? parseInt(usageIntervalMs, 10) : undefined,
    url: usageEventUrl,
    jwt: triggerJWT,
  });

  usage.setGlobalUsageManager(prodUsageManager);
  timeout.setGlobalManager(new UsageTimeoutManager(devUsageManager));

  // Register listener to send IPC message when max duration is exceeded
  timeout.registerListener(async (maxDurationInSeconds, elapsedTimeInSeconds) => {
    console.log(
      `[${new Date().toISOString()}] Max duration exceeded: ${maxDurationInSeconds}s, elapsed: ${elapsedTimeInSeconds}s`
    );
    await zodIpc.send("MAX_DURATION_EXCEEDED", {
      maxDurationInSeconds,
      elapsedTimeInSeconds,
    });
  });

  return prodUsageManager;
}

_sharedWorkerRuntime = new SharedRuntimeManager(zodIpc, true);
runtime.setGlobalRuntimeManager(_sharedWorkerRuntime);

process.title = "trigger-managed-worker";

standardHeartbeatsManager.registerListener(async (id) => {
  if (_isIpcQuiescing) {
    return;
  }
  await zodIpc.send("TASK_HEARTBEAT", { id });
});

console.log(`[${new Date().toISOString()}] Executor started`);
