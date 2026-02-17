import { type Namespace, Server, type Socket } from "socket.io";
import { SimpleStructuredLogger } from "@basicblock/trigger-core/v3/utils/structuredLogger";
import EventEmitter from "node:events";
import { z } from "zod";
import {
  type SupervisorHttpClient,
  WORKLOAD_HEADERS,
  type WorkloadClientSocketData,
  type WorkloadClientToServerEvents,
  type WorkloadContinueRunExecutionResponseBody,
  WorkloadDebugLogRequestBody,
  type WorkloadDequeueFromVersionResponseBody,
  WorkloadHeartbeatRequestBody,
  type WorkloadHeartbeatResponseBody,
  WorkloadRunAttemptCompleteRequestBody,
  type WorkloadRunAttemptCompleteResponseBody,
  WorkloadRunAttemptStartRequestBody,
  type WorkloadRunAttemptStartResponseBody,
  WorkloadRunSnapshotsSinceResponseBody,
  type WorkloadServerToClientEvents,
  type WorkloadSuspendRunResponseBody,
} from "@basicblock/trigger-core/v3/workers";
import { HttpServer, type CheckpointClient } from "@basicblock/trigger-core/v3/serverOnly";
import { type IncomingMessage } from "node:http";
import { register } from "../metrics.js";
import { env } from "../env.js";

// Use the official export when upgrading to socket.io@4.8.0
interface DefaultEventsMap {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [event: string]: (...args: any[]) => void;
}

const WorkloadActionParams = z.object({
  runFriendlyId: z.string(),
  snapshotFriendlyId: z.string(),
});

type WorkloadServerEvents = {
  runConnected: [
    {
      run: {
        friendlyId: string;
      };
    },
  ];
  runDisconnected: [
    {
      run: {
        friendlyId: string;
      };
    },
  ];
  runActivity: [
    {
      run: {
        friendlyId: string;
      };
      activity: string;
      metadata?: Record<string, unknown>;
    },
  ];
};

type WorkloadServerOptions = {
  port: number;
  host?: string;
  workerClient: SupervisorHttpClient;
  checkpointClient?: CheckpointClient;
};

export class WorkloadServer extends EventEmitter<WorkloadServerEvents> {
  private checkpointClient?: CheckpointClient;

  private readonly logger = new SimpleStructuredLogger("workload-server");

  private readonly httpServer: HttpServer;
  private readonly websocketServer: Namespace<
    WorkloadClientToServerEvents,
    WorkloadServerToClientEvents,
    DefaultEventsMap,
    WorkloadClientSocketData
  >;

  private readonly runSockets = new Map<
    string,
    Socket<
      WorkloadClientToServerEvents,
      WorkloadServerToClientEvents,
      DefaultEventsMap,
      WorkloadClientSocketData
    >
  >();

  private readonly workerClient: SupervisorHttpClient;

  constructor(opts: WorkloadServerOptions) {
    super();

    const host = opts.host ?? "0.0.0.0";
    const port = opts.port;

    this.workerClient = opts.workerClient;
    this.checkpointClient = opts.checkpointClient;

    this.httpServer = this.createHttpServer({ host, port });
    this.websocketServer = this.createWebsocketServer();
  }

  private headerValueFromRequest(req: IncomingMessage, headerName: string): string | undefined {
    const value = req.headers[headerName];

    if (Array.isArray(value)) {
      return value[0];
    }

    return value;
  }

  private runnerIdFromRequest(req: IncomingMessage): string | undefined {
    return this.headerValueFromRequest(req, WORKLOAD_HEADERS.RUNNER_ID);
  }

  private deploymentIdFromRequest(req: IncomingMessage): string | undefined {
    return this.headerValueFromRequest(req, WORKLOAD_HEADERS.DEPLOYMENT_ID);
  }

  private emitRunActivity(
    runFriendlyId: string,
    activity: string,
    metadata?: Record<string, unknown>
  ) {
    this.emit("runActivity", {
      run: { friendlyId: runFriendlyId },
      activity,
      metadata,
    });
  }

  private logWorkloadActionRequestIn(args: {
    method: string;
    path: string;
    runId?: string;
    snapshotId?: string;
    runnerId?: string;
    requestIdempotencyKey?: string;
  }) {
    this.logger.log("workload_action_request_in", args);
  }

  private logWorkloadActionRequestOut(args: {
    method: string;
    path: string;
    runId?: string;
    snapshotId?: string;
    runnerId?: string;
    requestIdempotencyKey?: string;
    status: number;
    elapsedMs: number;
    error?: string;
  }) {
    this.logger.log("workload_action_request_out", args);
  }

  private deploymentVersionFromRequest(req: IncomingMessage): string | undefined {
    return this.headerValueFromRequest(req, WORKLOAD_HEADERS.DEPLOYMENT_VERSION);
  }

  private projectRefFromRequest(req: IncomingMessage): string | undefined {
    return this.headerValueFromRequest(req, WORKLOAD_HEADERS.PROJECT_REF);
  }

  private createHttpServer({ host, port }: { host: string; port: number }) {
    const httpServer = new HttpServer({
      port,
      host,
      metrics: {
        register,
        expose: false,
      },
    })
      .route("/health", "GET", {
        handler: async ({ reply }) => {
          reply.text("OK");
        },
      })
      .route(
        "/api/v1/workload-actions/runs/:runFriendlyId/snapshots/:snapshotFriendlyId/attempts/start",
        "POST",
        {
          paramsSchema: WorkloadActionParams,
          bodySchema: WorkloadRunAttemptStartRequestBody,
          handler: async ({ req, reply, params, body }) => {
            const runnerId = this.runnerIdFromRequest(req);
            this.emitRunActivity(params.runFriendlyId, "http:attempt_start", {
              snapshotId: params.snapshotFriendlyId,
              runnerId,
            });
            const startResponse = await this.workerClient.startRunAttempt(
              params.runFriendlyId,
              params.snapshotFriendlyId,
              body,
              runnerId
            );

            if (!startResponse.success) {
              this.logger.error("Failed to start run", {
                params,
                error: startResponse.error,
              });
              reply.empty(500);
              return;
            }

            reply.json(startResponse.data satisfies WorkloadRunAttemptStartResponseBody);
            return;
          },
        }
      )
      .route(
        "/api/v1/workload-actions/runs/:runFriendlyId/snapshots/:snapshotFriendlyId/attempts/complete",
        "POST",
        {
          paramsSchema: WorkloadActionParams,
          bodySchema: WorkloadRunAttemptCompleteRequestBody,
          handler: async ({ req, reply, params, body }) => {
            const runnerId = this.runnerIdFromRequest(req);
            this.emitRunActivity(params.runFriendlyId, "http:attempt_complete", {
              snapshotId: params.snapshotFriendlyId,
              runnerId,
            });
            const completeResponse = await this.workerClient.completeRunAttempt(
              params.runFriendlyId,
              params.snapshotFriendlyId,
              body,
              runnerId
            );

            if (!completeResponse.success) {
              this.logger.error("Failed to complete run", {
                params,
                error: completeResponse.error,
              });
              reply.empty(500);
              return;
            }

            reply.json(completeResponse.data satisfies WorkloadRunAttemptCompleteResponseBody);
            return;
          },
        }
      )
      .route(
        "/api/v1/workload-actions/runs/:runFriendlyId/snapshots/:snapshotFriendlyId/heartbeat",
        "POST",
        {
          paramsSchema: WorkloadActionParams,
          bodySchema: WorkloadHeartbeatRequestBody,
          handler: async ({ req, reply, params, body }) => {
            const runnerId = this.runnerIdFromRequest(req);
            this.emitRunActivity(params.runFriendlyId, "http:heartbeat", {
              snapshotId: params.snapshotFriendlyId,
              runnerId,
            });
            const heartbeatResponse = await this.workerClient.heartbeatRun(
              params.runFriendlyId,
              params.snapshotFriendlyId,
              body,
              runnerId
            );

            if (!heartbeatResponse.success) {
              this.logger.error("Failed to heartbeat run", {
                params,
                error: heartbeatResponse.error,
              });
              reply.empty(500);
              return;
            }

            reply.json({
              ok: true,
            } satisfies WorkloadHeartbeatResponseBody);
          },
        }
      )
      .route(
        "/api/v1/workload-actions/runs/:runFriendlyId/snapshots/:snapshotFriendlyId/suspend",
        "GET",
        {
          paramsSchema: WorkloadActionParams,
          handler: async ({ reply, params, req }) => {
            this.logger.debug("Suspend request", { params, headers: req.headers });
            const runnerId = this.runnerIdFromRequest(req);
            this.emitRunActivity(params.runFriendlyId, "http:suspend", {
              snapshotId: params.snapshotFriendlyId,
              runnerId,
            });

            if (!this.checkpointClient) {
              reply.json(
                {
                  ok: false,
                  error: "Checkpoints disabled",
                } satisfies WorkloadSuspendRunResponseBody,
                false,
                400
              );
              return;
            }

            const deploymentVersion = this.deploymentVersionFromRequest(req);
            const projectRef = this.projectRefFromRequest(req);

            if (!runnerId || !deploymentVersion || !projectRef) {
              this.logger.error("Invalid headers for suspend request", {
                ...params,
                headers: req.headers,
              });
              reply.json(
                {
                  ok: false,
                  error: "Invalid headers",
                } satisfies WorkloadSuspendRunResponseBody,
                false,
                400
              );
              return;
            }

            reply.json(
              {
                ok: true,
              } satisfies WorkloadSuspendRunResponseBody,
              false,
              202
            );

            const suspendResult = await this.checkpointClient.suspendRun({
              runFriendlyId: params.runFriendlyId,
              snapshotFriendlyId: params.snapshotFriendlyId,
              body: {
                runnerId,
                runId: params.runFriendlyId,
                snapshotId: params.snapshotFriendlyId,
                projectRef,
                deploymentVersion,
              },
            });

            if (!suspendResult) {
              this.logger.error("Failed to suspend run", { params });
              return;
            }
          },
        }
      )
      .route(
        "/api/v1/workload-actions/runs/:runFriendlyId/snapshots/:snapshotFriendlyId/continue",
        "GET",
        {
          paramsSchema: WorkloadActionParams,
          handler: async ({ req, reply, params }) => {
            const startedAt = Date.now();
            this.logger.debug("Run continuation request", { params });
            const runnerId = this.runnerIdFromRequest(req);
            this.logWorkloadActionRequestIn({
              method: "GET",
              path: "/api/v1/workload-actions/runs/:runFriendlyId/snapshots/:snapshotFriendlyId/continue",
              runId: params.runFriendlyId,
              snapshotId: params.snapshotFriendlyId,
              runnerId,
            });
            this.emitRunActivity(params.runFriendlyId, "http:continue", {
              snapshotId: params.snapshotFriendlyId,
              runnerId,
            });

            const continuationResult = await this.workerClient.continueRunExecution(
              params.runFriendlyId,
              params.snapshotFriendlyId,
              runnerId
            );

            if (!continuationResult.success) {
              this.logger.error("Failed to continue run execution", { params });
              this.logWorkloadActionRequestOut({
                method: "GET",
                path: "/api/v1/workload-actions/runs/:runFriendlyId/snapshots/:snapshotFriendlyId/continue",
                runId: params.runFriendlyId,
                snapshotId: params.snapshotFriendlyId,
                runnerId,
                status: 400,
                elapsedMs: Date.now() - startedAt,
                error: continuationResult.error,
              });
              reply.json(
                {
                  ok: false,
                  error: "Failed to continue run execution",
                },
                false,
                400
              );
              return;
            }

            this.logWorkloadActionRequestOut({
              method: "GET",
              path: "/api/v1/workload-actions/runs/:runFriendlyId/snapshots/:snapshotFriendlyId/continue",
              runId: params.runFriendlyId,
              snapshotId: params.snapshotFriendlyId,
              runnerId,
              status: 200,
              elapsedMs: Date.now() - startedAt,
            });
            reply.json(continuationResult.data as WorkloadContinueRunExecutionResponseBody);
          },
        }
      )
      .route(
        "/api/v1/workload-actions/runs/:runFriendlyId/snapshots/since/:snapshotFriendlyId",
        "GET",
        {
          paramsSchema: WorkloadActionParams,
          handler: async ({ req, reply, params }) => {
            const startedAt = Date.now();
            const runnerId = this.runnerIdFromRequest(req);
            const requestIdempotencyKey = this.headerValueFromRequest(
              req,
              "x-trigger-request-idempotency-key"
            );
            const requestContext = {
              runId: params.runFriendlyId,
              snapshotId: params.snapshotFriendlyId,
              runnerId,
              requestIdempotencyKey,
            };
            this.logWorkloadActionRequestIn({
              method: "GET",
              path: "/api/v1/workload-actions/runs/:runFriendlyId/snapshots/since/:snapshotFriendlyId",
              runId: params.runFriendlyId,
              snapshotId: params.snapshotFriendlyId,
              runnerId,
              requestIdempotencyKey,
            });
            this.emitRunActivity(params.runFriendlyId, "http:snapshots_since", requestContext);

            req.once("aborted", () => {
              this.logger.warn("getSnapshotsSince request aborted by client", {
                ...requestContext,
                durationMs: Date.now() - startedAt,
              });
            });

            this.logger.log("getSnapshotsSince request started", requestContext);

            const sinceSnapshotResponse = await this.workerClient.getSnapshotsSince(
              params.runFriendlyId,
              params.snapshotFriendlyId,
              runnerId
            );

            if (!sinceSnapshotResponse.success) {
              this.logger.error("Failed to get snapshots since", {
                ...requestContext,
                error: sinceSnapshotResponse.error,
                durationMs: Date.now() - startedAt,
              });
              this.logWorkloadActionRequestOut({
                method: "GET",
                path: "/api/v1/workload-actions/runs/:runFriendlyId/snapshots/since/:snapshotFriendlyId",
                runId: params.runFriendlyId,
                snapshotId: params.snapshotFriendlyId,
                runnerId,
                requestIdempotencyKey,
                status: 500,
                elapsedMs: Date.now() - startedAt,
                error: sinceSnapshotResponse.error,
              });
              reply.empty(500);
              return;
            }

            this.logger.log("getSnapshotsSince request finished", {
              ...requestContext,
              durationMs: Date.now() - startedAt,
              statusCode: 200,
              snapshotCount: sinceSnapshotResponse.data.snapshots.length,
            });
            this.logWorkloadActionRequestOut({
              method: "GET",
              path: "/api/v1/workload-actions/runs/:runFriendlyId/snapshots/since/:snapshotFriendlyId",
              runId: params.runFriendlyId,
              snapshotId: params.snapshotFriendlyId,
              runnerId,
              requestIdempotencyKey,
              status: 200,
              elapsedMs: Date.now() - startedAt,
            });
            reply.json(sinceSnapshotResponse.data satisfies WorkloadRunSnapshotsSinceResponseBody);
          },
        }
      )
      .route("/api/v1/workload-actions/deployments/:deploymentId/dequeue", "GET", {
        paramsSchema: z.object({
          deploymentId: z.string(),
        }),

        handler: async ({ req, reply, params }) => {
          const dequeueResponse = await this.workerClient.dequeueFromVersion(
            params.deploymentId,
            1,
            this.runnerIdFromRequest(req)
          );

          if (!dequeueResponse.success) {
            this.logger.error("Failed to get latest snapshot", {
              deploymentId: params.deploymentId,
              error: dequeueResponse.error,
            });
            reply.empty(500);
            return;
          }

          reply.json(dequeueResponse.data satisfies WorkloadDequeueFromVersionResponseBody);
        },
      });

    if (env.SEND_RUN_DEBUG_LOGS) {
      httpServer.route("/api/v1/workload-actions/runs/:runFriendlyId/logs/debug", "POST", {
        paramsSchema: WorkloadActionParams.pick({ runFriendlyId: true }),
        bodySchema: WorkloadDebugLogRequestBody,
        handler: async ({ req, reply, params, body }) => {
          this.emitRunActivity(params.runFriendlyId, "http:debug_log", {
            runnerId: this.runnerIdFromRequest(req),
          });
          reply.empty(204);

          await this.workerClient.sendDebugLog(
            params.runFriendlyId,
            body,
            this.runnerIdFromRequest(req)
          );
        },
      });
    } else {
      // Lightweight mock route without schemas
      httpServer.route("/api/v1/workload-actions/runs/:runFriendlyId/logs/debug", "POST", {
        handler: async ({ reply }) => {
          reply.empty(204);
        },
      });
    }

    return httpServer;
  }

  private createWebsocketServer() {
    const io = new Server(this.httpServer.server, {
      pingInterval: env.TRIGGER_WORKLOAD_API_WS_PING_INTERVAL_MS,
      pingTimeout: env.TRIGGER_WORKLOAD_API_WS_PING_TIMEOUT_MS,
    });

    const websocketServer: Namespace<
      WorkloadClientToServerEvents,
      WorkloadServerToClientEvents,
      DefaultEventsMap,
      WorkloadClientSocketData
    > = io.of("/workload");

    websocketServer.on("disconnect", (socket) => {
      this.logger.log("[WS] disconnect", socket.id);
    });
    websocketServer.use(async (socket, next) => {
      const setSocketDataFromHeader = (
        dataKey: keyof typeof socket.data,
        headerName: string,
        required: boolean = true
      ) => {
        const value = socket.handshake.headers[headerName];

        if (value) {
          if (Array.isArray(value)) {
            if (value[0]) {
              socket.data[dataKey] = value[0];
              return;
            }
          } else {
            socket.data[dataKey] = value;
            return;
          }
        }

        if (required) {
          this.logger.error("[WS] missing required header", { headerName });
          throw new Error("missing header");
        }
      };

      try {
        setSocketDataFromHeader("deploymentId", WORKLOAD_HEADERS.DEPLOYMENT_ID);
        setSocketDataFromHeader("runnerId", WORKLOAD_HEADERS.RUNNER_ID);
      } catch (error) {
        this.logger.error("[WS] setSocketDataFromHeader error", { error });
        socket.disconnect(true);
        return;
      }

      this.logger.debug("[WS] auth success", socket.data);

      next();
    });
    websocketServer.on("connection", (socket) => {
      const socketLogger = this.logger.child({
        socketId: socket.id,
        socketData: socket.data,
      });

      const getSocketMetadata = () => {
        return {
          deploymentId: socket.data.deploymentId,
          runId: socket.data.runFriendlyId,
          snapshotId: socket.data.snapshotId,
          runnerId: socket.data.runnerId,
        };
      };

      const runConnected = (friendlyId: string) => {
        socketLogger.debug("runConnected", { ...getSocketMetadata() });

        // If there's already a run ID set, we should "disconnect" it from this socket
        if (socket.data.runFriendlyId && socket.data.runFriendlyId !== friendlyId) {
          socketLogger.debug("runConnected: disconnecting existing run", {
            ...getSocketMetadata(),
            newRunId: friendlyId,
            oldRunId: socket.data.runFriendlyId,
          });
          runDisconnected(socket.data.runFriendlyId);
        }

        this.runSockets.set(friendlyId, socket);
        this.emit("runConnected", { run: { friendlyId } });
        this.emitRunActivity(friendlyId, "ws:run_connected", {
          runnerId: socket.data.runnerId,
        });
        socket.data.runFriendlyId = friendlyId;
      };

      const runDisconnected = (friendlyId: string) => {
        socketLogger.debug("runDisconnected", { ...getSocketMetadata() });

        this.runSockets.delete(friendlyId);
        this.emit("runDisconnected", { run: { friendlyId } });
        socket.data.runFriendlyId = undefined;
      };

      socketLogger.log("wsServer socket connected", { ...getSocketMetadata() });

      // FIXME: where does this get set?
      if (socket.data.runFriendlyId) {
        runConnected(socket.data.runFriendlyId);
      }

      socket.on("disconnecting", (reason, description) => {
        socketLogger.log("Socket disconnecting", { ...getSocketMetadata(), reason, description });

        if (socket.data.runFriendlyId) {
          runDisconnected(socket.data.runFriendlyId);
        }
      });

      socket.on("disconnect", (reason, description) => {
        socketLogger.log("Socket disconnected", { ...getSocketMetadata(), reason, description });
      });

      socket.on("error", (error) => {
        socketLogger.error("Socket error", {
          ...getSocketMetadata(),
          error: {
            name: error.name,
            message: error.message,
            stack: error.stack,
          },
        });
      });

      socket.on("run:start", async (message) => {
        this.emitRunActivity(message.run.friendlyId, "ws:run_start", {
          runnerId: socket.data.runnerId,
          snapshotId: message.snapshot.friendlyId,
        });
        const log = socketLogger.child({
          eventName: "run:start",
          ...getSocketMetadata(),
          ...message,
        });

        log.log("Handling run:start");

        try {
          runConnected(message.run.friendlyId);
        } catch (error) {
          log.error("run:start error", { error });
        }
      });

      socket.on("run:stop", async (message) => {
        const log = socketLogger.child({
          eventName: "run:stop",
          ...getSocketMetadata(),
          ...message,
        });

        log.log("Handling run:stop");

        try {
          runDisconnected(message.run.friendlyId);
        } catch (error) {
          log.error("run:stop error", { error });
        }
      });
    });

    return websocketServer;
  }

  notifyRun({ run }: { run: { friendlyId: string } }) {
    try {
      const runSocket = this.runSockets.get(run.friendlyId);

      if (!runSocket) {
        this.logger.debug("notifyRun: Run socket not found", { run });

        this.workerClient.sendDebugLog(run.friendlyId, {
          time: new Date(),
          message: "run:notify socket not found on supervisor",
        });

        return;
      }

      runSocket.emit("run:notify", { version: "1", run });
      this.logger.debug("run:notify sent", { run });

      this.workerClient.sendDebugLog(run.friendlyId, {
        time: new Date(),
        message: "run:notify supervisor -> runner",
      });
    } catch (error) {
      this.logger.error("Error in notifyRun", { run, error });

      this.workerClient.sendDebugLog(run.friendlyId, {
        time: new Date(),
        message: "run:notify error on supervisor",
      });
    }
  }

  async start() {
    await this.httpServer.start();
  }

  async stop() {
    await this.httpServer.stop();
  }
}
