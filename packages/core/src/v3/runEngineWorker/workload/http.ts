import * as http from "node:http";
import * as https from "node:https";
import { z } from "zod";
import {
  WorkloadHeartbeatRequestBody,
  WorkloadHeartbeatResponseBody,
  WorkloadRunAttemptCompleteRequestBody,
  WorkloadRunAttemptCompleteResponseBody,
  WorkloadRunAttemptStartResponseBody,
  WorkloadDequeueFromVersionResponseBody,
  WorkloadRunAttemptStartRequestBody,
  WorkloadSuspendRunResponseBody,
  WorkloadContinueRunExecutionResponseBody,
  WorkloadDebugLogRequestBody,
  WorkloadRunSnapshotsSinceResponseBody,
} from "./schemas.js";
import { WorkloadClientCommonOptions } from "./types.js";
import { getDefaultWorkloadHeaders } from "./util.js";
import { wrapZodFetch } from "../../zodfetch.js";
import { randomUUID } from "../../utils/crypto.js";

type WorkloadHttpClientOptions = WorkloadClientCommonOptions;
type ApiResult<T> =
  | { success: true; data: T }
  | { success: false; error: string; isConnectionError?: boolean };

export class WorkloadHttpClient {
  private apiUrl: string;
  private runnerId: string;
  private readonly deploymentId: string;
  private readonly forceConnectionClose: boolean;
  private readonly traceHttpRequests = process.env.TRIGGER_HTTP_TRACE === "true";
  private readonly logHttpRequests = process.env.TRIGGER_HTTP_LOG === "true";

  constructor(private opts: WorkloadHttpClientOptions) {
    this.apiUrl = opts.workerApiUrl.replace(/\/$/, "");
    this.deploymentId = opts.deploymentId;
    this.runnerId = opts.runnerId;
    this.forceConnectionClose =
      opts.forceConnectionClose ?? process.env.TRIGGER_FORCE_CONNECTION_CLOSE === "true";

    if (!this.apiUrl) {
      throw new Error("apiURL is required and needs to be a non-empty string");
    }

    if (!this.deploymentId) {
      throw new Error("deploymentId is required and needs to be a non-empty string");
    }
  }

  updateApiUrl(apiUrl: string) {
    this.apiUrl = apiUrl.replace(/\/$/, "");
  }

  updateRunnerId(runnerId: string) {
    this.runnerId = runnerId;
  }

  defaultHeaders(): Record<string, string> {
    return getDefaultWorkloadHeaders({
      ...this.opts,
      runnerId: this.runnerId,
    });
  }

  private maybeCloseHeader(): Record<string, string> {
    return this.forceConnectionClose ? { Connection: "close" } : {};
  }

  private nowMs(): number {
    return typeof performance !== "undefined" ? performance.now() : Date.now();
  }

  private async timed<T>(
    name: string,
    close: boolean,
    fn: () => Promise<ApiResult<T>>
  ): Promise<ApiResult<T>> {
    const t0 = this.nowMs();
    const result = await fn();
    const dt = Math.round(this.nowMs() - t0);

    if (!this.logHttpRequests && result.success) {
      return result;
    }

    if (result.success) {
      console.log(`[http] ${name} success ${dt}ms close=${close}`);
    } else {
      console.log(
        `[http] ${name} fail ${dt}ms close=${close} conn=${result.isConnectionError ? "y" : "n"} err=${result.error}`
      );
    }

    return result;
  }

  private isConnectionError(error: string): boolean {
    const connectionErrors = [
      "Connection error",
      "ECONNREFUSED",
      "ETIMEDOUT",
      "ENOTFOUND",
      "ECONNRESET",
      "EHOSTUNREACH",
      "ENETUNREACH",
      "EPIPE",
      "ECONNABORTED",
    ];
    return connectionErrors.some((errType) => error.includes(errType));
  }

  private async withConnectionErrorDetection<T>(
    operation: () => Promise<{ success: true; data: T } | { success: false; error: string }>
  ): Promise<
    { success: true; data: T } | { success: false; error: string; isConnectionError?: boolean }
  > {
    const result = await operation();

    if (result.success) {
      return result;
    }

    // Check if this is a connection error
    if (this.isConnectionError(result.error)) {
      return {
        ...result,
        isConnectionError: true,
      };
    }

    return result;
  }

  private async requestJson<TSchema extends z.ZodTypeAny>(
    schema: TSchema,
    url: string,
    init: {
      method: "GET" | "POST";
      headers: Record<string, string>;
      body?: string;
      timeoutMs?: number;
    }
  ): Promise<ApiResult<z.output<TSchema>>> {
    const parsedUrl = new URL(url);
    const supervisorClusterIp = process.env.TRIGGER_SUPERVISOR_CLUSTER_IP;
    const requestHostname = supervisorClusterIp || parsedUrl.hostname;
    const transport = parsedUrl.protocol === "https:" ? https : http;
    const headers = new Headers(init.headers);

    headers.set("x-trigger-request-idempotency-key", await randomUUID());
    const requestId = headers.get("x-trigger-request-idempotency-key") ?? "unknown";
    const startedAt = this.nowMs();
    const path = `${parsedUrl.pathname}${parsedUrl.search}`;
    const trace = (event: string, properties?: Record<string, unknown>) => {
      if (!this.traceHttpRequests) {
        return;
      }

      const durationMs = Math.round(this.nowMs() - startedAt);
      console.log(
        `[http-trace] ${event} id=${requestId} ${init.method} ${requestHostname}:${parsedUrl.port || (parsedUrl.protocol === "https:" ? "443" : "80")}${path} t=${durationMs}ms runner=${this.runnerId}`,
        properties ?? {}
      );
    };

    trace("request_start", {
      timeoutMs: init.timeoutMs,
      hasBody: init.body !== undefined,
      forceConnectionClose: this.forceConnectionClose,
      protocol: parsedUrl.protocol,
      requestedHost: parsedUrl.host,
      requestHostname,
      usingClusterIpOverride: Boolean(supervisorClusterIp),
    });

    if (supervisorClusterIp && !headers.has("host")) {
      headers.set("host", parsedUrl.host);
    }

    if (init.body !== undefined && !headers.has("content-type")) {
      headers.set("content-type", "application/json");
    }

    if (init.body !== undefined && !headers.has("content-length")) {
      headers.set("content-length", Buffer.byteLength(init.body).toString());
    }

    return new Promise((resolve) => {
      let settled = false;
      const resolveOnce = (result: ApiResult<z.output<TSchema>>) => {
        if (settled) {
          trace("resolve_ignored_already_settled");
          return;
        }

        settled = true;
        if (timeoutHandle) {
          clearTimeout(timeoutHandle);
        }

        if (result.success) {
          trace("request_resolved_success");
        } else {
          trace("request_resolved_failure", {
            error: result.error,
            isConnectionError: result.isConnectionError ?? false,
          });
        }

        resolve(result);
      };

      const req = transport.request(
        {
          protocol: parsedUrl.protocol,
          hostname: requestHostname,
          port: parsedUrl.port
            ? Number(parsedUrl.port)
            : parsedUrl.protocol === "https:"
              ? 443
              : 80,
          path: `${parsedUrl.pathname}${parsedUrl.search}`,
          method: init.method,
          headers: Object.fromEntries(headers.entries()),
          agent: false,
        },
        (res) => {
          const chunks: Buffer[] = [];
          let totalResponseBytes = 0;
          trace("response_headers", {
            statusCode: res.statusCode ?? 0,
            hasContentLengthHeader: Boolean(res.headers["content-length"]),
          });

          res.on("data", (chunk: Buffer | string) => {
            const normalizedChunk = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
            chunks.push(normalizedChunk);
            totalResponseBytes += normalizedChunk.length;
            trace("response_data", {
              chunkCount: chunks.length,
              totalBytes: totalResponseBytes,
            });
          });

          res.on("error", (error) => {
            trace("response_error", { error: error.message });
            resolveOnce({
              success: false,
              error: `Connection error. (${error.message})`,
            });
          });

          res.on("end", () => {
            const statusCode = res.statusCode ?? 0;
            const rawBody = Buffer.concat(chunks).toString("utf8");
            trace("response_end", {
              statusCode,
              bodyLength: rawBody.length,
            });

            if (statusCode < 200 || statusCode >= 300) {
              resolveOnce({
                success: false,
                error: `Request failed with status ${statusCode}${rawBody ? `: ${rawBody}` : ""}`,
              });
              return;
            }

            let parsedBody: unknown = undefined;

            if (rawBody.length > 0) {
              try {
                parsedBody = JSON.parse(rawBody);
              } catch {
                resolveOnce({ success: false, error: "Invalid JSON response body" });
                return;
              }
            }

            const result = schema.safeParse(parsedBody);

            if (!result.success) {
              resolveOnce({
                success: false,
                error: `Response validation failed: ${result.error.message}`,
              });
              return;
            }

            resolveOnce({ success: true, data: result.data });
          });
        }
      );

      req.on("error", (error) => {
        trace("request_error", { error: error.message });
        resolveOnce({ success: false, error: `Connection error. (${error.message})` });
      });

      req.on("socket", (socket) => {
        trace("socket_assigned", {
          localAddress: socket.localAddress,
          localPort: socket.localPort,
          remoteAddress: socket.remoteAddress,
          remotePort: socket.remotePort,
        });

        socket.on("lookup", (error, address, family, host) => {
          trace("socket_lookup", {
            error: error?.message,
            address,
            family,
            host,
          });
        });

        socket.on("connect", () => {
          trace("socket_connect", {
            localAddress: socket.localAddress,
            localPort: socket.localPort,
            remoteAddress: socket.remoteAddress,
            remotePort: socket.remotePort,
          });
        });

        socket.on("secureConnect", () => {
          trace("socket_secure_connect");
        });

        socket.on("timeout", () => {
          trace("socket_timeout");
        });

        socket.on("error", (error) => {
          trace("socket_error", { error: error.message });
        });

        socket.on("close", (hadError) => {
          trace("socket_close", { hadError });
        });
      });

      req.on("finish", () => {
        trace("request_finish");
      });

      req.on("close", () => {
        trace("request_close");
      });

      const timeoutHandle =
        init.timeoutMs !== undefined
          ? setTimeout(() => {
              trace("request_timeout_reached", { timeoutMs: init.timeoutMs });
              req.destroy(new Error("The operation was aborted due to timeout"));
            }, init.timeoutMs)
          : undefined;

      if (init.body !== undefined) {
        trace("request_write_body", { bodyLength: Buffer.byteLength(init.body) });
        req.write(init.body);
      }

      trace("request_end_called");
      req.end();
    });
  }

  async heartbeatRun(runId: string, snapshotId: string, body?: WorkloadHeartbeatRequestBody) {
    return this.timed("heartbeatRun", this.forceConnectionClose, () =>
      this.withConnectionErrorDetection(() =>
        this.requestJson(
          WorkloadHeartbeatResponseBody,
          `${this.apiUrl}/api/v1/workload-actions/runs/${runId}/snapshots/${snapshotId}/heartbeat`,
          {
            method: "POST",
            headers: {
              ...this.defaultHeaders(),
              ...this.maybeCloseHeader(),
              "Content-Type": "application/json",
            },
            body: JSON.stringify(body ?? {}),
            timeoutMs: 10_000,
          }
        )
      )
    );
  }

  async suspendRun(runId: string, snapshotId: string) {
    return wrapZodFetch(
      WorkloadSuspendRunResponseBody,
      `${this.apiUrl}/api/v1/workload-actions/runs/${runId}/snapshots/${snapshotId}/suspend`,
      {
        method: "GET",
        headers: {
          ...this.defaultHeaders(),
        },
      }
    );
  }

  async continueRunExecution(runId: string, snapshotId: string) {
    return this.timed("continueRunExecution", this.forceConnectionClose, () =>
      this.withConnectionErrorDetection(() =>
        this.requestJson(
          WorkloadContinueRunExecutionResponseBody,
          `${this.apiUrl}/api/v1/workload-actions/runs/${runId}/snapshots/${snapshotId}/continue`,
          {
            method: "GET",
            headers: {
              ...this.defaultHeaders(),
              ...this.maybeCloseHeader(),
            },
            timeoutMs: 10_000,
          }
        )
      )
    );
  }

  async startRunAttempt(
    runId: string,
    snapshotId: string,
    body: WorkloadRunAttemptStartRequestBody
  ) {
    return wrapZodFetch(
      WorkloadRunAttemptStartResponseBody,
      `${this.apiUrl}/api/v1/workload-actions/runs/${runId}/snapshots/${snapshotId}/attempts/start`,
      {
        method: "POST",
        headers: {
          ...this.defaultHeaders(),
        },
        body: JSON.stringify(body),
      }
    );
  }

  async completeRunAttempt(
    runId: string,
    snapshotId: string,
    body: WorkloadRunAttemptCompleteRequestBody
  ) {
    return wrapZodFetch(
      WorkloadRunAttemptCompleteResponseBody,
      `${this.apiUrl}/api/v1/workload-actions/runs/${runId}/snapshots/${snapshotId}/attempts/complete`,
      {
        method: "POST",
        headers: {
          ...this.defaultHeaders(),
        },
        body: JSON.stringify(body),
      }
    );
  }

  async getSnapshotsSince(runId: string, snapshotId: string) {
    return this.timed("getSnapshotsSince", this.forceConnectionClose, () =>
      this.withConnectionErrorDetection(() =>
        this.requestJson(
          WorkloadRunSnapshotsSinceResponseBody,
          `${this.apiUrl}/api/v1/workload-actions/runs/${runId}/snapshots/since/${snapshotId}`,
          {
            method: "GET",
            headers: {
              ...this.defaultHeaders(),
              ...this.maybeCloseHeader(),
            },
            timeoutMs: 10_000,
          }
        )
      )
    );
  }

  async sendDebugLog(runId: string, body: WorkloadDebugLogRequestBody): Promise<void> {
    const res = await this.requestJson(
      z.unknown(),
      `${this.apiUrl}/api/v1/workload-actions/runs/${runId}/logs/debug`,
      {
        method: "POST",
        headers: {
          ...this.defaultHeaders(),
          ...this.maybeCloseHeader(),
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
        timeoutMs: 10_000,
      }
    );

    if (!res.success) {
      console.error("Failed to send debug log", res);
    }
  }

  /** @deprecated Not currently used */
  async dequeue() {
    return wrapZodFetch(
      WorkloadDequeueFromVersionResponseBody,
      `${this.apiUrl}/api/v1/workload-actions/deployments/${this.deploymentId}/dequeue`,
      {
        method: "GET",
        headers: {
          ...this.defaultHeaders(),
        },
      }
    );
  }
}
