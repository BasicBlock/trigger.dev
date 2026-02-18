import { EventEmitter } from "node:events";
import { existsSync, unlinkSync } from "node:fs";
import net, { Server, Socket } from "node:net";

type MessageListener = (message: any) => void;

export interface IpcProcessLike {
  send: (message: any) => Promise<void>;
  on: (event: "message", listener: MessageListener) => void;
  resetConnection?: (timeoutInMs?: number) => Promise<void>;
  close: () => Promise<void>;
}

export type ParentSocketIpcOptions = {
  connectTimeoutInMs?: number;
};

function parseSocketData(buffer: string, emitMessage: (message: any) => void): string {
  let remaining = buffer;
  let newlineIndex = remaining.indexOf("\n");

  while (newlineIndex >= 0) {
    const packet = remaining.slice(0, newlineIndex).trim();
    remaining = remaining.slice(newlineIndex + 1);

    if (packet.length > 0) {
      try {
        emitMessage(JSON.parse(packet));
      } catch {
        // Drop malformed packets and continue processing the stream.
      }
    }

    newlineIndex = remaining.indexOf("\n");
  }

  return remaining;
}

function writePacket(socket: Socket, message: any): Promise<void> {
  return new Promise((resolve, reject) => {
    const payload = `${JSON.stringify(message)}\n`;

    socket.write(payload, (error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}

function isRecoverableSocketWriteError(error: unknown): boolean {
  const code = (error as NodeJS.ErrnoException | undefined)?.code;
  return (
    code === "EPIPE" ||
    code === "ECONNRESET" ||
    code === "ECONNREFUSED" ||
    code === "ERR_STREAM_DESTROYED"
  );
}

export function createParentSocketIpcProcess(
  socketPath: string,
  options: ParentSocketIpcOptions = {}
): IpcProcessLike {
  const emitter = new EventEmitter();
  const SOCKET_CONNECTED_EVENT = "__socket_connected__";
  let server: Server | undefined;
  let activeSocket: Socket | undefined;
  let activeBuffer = "";
  let closed = false;
  let serverRestartPromise: Promise<void> | undefined;

  try {
    if (existsSync(socketPath)) {
      unlinkSync(socketPath);
    }
  } catch {
    // Best-effort cleanup of stale socket path.
  }
  const handleConnection = (socket: Socket) => {
    if (activeSocket && !activeSocket.destroyed) {
      activeSocket.destroy();
    }

    activeSocket = socket;
    activeBuffer = "";

    socket.setNoDelay(true);
    socket.setEncoding("utf8");

    socket.on("data", (chunk: string) => {
      activeBuffer = parseSocketData(activeBuffer + chunk, (message) => {
        emitter.emit("message", message);
      });
    });

    socket.on("close", () => {
      if (activeSocket === socket) {
        activeSocket = undefined;
        activeBuffer = "";
      }
    });

    emitter.emit(SOCKET_CONNECTED_EVENT, socket);
  };

  const createServer = async () => {
    let lastError: unknown;

    for (let attempt = 0; attempt < 10; attempt++) {
      const nextServer = net.createServer(handleConnection);

      try {
        await new Promise<void>((resolve, reject) => {
          nextServer.once("error", reject);
          nextServer.listen(socketPath, () => {
            nextServer.off("error", reject);
            resolve();
          });
        });

        server = nextServer;
        return;
      } catch (error) {
        lastError = error;
        nextServer.close();

        if ((error as NodeJS.ErrnoException)?.code !== "EADDRINUSE" || attempt === 9) {
          throw error;
        }

        try {
          if (existsSync(socketPath)) {
            unlinkSync(socketPath);
          }
        } catch {
          // Best-effort cleanup before retry.
        }

        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }

    throw lastError ?? new Error("Failed to start socket IPC server");
  };

  const restartServer = async () => {
    if (serverRestartPromise) {
      await serverRestartPromise;
      return;
    }

    serverRestartPromise = (async () => {
      const existingServer = server;
      server = undefined;

      if (existingServer) {
        await new Promise<void>((resolve) => existingServer.close(() => resolve()));
      }

      try {
        if (existsSync(socketPath)) {
          unlinkSync(socketPath);
        }
      } catch {
        // Best-effort cleanup of stale socket path.
      }

      await createServer();
    })();

    try {
      await serverRestartPromise;
    } finally {
      serverRestartPromise = undefined;
    }
  };

  serverRestartPromise = createServer().finally(() => {
    serverRestartPromise = undefined;
  });

  const connectTimeoutInMs = options.connectTimeoutInMs ?? 10_000;

  const waitForConnection = async (timeoutInMs = connectTimeoutInMs): Promise<Socket> => {
    if (serverRestartPromise) {
      await serverRestartPromise;
    }

    if (activeSocket && !activeSocket.destroyed) {
      return activeSocket;
    }

    return await new Promise<Socket>((resolve, reject) => {
      const timeout = setTimeout(() => {
        cleanup();
        reject(new Error(`Timed out waiting for socket IPC connection (${timeoutInMs}ms)`));
      }, timeoutInMs);

      const onConnection = (socket: Socket) => {
        cleanup();
        resolve(socket);
      };

      const cleanup = () => {
        clearTimeout(timeout);
        emitter.off(SOCKET_CONNECTED_EVENT, onConnection);
      };

      emitter.on(SOCKET_CONNECTED_EVENT, onConnection);

      // Close race window: connection may arrive between initial activeSocket check
      // and listener registration, so re-check after subscribing.
      if (activeSocket && !activeSocket.destroyed) {
        cleanup();
        resolve(activeSocket);
      }
    });
  };

  const destroyActiveSocket = () => {
    if (activeSocket && !activeSocket.destroyed) {
      activeSocket.destroy();
    }
    activeSocket = undefined;
    activeBuffer = "";
  };

  return {
    send: async (message: any) => {
      if (closed) {
        throw new Error("Socket IPC is closed");
      }

      const socket = await waitForConnection();
      await writePacket(socket, message);
    },
    on: (_event: "message", listener: MessageListener) => {
      emitter.on("message", listener);
    },
    resetConnection: async (timeoutInMs = connectTimeoutInMs) => {
      if (closed) {
        throw new Error("Socket IPC is closed");
      }

      destroyActiveSocket();

      try {
        await waitForConnection(timeoutInMs);
      } catch {
        // Fallback for post-restore stale listener state: restart the socket server and retry once.
        await restartServer();
        await waitForConnection(timeoutInMs);
      }
    },
    close: async () => {
      closed = true;

      destroyActiveSocket();

      await new Promise<void>((resolve) => {
        if (!server) {
          resolve();
          return;
        }

        server.close(() => resolve());
      });

      try {
        if (existsSync(socketPath)) {
          unlinkSync(socketPath);
        }
      } catch {
        // Ignore cleanup failures.
      }
    },
  };
}

export function createChildSocketIpcProcess(socketPath: string): IpcProcessLike {
  const emitter = new EventEmitter();
  let socket: Socket | undefined;
  let buffer = "";
  let closed = false;
  let connectingPromise: Promise<Socket> | undefined;
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined;
  const defaultConnectAttemptTimeoutInMs = 1_000;

  const clearReconnectTimer = () => {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = undefined;
    }
  };

  const destroySocket = (candidate?: Socket) => {
    if (!candidate) {
      return;
    }

    if (!candidate.destroyed) {
      candidate.destroy();
    }

    if (socket === candidate) {
      socket = undefined;
      buffer = "";
    }
  };

  const scheduleReconnect = () => {
    if (closed || reconnectTimer || connectingPromise || (socket && !socket.destroyed)) {
      return;
    }

    reconnectTimer = setTimeout(() => {
      reconnectTimer = undefined;

      if (closed || connectingPromise || (socket && !socket.destroyed)) {
        return;
      }

      void connect().catch(() => {
        // Keep trying to reconnect in the background until closed.
        scheduleReconnect();
      });
    }, 100);
  };

  const attachSocket = (candidate: Socket) => {
    socket = candidate;
    buffer = "";

    candidate.setNoDelay(true);
    candidate.setEncoding("utf8");

    candidate.on("data", (chunk: string) => {
      buffer = parseSocketData(buffer + chunk, (message) => {
        emitter.emit("message", message);
      });
    });

    candidate.on("error", () => {
      // Handled by close/reconnect path.
    });

    candidate.on("close", () => {
      if (socket === candidate) {
        socket = undefined;
        buffer = "";
      }

      if (!closed) {
        scheduleReconnect();
      }
    });
  };

  const connectOnce = (timeoutInMs: number): Promise<Socket> => {
    return new Promise<Socket>((resolve, reject) => {
      if (closed) {
        reject(new Error("Socket IPC is closed"));
        return;
      }

      const candidate = net.createConnection(socketPath);

      let settled = false;
      const finish = (fn: () => void) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timeout);
        candidate.removeAllListeners("connect");
        candidate.removeAllListeners("error");
        fn();
      };

      const timeout = setTimeout(() => {
        finish(() => {
          candidate.destroy();
          reject(
            new Error(`Timed out establishing child socket IPC connection (${timeoutInMs}ms)`)
          );
        });
      }, timeoutInMs);

      candidate.once("connect", () => {
        finish(() => {
          attachSocket(candidate);
          resolve(candidate);
        });
      });

      candidate.once("error", (error) => {
        finish(() => {
          candidate.destroy();
          reject(error);
        });
      });
    });
  };

  const connect = async (
    attemptTimeoutInMs: number = defaultConnectAttemptTimeoutInMs
  ): Promise<Socket> => {
    if (socket && !socket.destroyed) {
      return socket;
    }

    if (connectingPromise) {
      return connectingPromise;
    }

    connectingPromise = connectOnce(attemptTimeoutInMs).finally(() => {
      connectingPromise = undefined;
    });
    return connectingPromise;
  };

  const waitForConnectedSocket = async (timeoutInMs = 2_000): Promise<Socket> => {
    const startedAt = Date.now();
    let lastError: unknown;

    while (Date.now() - startedAt < timeoutInMs) {
      const remainingMs = timeoutInMs - (Date.now() - startedAt);
      const attemptTimeoutInMs = Math.max(50, Math.min(defaultConnectAttemptTimeoutInMs, remainingMs));

      try {
        const connectedSocket = await connect(attemptTimeoutInMs);
        if (!connectedSocket.destroyed) {
          return connectedSocket;
        }
      } catch (error) {
        lastError = error;
      }

      if (remainingMs > 0) {
        await new Promise((resolve) => setTimeout(resolve, Math.min(100, remainingMs)));
      }
    }

    const suffix = lastError ? `: ${String(lastError)}` : "";
    throw new Error(`Timed out waiting for child socket IPC reconnection (${timeoutInMs}ms)${suffix}`);
  };

  // Parent sends first message (EXECUTE_TASK_RUN), so the child must proactively connect.
  void connect().catch(() => {
    scheduleReconnect();
  });

  return {
    send: async (message: any) => {
      if (closed) {
        throw new Error("Socket IPC is closed");
      }

      let connectedSocket = await waitForConnectedSocket(2_000);

      if (closed || connectedSocket.destroyed) {
        throw new Error("Socket IPC is not connected");
      }

      try {
        await writePacket(connectedSocket, message);
      } catch (error) {
        if (!isRecoverableSocketWriteError(error)) {
          throw error;
        }

        destroySocket(connectedSocket);
        scheduleReconnect();
        connectedSocket = await waitForConnectedSocket(2_000);
        await writePacket(connectedSocket, message);
      }
    },
    on: (_event: "message", listener: MessageListener) => {
      emitter.on("message", listener);
    },
    resetConnection: async (timeoutInMs = 2_000) => {
      if (closed) {
        throw new Error("Socket IPC is closed");
      }

      if (socket && !socket.destroyed) {
        destroySocket(socket);
      }

      await waitForConnectedSocket(timeoutInMs);
    },
    close: async () => {
      closed = true;

      clearReconnectTimer();

      if (socket && !socket.destroyed) {
        destroySocket(socket);
      }
    },
  };
}
