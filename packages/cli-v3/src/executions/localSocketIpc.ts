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

export function createParentSocketIpcProcess(
  socketPath: string,
  options: ParentSocketIpcOptions = {}
): IpcProcessLike {
  const emitter = new EventEmitter();
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
        server?.off("connection", onConnection);
      };

      server?.on("connection", onConnection);
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

  const connect = async (): Promise<Socket> => {
    if (socket && !socket.destroyed) {
      return socket;
    }

    if (connectingPromise) {
      return connectingPromise;
    }

    connectingPromise = new Promise<Socket>((resolve, reject) => {
      const attempt = () => {
        if (closed) {
          connectingPromise = undefined;
          reject(new Error("Socket IPC is closed"));
          return;
        }

        const candidate = net.createConnection(socketPath);

        candidate.once("connect", () => {
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
              reconnectTimer = setTimeout(() => {
                void connect().catch(() => {
                  // connect() already retries until closed.
                });
              }, 100);
            }
          });

          connectingPromise = undefined;
          resolve(candidate);
        });

        candidate.once("error", () => {
          candidate.destroy();
          setTimeout(attempt, 100);
        });
      };

      attempt();
    });

    return connectingPromise;
  };

  const waitForConnectedSocket = async (timeoutInMs = 2_000): Promise<Socket> => {
    const startedAt = Date.now();

    while (Date.now() - startedAt < timeoutInMs) {
      const connectedSocket = await connect();
      if (!connectedSocket.destroyed) {
        return connectedSocket;
      }

      await new Promise((resolve) => setTimeout(resolve, 25));
    }

    throw new Error(`Timed out waiting for child socket IPC reconnection (${timeoutInMs}ms)`);
  };

  // Parent sends first message (EXECUTE_TASK_RUN), so the child must proactively connect.
  void connect().catch(() => {
    // connect() already retries until closed.
  });

  return {
    send: async (message: any) => {
      if (closed) {
        throw new Error("Socket IPC is closed");
      }

      const connectedSocket = await connect();

      if (closed || connectedSocket.destroyed) {
        throw new Error("Socket IPC is not connected");
      }

      await writePacket(connectedSocket, message);
    },
    on: (_event: "message", listener: MessageListener) => {
      emitter.on("message", listener);
    },
    resetConnection: async (timeoutInMs = 2_000) => {
      if (closed) {
        throw new Error("Socket IPC is closed");
      }

      if (socket && !socket.destroyed) {
        socket.destroy();
      }
      socket = undefined;
      buffer = "";

      await waitForConnectedSocket(timeoutInMs);
    },
    close: async () => {
      closed = true;

      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
      }

      if (socket && !socket.destroyed) {
        socket.destroy();
      }
    },
  };
}
