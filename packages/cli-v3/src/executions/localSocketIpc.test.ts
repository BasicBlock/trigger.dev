import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  createChildSocketIpcProcess,
  createParentSocketIpcProcess,
} from "./localSocketIpc.js";

describe("localSocketIpc", () => {
  it("allows parent-first message flow without child sending first", async () => {
    const socketPath = path.join(os.tmpdir(), `trigger-ipc-test-${randomUUID()}.sock`);
    const parent = createParentSocketIpcProcess(socketPath, { connectTimeoutInMs: 2_000 });
    const child = createChildSocketIpcProcess(socketPath);

    const received = new Promise<any>((resolve) => {
      child.on("message", (message) => resolve(message));
    });

    try {
      const payload = { type: "EVENT", message: { type: "PING", value: "hello" } };
      await parent.send(payload);
      await expect(received).resolves.toEqual(payload);
    } finally {
      await child.close();
      await parent.close();
    }
  });
});
