import { ManagedRunController } from "./controller.js";

type Handler = (...args: any[]) => any;

type MockSocket = {
  on: ReturnType<typeof vi.fn>;
  emit: ReturnType<typeof vi.fn>;
  removeAllListeners: ReturnType<typeof vi.fn>;
  disconnect: ReturnType<typeof vi.fn>;
  close: ReturnType<typeof vi.fn>;
  handlers: Record<string, Handler>;
};

const ioMock = vi.hoisted(() => vi.fn());

vi.mock("socket.io-client", () => {
  return {
    io: ioMock,
  };
});

function createMockSocket(): MockSocket {
  const handlers: Record<string, Handler> = {};

  return {
    handlers,
    on: vi.fn((event: string, handler: Handler) => {
      handlers[event] = handler;
      return undefined;
    }),
    emit: vi.fn(),
    removeAllListeners: vi.fn(),
    disconnect: vi.fn(),
    close: vi.fn(),
  };
}

function createControllerHarness({
  runFriendlyId = "run_123",
  snapshotFriendlyId = "snap_123",
  processEnvOverridesResult,
}: {
  runFriendlyId?: string;
  snapshotFriendlyId?: string;
  processEnvOverridesResult?: Awaited<
    ReturnType<NonNullable<any["processEnvOverrides"]>>
  >;
} = {}) {
  const sockets: MockSocket[] = [];

  ioMock.mockImplementation(() => {
    const socket = createMockSocket();
    sockets.push(socket);
    return socket;
  });

  const controller = Object.create(ManagedRunController.prototype) as any;
  controller.env = {
    TRIGGER_DEPLOYMENT_ID: "deployment_123",
    TRIGGER_RUNNER_ID: "runner_123",
    TRIGGER_SUPERVISOR_API_URL: "https://example.com",
  };
  controller.currentExecution = runFriendlyId
    ? {
        runFriendlyId,
        currentSnapshotFriendlyId: snapshotFriendlyId,
        processEnvOverrides: vi.fn().mockResolvedValue(processEnvOverridesResult),
      }
    : null;
  controller.sendDebugLog = vi.fn();
  controller.subscribeToRunNotifications = vi.fn();

  return { controller, sockets };
}

describe("ManagedRunController socket reconnect behavior", () => {
  beforeEach(() => {
    ioMock.mockReset();
  });

  it("re-subscribes when the socket reconnects and there is an active run", async () => {
    const { controller, sockets } = createControllerHarness();

    controller.createSupervisorSocket();
    expect(sockets).toHaveLength(1);

    await sockets[0]!.handlers.connect();

    expect(controller.subscribeToRunNotifications).toHaveBeenCalledWith("run_123", "snap_123");
  });

  it("forces socket recreation on ping timeout when env overrides cannot be fetched", async () => {
    const { controller, sockets } = createControllerHarness({
      processEnvOverridesResult: undefined,
    });

    controller.createSupervisorSocket();
    expect(sockets).toHaveLength(1);

    await sockets[0]!.handlers.disconnect("ping timeout");

    expect(sockets[0]!.removeAllListeners).toHaveBeenCalled();
    expect(sockets[0]!.disconnect).toHaveBeenCalled();
    expect(sockets).toHaveLength(2);
    expect(controller.subscribeToRunNotifications).toHaveBeenCalledWith("run_123", "snap_123");
  });

  it("recreates socket when restore-related env changes are detected", async () => {
    const { controller, sockets } = createControllerHarness({
      processEnvOverridesResult: {
        runnerIdChanged: true,
        supervisorChanged: false,
      },
    });

    controller.createSupervisorSocket();
    expect(sockets).toHaveLength(1);

    await sockets[0]!.handlers.disconnect("transport close");

    expect(sockets[0]!.removeAllListeners).toHaveBeenCalled();
    expect(sockets[0]!.disconnect).toHaveBeenCalled();
    expect(sockets).toHaveLength(2);
    expect(controller.subscribeToRunNotifications).toHaveBeenCalledWith("run_123", "snap_123");
  });
});

