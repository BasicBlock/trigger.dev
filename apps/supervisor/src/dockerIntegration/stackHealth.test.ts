import { describe, expect, it } from "vitest";
import { shell, waitForHttpOk, withDiagnostics } from "./harness.js";
import { isSmokeEnabled } from "./testMode.js";

const webappUrl = process.env.DOCKER_INTEGRATION_WEBAPP_URL ?? "http://127.0.0.1:3030";
const supervisorUrl = process.env.DOCKER_INTEGRATION_SUPERVISOR_URL ?? "http://127.0.0.1:8020";
const itSmoke = isSmokeEnabled() ? it : it.skip;

describe("docker integration stack health", () => {
  itSmoke(
    "reaches webapp and supervisor health endpoints and talks to docker daemon",
    async () => {
      await withDiagnostics(
        {
          testName: "stack health endpoints and docker daemon",
        },
        async () => {
          await waitForHttpOk({ url: `${webappUrl}/healthcheck` });
          await waitForHttpOk({ url: `${supervisorUrl}/health` });

          const dockerVersion = await shell("docker version --format '{{.Server.Version}}'");
          expect(dockerVersion.trim().length).toBeGreaterThan(0);
        }
      );
    },
    120_000
  );
});
