import { describe, it } from "vitest";
import { shell, waitForHttpOk, withDiagnostics } from "./harness.js";
import { isFullEnabled } from "./testMode.js";

const webappUrl = process.env.DOCKER_INTEGRATION_WEBAPP_URL ?? "http://127.0.0.1:3030";
const composeProject = process.env.DOCKER_INTEGRATION_COMPOSE_PROJECT ?? "triggerdotdev-dev-docker";
const composeFile = process.env.DOCKER_INTEGRATION_COMPOSE_FILE ?? "../../docker/dev-compose.yml";

const itFull = isFullEnabled() ? it : it.skip;

describe("docker integration redis recovery", () => {
  itFull(
    "recovers webapp health after redis restart",
    async () => {
      await withDiagnostics(
        {
          testName: "redis restart recovery",
        },
        async () => {
          await waitForHttpOk({ url: `${webappUrl}/healthcheck`, timeoutMs: 120_000 });

          await shell(
            `docker compose -p ${composeProject} -f ${composeFile} restart redis`
          );

          await waitForHttpOk({ url: `${webappUrl}/healthcheck`, timeoutMs: 120_000 });
        }
      );
    },
    180_000
  );
});
