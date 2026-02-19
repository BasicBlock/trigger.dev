import { setTimeout as sleep } from "timers/promises";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

const DEFAULT_ARTIFACTS_DIR =
  process.env.DOCKER_TEST_ARTIFACTS_DIR ?? join(process.cwd(), "docker-test-artifacts");

export async function waitForHttpOk(params: {
  url: string;
  timeoutMs?: number;
  pollIntervalMs?: number;
}): Promise<void> {
  const timeoutMs = params.timeoutMs ?? 90_000;
  const pollIntervalMs = params.pollIntervalMs ?? 1_000;
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    try {
      const response = await fetch(params.url);
      if (response.ok) {
        return;
      }
    } catch {
      // Keep polling while service is starting.
    }

    await sleep(pollIntervalMs);
  }

  throw new Error(`Timed out waiting for HTTP 200 from ${params.url} after ${timeoutMs}ms`);
}

export async function shell(command: string): Promise<string> {
  const { stdout } = await execFileAsync("bash", ["-lc", command], {
    encoding: "utf8",
    maxBuffer: 10 * 1024 * 1024,
  });

  return stdout;
}

export async function shellWithStderr(command: string): Promise<string> {
  const { stdout, stderr } = await execFileAsync("bash", ["-lc", command], {
    encoding: "utf8",
    maxBuffer: 10 * 1024 * 1024,
  });

  return `${stdout}${stderr}`;
}

export async function withDiagnostics<T>(params: { testName: string }, fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    const artifactDir = await collectDiagnostics(params.testName);
    console.error(`docker diagnostics captured at ${artifactDir}`);
    throw error;
  }
}

export type WaitpointConfig = {
  apiUrl: string;
  apiKey: string;
  taskId: string;
};

export async function resolveWaitpointConfig(): Promise<WaitpointConfig> {
  const apiUrl =
    process.env.DOCKER_WAIT_API_URL ??
    process.env.DOCKER_INTEGRATION_WEBAPP_URL ??
    "http://127.0.0.1:3030";
  const envSlug = process.env.DOCKER_WAIT_ENV_SLUG ?? "prod";
  let projectRef = process.env.DOCKER_WAIT_PROJECT_REF?.trim() ?? "";

  let apiKey = process.env.DOCKER_WAIT_API_KEY?.trim() ?? "";
  let taskId = process.env.DOCKER_WAIT_TASK_ID?.trim() ?? "";

  if (!taskId) {
    taskId = (await discoverFromPostgres(
      "SELECT slug FROM \"BackgroundWorkerTask\" WHERE slug = 'test.wait' LIMIT 1;"
    ))?.trim() ?? "";
  }

  if (!projectRef && taskId) {
    projectRef = (await discoverFromPostgres(
      `SELECT p."externalRef"
       FROM "BackgroundWorkerTask" bwt
       INNER JOIN "Project" p ON p.id = bwt."projectId"
       WHERE bwt.slug = '${escapeSqlLiteral(taskId)}'
       LIMIT 1;`
    ))?.trim() ?? "";
  }

  if (apiKey && projectRef) {
    const isValid = await isApiKeyValidForProjectEnv({
      apiUrl,
      apiKey,
      projectRef,
      envSlug,
    });

    if (!isValid) {
      apiKey = "";
    }
  }

  if (!apiKey && taskId) {
    const keyFromTaskProject = (await discoverFromPostgres(
      `SELECT re."apiKey"
       FROM "BackgroundWorkerTask" bwt
       INNER JOIN "RuntimeEnvironment" re ON re."projectId" = bwt."projectId"
       WHERE bwt.slug = '${escapeSqlLiteral(taskId)}'
         AND re."apiKey" IS NOT NULL
         AND re."apiKey" <> ''
       ORDER BY re."createdAt" DESC
       LIMIT 50;`
    ))?.trim();

    if (keyFromTaskProject) {
      if (!projectRef) {
        apiKey = keyFromTaskProject;
      } else {
        for (const candidate of keyFromTaskProject.split("\n").map((line) => line.trim())) {
          if (!candidate) continue;
          const isValid = await isApiKeyValidForProjectEnv({
            apiUrl,
            apiKey: candidate,
            projectRef,
            envSlug,
          });
          if (isValid) {
            apiKey = candidate;
            break;
          }
        }
      }
    }
  }

  if (!apiKey) {
    const anyKeys =
      (await discoverFromPostgres(
        'SELECT "apiKey" FROM "RuntimeEnvironment" WHERE "apiKey" IS NOT NULL AND "apiKey" <> \'\' ORDER BY "createdAt" DESC LIMIT 50;'
      )) ?? "";

    if (!projectRef) {
      apiKey = anyKeys.split("\n").map((line) => line.trim()).find(Boolean) ?? "";
    } else {
      for (const candidate of anyKeys.split("\n").map((line) => line.trim())) {
        if (!candidate) continue;
        const isValid = await isApiKeyValidForProjectEnv({
          apiUrl,
          apiKey: candidate,
          projectRef,
          envSlug,
        });
        if (isValid) {
          apiKey = candidate;
          break;
        }
      }
    }
  }

  if (!taskId) {
    taskId = (await discoverFromPostgres(
      "SELECT slug FROM \"BackgroundWorkerTask\" WHERE slug ILIKE '%wait%' ORDER BY \"createdAt\" DESC LIMIT 1;"
    ))?.trim() ?? "";
  }

  const missing: string[] = [];
  if (!apiUrl) missing.push("DOCKER_WAIT_API_URL");
  if (!apiKey) missing.push("DOCKER_WAIT_API_KEY");
  if (!taskId) missing.push("DOCKER_WAIT_TASK_ID");

  if (missing.length > 0) {
    throw new Error(
      `Missing required waitpoint inputs (${missing.join(
        ", "
      )}). Set env vars directly or run through scripts/docker-integration/run.sh.`
    );
  }

  return { apiUrl, apiKey, taskId };
}

async function discoverFromPostgres(sql: string): Promise<string | undefined> {
  const explicit = process.env.DOCKER_INTEGRATION_POSTGRES_CONTAINER;
  const candidates = explicit ? [explicit] : ["db-dev", "database"];

  for (const container of candidates) {
    const value = await queryPostgres(container, "main", sql);
    if (value) return value;

    const fallback = await queryPostgres(container, "postgres", sql);
    if (fallback) return fallback;
  }

  return undefined;
}

async function queryPostgres(container: string, db: string, sql: string): Promise<string | undefined> {
  const quotedSql = shellSingleQuote(sql);
  const command = `PGPASSWORD=postgres psql -U postgres -d ${db} -tAc ${quotedSql}`;

  try {
    const { stdout } = await execFileAsync("docker", ["exec", container, "sh", "-lc", command], {
      encoding: "utf8",
      maxBuffer: 5 * 1024 * 1024,
    });

    const lines = stdout
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);

    if (lines.length === 0) {
      return undefined;
    }

    return lines.join("\n");
  } catch {
    return undefined;
  }
}

function shellSingleQuote(input: string): string {
  return `'${input.replace(/'/g, `'\\''`)}'`;
}

function escapeSqlLiteral(input: string): string {
  return input.replace(/'/g, "''");
}

async function isApiKeyValidForProjectEnv(params: {
  apiUrl: string;
  apiKey: string;
  projectRef: string;
  envSlug: string;
}): Promise<boolean> {
  try {
    const response = await fetch(
      `${params.apiUrl}/api/v1/projects/${encodeURIComponent(params.projectRef)}/${encodeURIComponent(
        params.envSlug
      )}`,
      {
        method: "GET",
        headers: {
          Authorization: `Bearer ${params.apiKey}`,
        },
      }
    );

    return response.status === 200;
  } catch {
    return false;
  }
}

async function collectDiagnostics(testName: string): Promise<string> {
  const timestamp = new Date().toISOString().replaceAll(":", "-");
  const artifactDir = join(DEFAULT_ARTIFACTS_DIR, `${sanitizeFileName(testName)}-${timestamp}`);
  await mkdir(artifactDir, { recursive: true });

  await writeCommandOutput(artifactDir, "docker-version.txt", "docker version");
  await writeCommandOutput(artifactDir, "docker-ps.txt", "docker ps");
  await writeCommandOutput(artifactDir, "docker-ps-a.txt", "docker ps -a");

  const composeProject = process.env.DOCKER_INTEGRATION_COMPOSE_PROJECT ?? "triggerdotdev-dev-docker";
  const composeFile = process.env.DOCKER_INTEGRATION_COMPOSE_FILE ?? "docker/dev-compose.yml";
  await writeCommandOutput(
    artifactDir,
    "docker-compose-logs.txt",
    `docker compose -p ${composeProject} -f ${composeFile} logs --tail=300`
  );

  const supervisorContainer = process.env.DOCKER_WAIT_SUPERVISOR_CONTAINER;
  if (supervisorContainer) {
    await writeCommandOutput(
      artifactDir,
      "supervisor-container-logs.txt",
      `docker logs --since 30m ${supervisorContainer}`
    );
  }

  return artifactDir;
}

async function writeCommandOutput(artifactDir: string, fileName: string, command: string): Promise<void> {
  const output = await tryShellWithStderr(command);
  await writeFile(join(artifactDir, fileName), output, "utf8");
}

async function tryShellWithStderr(command: string): Promise<string> {
  try {
    return await shellWithStderr(command);
  } catch (error) {
    return stringifyError(error);
  }
}

function sanitizeFileName(input: string): string {
  return input.toLowerCase().replace(/[^a-z0-9]+/g, "-");
}

function stringifyError(error: unknown): string {
  if (error instanceof Error) {
    return `${error.name}: ${error.message}\n${error.stack ?? ""}`;
  }

  return String(error);
}
