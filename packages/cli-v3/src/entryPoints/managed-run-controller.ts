import { readFile } from "node:fs/promises";
import { readJSONFile } from "../utilities/fileSystem.js";
import { WorkerManifest } from "@basicblock/trigger-core/v3";
import { ManagedRunController } from "./managed/controller.js";

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

await loadMountedEnvFile();

const manifest = await readJSONFile("./index.json");
const workerManifest = WorkerManifest.parse(manifest);

new ManagedRunController({
  workerManifest,
  env: process.env,
}).start();
