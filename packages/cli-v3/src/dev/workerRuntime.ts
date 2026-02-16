import { BuildManifest } from "@basicblock/trigger-core/v3";
import { ResolvedConfig } from "@basicblock/trigger-core/v3/build";
import { CliApiClient } from "../apiClient.js";
import { DevCommandOptions } from "../commands/dev.js";
import type { Metafile } from "esbuild";

export interface WorkerRuntime {
  shutdown(): Promise<void>;
  initializeWorker(manifest: BuildManifest, metafile: Metafile, stop: () => void): Promise<void>;
}

export type WorkerRuntimeOptions = {
  name: string | undefined;
  config: ResolvedConfig;
  args: DevCommandOptions;
  client: CliApiClient;
  dashboardUrl: string;
};
