import { env as stdEnv } from "std-env";
import { readJSONFile } from "../utilities/fileSystem.js";
import { WorkerManifest } from "@basicblock/trigger-core/v3";
import { ManagedRunController } from "./managed/controller.js";

const manifest = await readJSONFile("./index.json");
const workerManifest = WorkerManifest.parse(manifest);

new ManagedRunController({
  workerManifest,
  env: stdEnv,
}).start();
