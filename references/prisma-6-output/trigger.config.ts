import { defineConfig } from "@basicblock/trigger-sdk";
import { prismaExtension } from "@basicblock/trigger-build/extensions/prisma";

export default defineConfig({
  project: process.env.TRIGGER_PROJECT_REF!,
  logLevel: "debug",
  maxDuration: 3600,
  retries: {
    enabledInDev: true,
    default: {
      maxAttempts: 3,
      minTimeoutInMs: 1000,
      maxTimeoutInMs: 10000,
      factor: 2,
      randomize: true,
    },
  },
  machine: "small-1x",
  build: {
    extensions: [
      prismaExtension({
        mode: "engine-only",
        version: "6.19.0",
        binaryTarget: "linux-arm64-openssl-3.0.x",
      }),
    ],
  },
});
