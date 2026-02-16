import { defineConfig } from "@basicblock/trigger-sdk/v3";
import { emitDecoratorMetadata } from "@basicblock/trigger-build/extensions/typescript";

export default defineConfig({
  project: "<fixture project>",
  dirs: ["./src/trigger"],
  build: {
    extensions: [emitDecoratorMetadata()],
  },
  maxDuration: 3600,
});
