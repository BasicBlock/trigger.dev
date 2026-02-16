import { defineConfig } from "@basicblock/trigger-sdk/v3";

export default defineConfig({
  project: "<fixture project>",
  dirs: ["./src/trigger"],
  build: {
    external: ["mupdf"],
  },
  maxDuration: 3600,
});
