import { defineConfig } from "@basicblock/trigger-sdk";
import { pythonExtension } from "@basicblock/trigger-python/extension";
import { installPlaywrightChromium } from "./src/extensions/playwright";

export default defineConfig({
  project: "proj_wkbbtayxrmeyqhankehb",
  dirs: ["./src/trigger"],
  maxDuration: 3600,
  build: {
    extensions: [
      // This is required to use the Python extension
      pythonExtension({
        requirementsFile: "./requirements.txt", // Optional: Path to your requirements file
        devPythonBinaryPath: `.venv/bin/python`, // Optional: Custom Python binary path
        scripts: ["src/trigger/python/**/*.py"], // List of Python scripts to include
      }),
      installPlaywrightChromium(),
    ],
  },
});
