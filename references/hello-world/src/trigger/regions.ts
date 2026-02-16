import { task } from "@basicblock/trigger-sdk";
import { fixedLengthTask } from "./batches.js";

export const regionsTask = task({
  id: "regions",
  run: async ({ region }: { region?: string }, { ctx }) => {
    await fixedLengthTask.triggerAndWait({ waitSeconds: 1 }, { region });
  },
});
