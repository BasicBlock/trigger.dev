import { logger, task } from "@basicblock/trigger-sdk";

export const telemetryTestTask = task({
  id: "telemetry-test",
  run: async (payload: any, { ctx }) => {
    logger.info("Hello, world!", { payload, ctx });
    return { message: "Hello, world!" };
  },
});
