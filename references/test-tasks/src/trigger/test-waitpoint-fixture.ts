import { configure, logger, task, wait } from "@basicblock/trigger-sdk/v3";

export const testWaitpointFixture = task({
  id: "test.wait",
  retry: {
    maxAttempts: 1,
  },
  run: async ({ waitTimeSec = 10 }: { waitTimeSec?: number }) => {
    const taskApiUrl = process.env.DOCKER_WAIT_TASK_API_URL;
    const secretKey = process.env.TRIGGER_SECRET_KEY;
    if (taskApiUrl && secretKey) {
      process.env.TRIGGER_API_URL = taskApiUrl;
      process.env.TRIGGER_STREAM_URL = taskApiUrl;
      configure({ baseURL: taskApiUrl, accessToken: secretKey });
    }

    const seconds = Number.isFinite(waitTimeSec) ? Math.max(1, Math.floor(waitTimeSec)) : 10;

    logger.info("WAIT_START_MARKER", { waitTimeSec: seconds });
    await wait.for({ seconds });
    logger.info("WAIT_COMPLETE_MARKER", { waitTimeSec: seconds });

    return { waitedSeconds: seconds };
  },
});
