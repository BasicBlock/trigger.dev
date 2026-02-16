import { schedules } from "@basicblock/trigger-sdk/v3";

export const simpleSchedule = schedules.task({
  id: "simple-schedule",
  cron: "0 0 * * *",
  run: async (payload, { ctx }) => {
    return {
      message: "Hello, world!",
    };
  },
});
