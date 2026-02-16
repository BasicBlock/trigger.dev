import { traceContext } from "@basicblock/trigger-core/v3";

export const otel = {
  withExternalTrace: <T>(fn: () => T): T => {
    return traceContext.withExternalTrace(fn);
  },
};
