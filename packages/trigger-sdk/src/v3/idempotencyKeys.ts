import { createIdempotencyKey, resetIdempotencyKey, type IdempotencyKey } from "@basicblock/trigger-core/v3";

export const idempotencyKeys = {
  create: createIdempotencyKey,
  reset: resetIdempotencyKey,
};

export type { IdempotencyKey };
