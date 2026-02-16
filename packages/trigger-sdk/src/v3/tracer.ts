import { TriggerTracer } from "@basicblock/trigger-core/v3/tracer";
import { VERSION } from "../version.js";

export const tracer = new TriggerTracer({ name: "@basicblock/trigger-sdk", version: VERSION });
