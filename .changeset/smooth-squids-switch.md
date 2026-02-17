---
"@basicblock/trigger-core": patch
---

Improve workload HTTP retry/telemetry behavior by preserving caller-provided retry options, adding structured per-request timing logs with connection-error context, and disabling retries for snapshot polling requests.
