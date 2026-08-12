# Inbox and Outbox

## Outbox

Publishing creates a `PENDING` durable row. When an application supplies the required core `RetryPolicy`, workers atomically claim eligible rows with an owner and lease, changing them to `PROCESSING`. A broker acknowledgement moves the row to `PUBLISHED`. A send failure is returned to `PENDING` while that policy permits it, or becomes `FAILED` when exhausted. `attemptCount`, `availableAt`, `lockedAt`, and `lockedBy` make retry and lease recovery visible.

Multiple pods may run workers. Short database claims and leases prevent simultaneous active ownership; no leader election or ShedLock is required. Delivery is at-least-once: a crash after broker acknowledgement but before `PUBLISHED` is durably recorded can cause a later duplicate send.

## Inbox

The Inbox is keyed by `EventId`, so it has one durable row per logical inbound event. Its states are:

- `RECEIVED` — persisted from the broker, ready to claim.
- `PROCESSING` — owned by a worker under a lease.
- `RETRY_PENDING` — a retryable handler failure is scheduled for `availableAt`.
- `PROCESSED` — handler completion is durably recorded.
- `FAILED` — terminal automatic-processing outcome.

The normal path is `RECEIVED -> PROCESSING -> PROCESSED`. A retryable failure follows `PROCESSING -> RETRY_PENDING -> PROCESSING`; other failures follow `PROCESSING -> FAILED`. A new processor can reclaim an expired `PROCESSING` lease. Redelivery with an already durable terminal/retry row is suppressed and acknowledged/deleted after the durable state is known. `FAILED` is terminal for automatic processing.

The broker acknowledgement happens only after Inbox registration and a durable outcome. If that acknowledgement fails, redelivery remains possible; the Inbox prevents ordinary duplicate handler execution, but handlers must still be idempotent for external effects.
