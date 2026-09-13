# Inbox and Outbox

## Outbox

Publishing creates a `PENDING` durable row. When an application supplies the required core `RetryPolicy`, workers atomically claim eligible rows with an owner and lease, changing them to `PROCESSING`. Every claim or reclaim increments the durable `claimVersion` fencing token. A broker acknowledgement moves the row to `PUBLISHED`. A send failure is returned to `PENDING` while that policy permits it, or becomes `FAILED` when exhausted. `attemptCount`, `availableAt`, `lockedAt`, `lockedBy`, and `claimVersion` make retry and ownership visible.

An optional `orderingKey` is persisted with the row. A claim excludes a keyed row while any earlier `PENDING` or `PROCESSING` row has the same key, including across replicas and retry cycles. Other keys and unkeyed rows remain independently claimable. This guarantees Outbox dispatch sequence for a key, subject to at-least-once publication and transport-specific ordering.

Multiple pods may run workers. Short database claims and leases allow abandoned work to be recovered; the fencing token prevents an old worker from publishing a durable state transition after a newer claim has taken ownership. Every worker-owned transition from `PROCESSING` checks the row id, status, owner, and exact `claimVersion`. A rejected transition is reported as an unresolved dispatch outcome and does not modify the newer claim. No leader election or ShedLock is required.

Fencing protects Outbox database state, not broker exactly-once publication. A worker can publish successfully, stall until its lease expires, and then be fenced only after another worker publishes the same event. Delivery therefore remains at-least-once, and the Inbox/idempotency layer is responsible for making duplicate delivery safe.

## Inbox

The Inbox is keyed by `EventId`, so it has one durable row per logical inbound event. Its states are:

- `RECEIVED` — persisted from the broker, ready to claim.
- `PROCESSING` — owned by a worker under a lease.
- `RETRY_PENDING` — a retryable handler failure is scheduled for `availableAt`.
- `PROCESSED` — handler completion is durably recorded.
- `FAILED` — terminal automatic-processing outcome.

The normal path is `RECEIVED -> PROCESSING -> PROCESSED`. A retryable failure follows `PROCESSING -> RETRY_PENDING -> PROCESSING`; other failures follow `PROCESSING -> FAILED`. A new processor can reclaim an expired `PROCESSING` lease. Redelivery with an already durable terminal/retry row is suppressed and acknowledged/deleted after the durable state is known. `FAILED` is terminal for automatic processing.

The broker acknowledgement happens only after Inbox registration and a durable outcome. Handler database work that participates in NERV's transaction commits atomically with `PROCESSED`; failure rolls both back before retry metadata is recorded separately. If broker acknowledgement fails, redelivery remains possible. Handlers must still be idempotent for external effects, which cannot join the database transaction.
