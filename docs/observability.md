# Observability and tracing

When Micrometer is available and observability is enabled, metrics include:

- `nerv.event.outbox.dispatch.claimed`, `.events` (result tag), `.duration`, `.cycles` (result tag)
- `nerv.event.inbox.retry.claimed`, `.events` (result tag), `.duration`
- `nerv.event.consumer.received`, `.events` (result tag), `.handler.executions` (result tag), `.retries`, `.processing.duration` (result tag), `.in.progress`
- `nerv.event.broker.publish` and `.duration` (broker/result tags)
- `nerv.event.retention.deleted` (type tag), `.duration`, `.cycles` (result tag)

Tags are deliberately bounded. Never add `eventId`, `correlationId`, `OutboxId`, SQS message ID, Kafka partition/offset, payload, owner, or error text as metric tags; use logs and traces for those values.

Consumer metrics distinguish broker delivery from durable processing. `nerv.event.consumer.events` has only these
`result` values: `processed`, `retry_pending`, `failed`, `duplicate`, and `unresolved`. A received delivery normally
emits exactly one result. `processed`, `retry_pending`, and `failed` are emitted only after the corresponding Inbox
transition is durable. `duplicate` is emitted when an existing durable terminal or retry-pending Inbox row suppresses
handler execution. `unresolved` is emitted only when Inbox registration or claiming fails, a handler result cannot be
persisted, or retry classification cannot determine a durable outcome. Broker acknowledgement failures do not change a
previously confirmed outcome.

`nerv.event.consumer.handler.executions` and `nerv.event.consumer.processing.duration` measure actual handler
executions with `result=success|failure`; duplicate deliveries do not affect either. `nerv.event.consumer.retries`
counts durable Inbox retry scheduling, not broker redelivery. `nerv.event.consumer.in.progress` is the current number
of handler executions in progress.

Health support can report optional thresholds for pending Outbox backlog/age and retry-pending Inbox backlog/age. Thresholds are unset by default, so configure them deliberately for your service's SLO.

`EventId` identifies the logical event; `correlationId` is application-supplied business correlation; `traceId` belongs to tracing. They are not interchangeable. Outbox publication crosses an asynchronous boundary, so trace context is stored in the EventId-keyed `nerv_event_trace_context` sidecar instead of being added to core event models. The same EventId may have several Outbox publications. Sidecar cleanup follows retained event references. Kafka and SQS adapters propagate tracing when tracing is active; Inbox retries are separate processing attempts.
