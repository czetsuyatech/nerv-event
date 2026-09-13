# Production guide

Use PostgreSQL, apply the canonical migrations through deployment tooling, and run Hibernate with `ddl-auto=validate`. The runtime application should not need DDL rights. Monitor `FAILED` rows, Outbox backlog, retry-pending age, broker errors, and retention activity.

## Guarantees and transactions

NERV provides **at-least-once delivery**, not distributed exactly-once processing. The producer's business update and Outbox save are one application transaction. Outbox claims and marks use short independent transactions; broker/network I/O is outside them. Inbox registration and claim are short transactions, followed by one processing transaction containing the handler's database-local work and the `PROCESSED` transition. Handler failure rolls both back before retry/failure metadata is recorded separately. Register a core `RetryPolicy` bean or a custom `OutboxDispatcher`; startup fails instead of silently accepting Outbox rows without a dispatch path. Consumer-only services should set `nerv.event.outbox.enabled=false`.

Important windows remain:

- Broker publish succeeds but persisting `PUBLISHED` fails or the process crashes: the Outbox may publish again.
- An Outbox worker publishes, loses its lease, and resumes after another worker reclaims and republishes: its stale database transition is fenced, but both broker publications may already have happened.
- An external handler effect succeeds but its database transaction or `PROCESSED` transition rolls back: the broker can redeliver, so external effects still require idempotency.
- `PROCESSED` is persisted but broker acknowledgement/delete fails: the broker can redeliver, and Inbox state suppresses ordinary handler execution.

Make external effects idempotent (provider idempotency keys, unique constraints, or application-side deduplication). Inbox is durable protection, not a distributed transaction coordinator.

## Operations at scale

Multiple pods are safe without leader election: correctness relies on database claims, processing owners, Outbox claim-version fencing, leases, Kafka consumer groups, and SQS visibility. An Outbox lease lets another worker recover abandoned work; its monotonic fencing token prevents the previous worker from changing durable state after ownership moves. Inbox idempotency protects consumers from duplicate broker publication. Treat broker publication, acknowledgement, claim expiry, and process restart as at-least-once paths, and make handlers idempotent. Tune batches, lease durations, and adaptive polling to the service's latency and database capacity; leases must exceed worst-case claimed work. For SQS, set visibility timeout above the complete processing path and the Inbox lease because visibility is not extended. Configure Kafka poll/heartbeat limits above worst-case handler duration. Broker send timeouts are ambiguous and can produce duplicate publication.

Optional ordering keys serialize claims only within the same key; unrelated keys remain concurrent. Kafka maps the key to a partition key, and SQS FIFO maps it to a message group. This is dispatch sequencing, not global ordering or exactly-once delivery. Standard SQS offers no ordering guarantee.

Retention is disabled by default. Enable it only after selecting an approved retention window; it deletes successful terminal records and limits historical inspection. Configure Outbox and Inbox backlog health thresholds from the service SLO, alert before they are exceeded, and include scheduler health in readiness monitoring. Protect operations endpoints, use payload exposure sparingly, and expose health/metrics to your normal monitoring system. Ensure graceful shutdown lets broker containers and in-flight handlers stop according to the host application's lifecycle.

## Known limitations

- PostgreSQL is the reference/tested production database; other databases are unverified.
- Auto-configuration assumes one default `DataSource`/`EntityManagerFactory`; multiple persistence units require explicit application configuration.
- The default Outbox dispatcher requires an application `RetryPolicy`; enabled publishing without a functional dispatcher fails startup.
- Standard SQS queues are supported; FIFO consumer queues are not.
- No replay or permanent event-store capability exists.
- At-least-once semantics require idempotent application behavior.
- Slow SQS handlers need a suitably large visibility timeout.
- Retention removes successful history; failed rows require operational policy/manual recovery.
