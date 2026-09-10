# Production guide

Use PostgreSQL, apply the canonical migrations through deployment tooling, and run Hibernate with `ddl-auto=validate`. The runtime application should not need DDL rights. Monitor `FAILED` rows, Outbox backlog, retry-pending age, broker errors, and retention activity.

## Guarantees and transactions

NERV provides **at-least-once delivery**, not exactly-once business processing. The producer's business update and Outbox save are one application transaction. Claim/mark state changes use short independent transactions; broker/network I/O is outside them. Consumer registration/claim/state changes are also short transactions; handlers may define their own `@Transactional` scope. Register a core `RetryPolicy` bean before relying on automated Outbox dispatch; the current starter has no default.

Important windows remain:

- Broker publish succeeds but persisting `PUBLISHED` fails or the process crashes: the Outbox may publish again.
- An Outbox worker publishes, loses its lease, and resumes after another worker reclaims and republishes: its stale database transition is fenced, but both broker publications may already have happened.
- Handler business work succeeds but `PROCESSED` cannot be persisted: the broker can redeliver and work may run again.
- `PROCESSED` is persisted but broker acknowledgement/delete fails: the broker can redeliver, and Inbox state suppresses ordinary handler execution.

Make external effects idempotent (provider idempotency keys, unique constraints, or application-side deduplication). Inbox is durable protection, not a distributed transaction coordinator.

## Operations at scale

Multiple pods are safe without leader election: correctness relies on database claims, processing owners, Outbox claim-version fencing, leases, Kafka consumer groups, and SQS visibility. An Outbox lease lets another worker recover abandoned work; its monotonic fencing token prevents the previous worker from changing durable state after ownership moves. Inbox idempotency protects consumers from duplicate broker publication. Treat broker publication, acknowledgement, claim expiry, and process restart as at-least-once paths, and make handlers idempotent. Tune batches, lease durations, and adaptive polling to the service's latency and database capacity; leases must exceed worst-case claimed work. For SQS, set visibility timeout above the complete processing path and the Inbox lease because visibility is not extended. Configure Kafka poll/heartbeat limits above worst-case handler duration. Broker send timeouts are ambiguous and can produce duplicate publication.

Retention is disabled by default. Enable it only after selecting an approved retention window; it deletes successful terminal records and limits historical inspection. Configure Outbox and Inbox backlog health thresholds from the service SLO, alert before they are exceeded, and include scheduler health in readiness monitoring. Protect operations endpoints, use payload exposure sparingly, and expose health/metrics to your normal monitoring system. Ensure graceful shutdown lets broker containers and in-flight handlers stop according to the host application's lifecycle.

## Known limitations

- PostgreSQL is the reference/tested production database; other databases are unverified.
- Auto-configuration assumes one default `DataSource`/`EntityManagerFactory`; multiple persistence units require explicit application configuration.
- The Outbox dispatcher is conditional on an application `RetryPolicy`; no built-in Outbox retry properties/default exist yet.
- Standard SQS queues are supported; FIFO consumer queues are not.
- No replay or permanent event-store capability exists.
- At-least-once semantics require idempotent application behavior.
- Slow SQS handlers need a suitably large visibility timeout.
- Retention removes successful history; failed rows require operational policy/manual recovery.
