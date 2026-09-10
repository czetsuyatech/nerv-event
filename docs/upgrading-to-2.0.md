# Upgrading to 2.0

Version 2.0.0 introduces lease-expiration fencing for Outbox processing. It is a major release
because the public `OutboxService` post-claim transition contract is source-incompatible with 1.x.

## Required database migration

Before starting any 2.0 instance, apply the packaged PostgreSQL migration
`005-add-outbox-claim-version.sql` through your application-owned migration process. If your
Flyway namespace already uses version 5, copy the canonical SQL under the next available
application version without changing its statements.

The migration adds:

```sql
claim_version bigint not null default 0
```

Existing rows remain valid. Each claim or expired-lease reclaim increments the value, and every
worker-owned transition must match the current token.

Do not run 1.x and 2.0 Outbox workers at the same time. Stop the 1.x workers, apply the migration,
then deploy 2.0. A rolling deployment with mixed versions would leave 1.x workers outside the new
fencing protocol.

## `OutboxService` API changes

The 1.x transition methods accepted no claim token and returned `void`. In 2.0 they require the
`claimVersion` returned on the claimed `OutboxEvent` and return whether the guarded transition was
applied:

```java
boolean markPublished(OutboxId id, long claimVersion, BrokerPublishResult result);

boolean reschedule(
    OutboxId id,
    long claimVersion,
    int attemptCount,
    Instant nextAttemptAt,
    String failureReason
);

boolean markFailed(
    OutboxId id,
    long claimVersion,
    int attemptCount,
    String failureReason
);
```

Update custom `OutboxService` implementations, decorators, test doubles, and direct callers. Pass
the token from the exact `OutboxEvent` being processed; do not reread or synthesize it. Treat a
`false` result as a fenced or otherwise unresolved transition. The current dispatcher records that
outcome as unresolved and does not overwrite the newer claim.

`OutboxEvent` now exposes `lockedBy` and `claimVersion`. The legacy six-argument constructor
remains available and initializes these fields to `null` and `0`, but events returned by
`claimPending` contain the persisted owner and positive claim token.

## Operations and behavior

Outbox operation details now expose `claimVersion`, making ownership changes visible during
support and incident investigation. Normal publication, retry, retention, and Inbox behavior is
unchanged.

Fencing protects durable Outbox state; it does not make broker delivery exactly once. If a worker
publishes and then loses its lease before recording success, a newer worker may publish the event
again. Consumers must remain idempotent.

## Verification

After upgrading:

1. Verify migration 005 was recorded by the application's migration tool.
2. Start only 2.0 workers and confirm newly claimed rows have `claim_version > 0`.
3. Run the full integration and resilience profiles.
4. Confirm operations responses include `claimVersion` for Outbox records.
