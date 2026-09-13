# Database and migrations

PostgreSQL is the tested production database. Canonical SQL lives in `nerv-event-persistence-jpa/src/main/resources/META-INF/nerv-event/db/postgresql/migration/`:

- `001-create-outbox.sql` creates `nerv_outbox_event`.
- `002-create-inbox.sql` creates `nerv_inbox_event`.
- `003-create-trace-context.sql` creates `nerv_event_trace_context`.
- `004-create-indexes.sql` creates claim, lease, retention, and search indexes.
- `005-add-outbox-claim-version.sql` adds the durable Outbox fencing token.
- `006-add-outbox-ordering-key.sql` adds the nullable ordering key and keyed-sequence claim index.

Applications own migration execution. Copy the scripts into an application-controlled Flyway version namespace, configure an explicit safe location, or include them from an application-owned Liquibase `sqlFile` changeset. Do not add the NERV directory blindly to Flyway defaults because numeric versions can collide. Released files are immutable; later upgrades require a new higher-numbered migration. Runtime identities should not require DDL privileges. Use Hibernate `validate` in production—never `update`, `create`, or `create-drop`.

When upgrading an existing 1.x database to 2.0, apply
`005-add-outbox-claim-version.sql` before deploying 2.0 application instances. The migration adds
`claim_version bigint not null default 0`, so existing rows remain valid and receive their first
positive fencing token when next claimed. Do not deploy 1.x and 2.0 workers concurrently: 1.x
workers do not participate in claim-version fencing. See [Upgrading to 2.0](upgrading-to-2.0.md).

Apply `006-add-outbox-ordering-key.sql` before deploying code that publishes ordering keys. The nullable column keeps existing rows and unkeyed publications compatible.

Useful PostgreSQL queries:

```sql
select id, event_id, event_type, destination, attempt_count, claim_version, last_error
from nerv_outbox_event where status = 'FAILED' order by updated_at desc;

select event_id, event_type, attempt_count, last_error
from nerv_inbox_event where status = 'FAILED' order by updated_at desc;

select event_id, available_at, attempt_count
from nerv_inbox_event where status = 'RETRY_PENDING' order by available_at;

select id, event_id, destination, available_at
from nerv_outbox_event where status = 'PENDING' order by available_at;
```

`nerv_event_trace_context` stores an EventId-keyed tracing sidecar and intentionally has no foreign key: one EventId can have multiple Outbox publications. Payload and trace values are PostgreSQL `text`; timestamps use `timestamp with time zone`.

Retention removes only `PUBLISHED` Outbox and `PROCESSED` Inbox rows after their configured age. Failed, pending, processing, received, and retry-pending rows are protected. Trace sidecars are removed only when no remaining Outbox or Inbox row refers to the EventId.
