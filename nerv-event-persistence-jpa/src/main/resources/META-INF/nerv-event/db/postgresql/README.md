# Nerv Event PostgreSQL migrations

These numbered SQL files are the canonical, PostgreSQL-only schema for the JPA persistence module.
They are packaged under `META-INF/nerv-event/db/postgresql/migration/` specifically so they are
not discovered automatically by Flyway. Execute them once, in lexical order, using the consuming
application's migration process. Nerv Event never executes them and has no Flyway or Liquibase
runtime dependency.

For a new database, execute `001` through `004`. The tables are `nerv_outbox_event`,
`nerv_inbox_event`, and `nerv_event_trace_context`; no PostgreSQL schema is hard-coded.

Migration files are immutable once released. Later schema changes must be represented by a new,
higher-numbered file. Applications with pre-existing Hibernate-created tables should first compare
their deployed schema with these files, make any required application-owned reconciliation migration,
then record this release as their adopted baseline. Nerv Event does not alter unknown schemas.

The indexes support claims and lease recovery (`status` with `available_at` or lock/processing time),
successful-record retention (`status` with published/processed time), EventId lookup, and status-filtered
operations pages ordered by update time. The trace-context primary key supports lookup and orphan cleanup.
The sidecar intentionally has no foreign key: one EventId can be represented by several Outbox rows.
