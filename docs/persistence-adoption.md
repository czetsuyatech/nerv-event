# Persistence adoption notes

## Shared specifications

The operational inbox and outbox filters use `nerv-persistence` 1.0.0 through
`AbstractSpecificationsBuilder` and `GenericSpecification`. `InboxSpecifications`
and `OutboxSpecifications` preserve the previous exact-match and inclusive `Instant`
range predicates; paging and newest-updated-first ordering remain in the operations
services.

## Intentionally unchanged entity and model inheritance

`InboxEventEntity`, `OutboxEventEntity`, and `TraceContextEntity` do not extend the shared
entity hierarchy. `BaseEntity` requires a generated `Long` identifier and an
`Integer` version, while these mappings deliberately use assigned `String` identifiers
and `long` versions. `AuditableEntity` also maps `created`, `updated`,
`created_by`, and `updated_by`, which is incompatible with the existing
`created_at`/`updated_at` schema and the absence of actor columns.

The public event and operations models are immutable Java records. The shared model
hierarchy is mutable, Lombok `@SuperBuilder`-based, and carries an identifier/auditing
shape that does not match these value objects. Extending it would change their
serialization and builder contracts.

## Package moves

JPA entities now live under `persistence.entity`, JPA auto-configuration under
`persistence.config`, repository-specific query specifications under
`persistence.repository`, and the optional web adapter is separated into
`web.config`, `web.controller`, and `web.advice`, with its response DTO and query
mapping support under `application`.

The database table, column, ID-generation, audit-timestamp, optimistic-locking, and
index annotations were not changed. Consumers importing moved optional JPA or web
implementation classes must update their imports; public event-domain records and
operations contracts retain their existing packages.
