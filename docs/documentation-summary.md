# Documentation summary

This release-documentation baseline covers transactional Outbox, durable Inbox, Kafka, standard SQS with multiple client IDs, automatic and manual retry, PostgreSQL migrations, retention, observability, and the optional administrative web surface.

Compatibility: Java 21+, Spring Boot 4.1.0 dependency management, PostgreSQL as the tested production database. Known limitations are at-least-once delivery, no replay/event store, single default JPA auto-configuration, no FIFO SQS consumer support, and an application-required Outbox `RetryPolicy`. See [Production guide](production-guide.md).
