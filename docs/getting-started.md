# Getting Started

This guide follows the runnable [`nerv-event-spring-boot-demo`](../../nerv-examples/nerv-event-spring-boot-demo). It uses only the starter and public application APIs.

1. Add `com.czetsuyatech.nerv:nerv-event-spring-boot-starter` as shown in the [README](../README.md#installation).
2. Configure a PostgreSQL `DataSource` and use `spring.jpa.hibernate.ddl-auto=validate` in deployed environments.
3. Apply `001-create-outbox.sql` through `004-create-indexes.sql` from `nerv-event-persistence-jpa/src/main/resources/META-INF/nerv-event/db/postgresql/migration/` through your application-owned migration process.
4. Enable one broker and map a logical destination:

```yaml
nerv:
  event:
    destinations:
      orders-kafka: { broker: kafka, target: order-events }
    kafka:
      enabled: true
      consumers:
        order-events:
          enabled: true
          topic: order-events
          group-id: orders
```

5. Register an Outbox `RetryPolicy`. This is currently an application-supplied extension point; without it, the starter does not create an Outbox dispatcher:

```java
@Bean
RetryPolicy outboxRetryPolicy() {
  return new RetryPolicy() {
    public boolean allowsRetry(int failedAttemptCount) { return failedAttemptCount < 5; }
    public Instant nextEligibleAt(int failedAttemptCount, Instant failedAt) {
      return failedAt.plusSeconds(5);
    }
  };
}
```

6. Inject `EventPublisher` and call it inside the same `@Transactional` business method that changes your data. The call stores a `PENDING` Outbox row; it does not synchronously send to Kafka/SQS.
7. Register a Spring bean implementing `EventHandler<T>` for each inbound event type.
8. Run the application. The Outbox dispatcher publishes durable rows; configured consumer adapters register and process Inbox rows.
8. Inspect `nerv_outbox_event` and `nerv_inbox_event`, or add the optional operations modules for administrative inspection.

The demo also demonstrates Kafka publication/consumption, two SQS client IDs, SQS destinations, Inbox retry, retention settings, and the optional operations web surface. It deliberately contains no manual NERV scan annotations or broker listener annotations. Its current configuration does not supply an Outbox `RetryPolicy`; treat it as a configuration and API reference, not a complete successful Outbox-dispatch deployment until that application extension is added.
