# nerv-event

`nerv-event` is a Spring Boot event-delivery library for applications that need durable, broker-neutral publication and consumption. It persists delivery intent before contacting Kafka or SQS, then uses a durable Inbox to make consumer processing recoverable and idempotency-aware.

## Why nerv-event?

Publishing directly to a broker from a business transaction creates a failure window: the database can commit while the broker send fails, or the broker can receive an event while the database rolls back. `nerv-event` writes an Outbox record in the same transaction as the business update and sends it later. On the consumer side it persists an Inbox record before acknowledging the broker message.

Key capabilities include transactional Outbox publication, durable Inbox processing, Kafka and SQS adapters, multiple SQS clients/accounts, configuration-driven broker consumers, broker-neutral `EventHandler`s, database-backed retry, `EventRetryableException`, PostgreSQL migrations, observability, operations/manual recovery, retention, and multi-pod-safe claims with Outbox fencing tokens.

## Installation

This source tree targets `2.0.0`. Applications upgrading from 1.x must apply the new Outbox fencing
migration and update any direct `OutboxService` integrations; see [Upgrading to 2.0](docs/upgrading-to-2.0.md).

For normal Spring Boot applications, add the public starter:

```xml
<dependency>
  <groupId>com.czetsuyatech.nerv</groupId>
  <artifactId>nerv-event-spring-boot-starter</artifactId>
  <version>${nerv-event.version}</version>
</dependency>
```

The starter brings Spring integration, JPA persistence, and the Kafka and SQS adapters. Kafka and SQS are inactive until `nerv.event.kafka.enabled=true` and/or `nerv.event.sqs.enabled=true`. The current starter does not provide an Outbox `RetryPolicy`; applications must register one before the Outbox dispatcher is created. `nerv-event-operations` and `nerv-event-operations-web` are intentional optional dependencies; the web module is not part of the starter. Observability contracts are currently transitively present through the JPA implementation; Micrometer/tracing integrations still back off when their prerequisites are absent.

## Quick Start

Configure PostgreSQL, apply the packaged migrations, select a broker, and map a logical destination:

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/orders
    username: orders
    password: ${ORDERS_DB_PASSWORD}
  jpa.hibernate.ddl-auto: validate

nerv:
  event:
    destinations:
      orders:
        broker: kafka
        target: order-events
    kafka:
      enabled: true
```

Inside a business transaction, build an `EventMessage` and publish an `EventPublication` to the logical destination. Implement `EventHandler<T>` for inbound event types. You do **not** need NERV component scanning, `@EnableJpaRepositories`, `@EntityScan`, `@KafkaListener`, or `@SqsListener`.

```java
@Transactional
void createOrder(Order order) {
  orders.save(order);
  eventPublisher.publish(EventPublication.<OrderCreated>builder()
      .event(EventMessage.<OrderCreated>builder()
          .id(new EventId(UUID.randomUUID().toString()))
          .type("order.created")
          .timestamp(clock.instant())
          .source("orders")
          .payload(new OrderCreated(order.id()))
          .build())
      .destination(new Destination("orders"))
      .build());
}
```

See [Getting Started](docs/getting-started.md), [Publishing](docs/publishing.md), and [Consuming](docs/consuming.md) for the complete path.

## Architecture

```text
Application -> EventPublisher -> Outbox -> OutboxDispatcher -> Kafka / SQS
Kafka / SQS -> consumer adapter -> Inbox -> ConsumerDispatcher -> EventHandlerInterceptor chain -> EventHandler<T>
```

The detailed module and lifecycle view is in [Architecture](docs/architecture.md).

## Documentation

- [Getting Started](docs/getting-started.md)
- [Configuration reference](docs/configuration.md)
- [Inbox and Outbox](docs/inbox-outbox.md)
- [Kafka](docs/kafka.md) and [SQS](docs/sqs.md)
- [Database](docs/database.md), [Operations](docs/operations.md), and [Observability](docs/observability.md)
- [Production guide and limitations](docs/production-guide.md)
- [Upgrading from 1.x to 2.0](docs/upgrading-to-2.0.md)

The runnable adoption reference is [`nerv-examples/nerv-event-spring-boot-demo`](../nerv-examples/nerv-event-spring-boot-demo).

## Requirements and compatibility

- Java 21 or newer
- Spring Boot 4.1.0 dependency management (the current build baseline)
- PostgreSQL is the tested production database; H2 is only a test/development convenience
- Kafka and standard SQS queues are supported adapters

`nerv-event` provides at-least-once delivery, not distributed exactly-once processing. See [Production guide](docs/production-guide.md) before production adoption.

## License and repository conventions

Use the project license and repository conventions in this repository. Maven coordinates use `com.czetsuyatech.nerv`; Java packages remain `com.czetsuyatech.nerv.event`.
