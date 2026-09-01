# Architecture

```text
Application
  -> EventPublisher -> OutboxRepository -> OutboxDispatcher
  -> DestinationResolver -> BrokerProducerRegistry -> Kafka / SQS
Kafka / SQS -> consumer adapter -> InboxRepository -> ConsumerDispatcher -> EventHandlerInterceptor chain -> EventHandler<T>
                                      ^
                              InboxRetryDispatcher

Operations -> inspect/manual retry       Observability -> metrics/health/tracing
Retention  -> PUBLISHED/PROCESSED cleanup
```

`nerv-event-api` contains public event, publisher, handler, interceptor, and exception contracts. `nerv-event-core` contains broker-neutral lifecycle, routing, retry, and scheduling behavior. `nerv-event-spring` binds configuration and Spring beans. `nerv-event-persistence-jpa` supplies JPA persistence and packaged PostgreSQL migrations. Kafka and SQS concepts stay in `nerv-event-kafka` and `nerv-event-sqs`. `nerv-event-observability` contains Micrometer, health, and tracing integration; `nerv-event-operations` is the programmatic operations layer; `nerv-event-operations-web` is the optional MVC surface. The starter assembles the standard publication/consumption path.

This separation keeps public application handlers independent of broker technology and lets future brokers implement the same core boundaries without leaking broker types into application contracts.
