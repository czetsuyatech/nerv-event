# Publishing

`EventPublisher.publish(EventPublication<T>)` accepts an `EventMessage<T>`, a logical `Destination`, and an optional `orderingKey`. The logical destination is resolved later from `nerv.event.destinations` to a broker ID and target.

Call it from the business transaction:

```java
@Transactional
public void createOrder(CreateOrder command) {
  Order order = orders.save(new Order(command.customerId()));
  EventMessage<OrderCreated> event = EventMessage.<OrderCreated>builder()
      .id(new EventId(UUID.randomUUID().toString()))
      .type("order.created")
      .timestamp(clock.instant())
      .source("orders")
      .correlationId(order.id().toString())
      .payload(new OrderCreated(order.id()))
      .build();
  eventPublisher.publish(EventPublication.<OrderCreated>builder()
      .event(event)
      .destination(new Destination("orders-kafka"))
      .orderingKey(order.id().toString())
      .build());
}
```

`EventPublisher` persists the Outbox row; it does not start a transaction itself. The application supplies `@Transactional` so the business write and Outbox save commit or roll back together. A later dispatcher claims the row and contacts the broker. This is persistence-before-broker delivery, not synchronous publication.

Rows without an ordering key retain the existing behavior. For keyed rows, NERV will not claim a later `PENDING` or `PROCESSING` row while an earlier row with the same key remains unfinished. Different keys remain independently claimable. This preserves Outbox dispatch sequence across batches, retries, and replicas, but delivery is still at-least-once and subject to broker guarantees.
