# Publishing

`EventPublisher.publish(EventPublication<T>)` accepts an `EventMessage<T>` and a logical `Destination`. `EventMessage` carries an `EventId`, type, timestamp, source, optional correlation ID, and payload. The logical destination is resolved later from `nerv.event.destinations` to a broker ID and target.

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
      .event(event).destination(new Destination("orders-kafka")).build());
}
```

`EventPublisher` persists the Outbox row; it does not start a transaction itself. The application supplies `@Transactional` so the business write and Outbox save commit or roll back together. A later dispatcher claims the row and contacts the broker. This is persistence-before-broker delivery, not synchronous publication.
