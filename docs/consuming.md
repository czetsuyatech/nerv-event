# Consuming

Implement the broker-neutral public contract as a Spring bean:

```java
@Component
final class PaymentRequestedHandler implements EventHandler<PaymentRequested> {
  public String eventType() { return "payment.requested"; }
  public Class<PaymentRequested> payloadType() { return PaymentRequested.class; }
  public void handle(EventMessage<PaymentRequested> event) {
    paymentService.request(event.payload());
  }
}
```

Configure Kafka or SQS consumers under `nerv.event`; NERV creates the adapter containers. Do **not** add `@KafkaListener` or `@SqsListener` for this flow. The adapter deserializes the broker message, registers/claims the durable Inbox row, invokes the matching handler, persists the terminal or retry state, then acknowledges/deletes the broker message only after a durable result.

Handlers should use their own `@Transactional` boundary when their business work requires one. No NERV database transaction is deliberately held across broker or other network I/O.
