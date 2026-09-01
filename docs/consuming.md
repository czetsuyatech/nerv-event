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

## Handler interception

Register an `EventHandlerInterceptor` bean to set up and clean up application context around the resolved handler. An
interceptor runs only after NERV has durably registered and claimed the Inbox event; durable duplicates never invoke
it. Call `chain.proceed()` exactly once to continue to the next interceptor or handler.

```java
@Component
@Order(10)
final class TenantContextInterceptor implements EventHandlerInterceptor {
  public void intercept(EventMessage<?> event, EventHandlerChain chain) {
    TenantContext previous = tenantContext.get();
    try {
      tenantContext.set(event.source());
      chain.proceed();
    } finally {
      if (previous == null) {
        tenantContext.clear();
      } else {
        tenantContext.set(previous);
      }
    }
  }
}
```

Spring orders interceptor beans using its normal `Ordered` / `@Order` rules. Lower values are outermost: an
interceptor with order `10` runs before one with order `20`, and its cleanup runs last. An interceptor exception uses
the normal handler failure path and its existing retry classification. An interceptor that does not call
`chain.proceed()` fails processing explicitly; it never silently marks the Inbox event as processed. The chain is
synchronous: invoke `chain.proceed()` in the current call rather than scheduling it on another thread.
