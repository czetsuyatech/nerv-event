# Retry and Failure

Throw `EventRetryableException` only when the same event may succeed later without changing its payload or business meaning:

```java
try {
  gateway.charge(event.payload());
} catch (TemporaryGatewayException ex) {
  throw new EventRetryableException("Payment gateway unavailable", ex);
}
```

The default classifier treats a failure as retryable when `EventRetryableException` occurs anywhere in its cause chain. Other exceptions, including invalid data, absent handler, permanent validation, and deserialization/configuration failures, become `FAILED` immediately.

`nerv.event.inbox.retry.max-attempts` counts actual `EventHandler` executions, including the initial execution—not broker deliveries or claim attempts. Defaults are enabled, 5 attempts, 1 second initial delay, multiplier 2.0, and a 30 second maximum delay. A retryable failure becomes `RETRY_PENDING` while its budget remains, otherwise `FAILED`. Scheduling is database-backed; no Kafka retry topic or SQS business-retry queue is used.

Automatic retry and manual retry differ. Automatic retry follows the policy. Manual recovery can transition only `FAILED` Outbox to `PENDING` and `FAILED` Inbox to `RETRY_PENDING`, retaining the same row and identifiers. It is not replay: NERV does not clone records, generate a new `EventId`, or reprocess successful history. For a new intentional delivery, publish a new event. See [Operations](operations.md).
