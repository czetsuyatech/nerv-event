# Operations and manual recovery

Add `nerv-event-operations` for the programmatic `OutboxOperationService` and `InboxOperationService` APIs. Add `nerv-event-operations-web` plus Spring MVC for the optional HTTP surface. The web module is disabled until `nerv.event.operations.web.enabled=true` and is not an Actuator endpoint.

With the default base path `/management/nerv-event`, available endpoints are:

- `GET /outbox`, `GET /inbox` — bounded paginated search, payload-free.
- `GET /outbox/{outboxId}`, `GET /inbox/{eventId}` — detail; stored payload only if `payload.enabled=true`.
- `POST /outbox/{outboxId}/retry` — `FAILED -> PENDING`.
- `POST /inbox/{eventId}/retry` — `FAILED -> RETRY_PENDING`.

Lists default to page 0 and size 20; sizes above 100 are rejected. Search is stable (`updatedAt DESC` plus durable-ID tie-breaker). Missing rows return 404, malformed/oversized requests 400, and an invalid or stale manual retry 409. A successful retry response is `202 Accepted`: a normal worker performs publication/handling later.

These endpoints are administrative and have no installed authentication or authorization. Do not expose them publicly. The application owns Spring Security, for example:

```java
requestMatchers("/management/nerv-event/**").hasRole("NERV_EVENT_ADMIN")
```

No replay, bulk retry, delete, direct publish, or direct handler endpoint exists. Manual retry preserves the durable row, payload, ID, metadata, attempt count, and error history.
