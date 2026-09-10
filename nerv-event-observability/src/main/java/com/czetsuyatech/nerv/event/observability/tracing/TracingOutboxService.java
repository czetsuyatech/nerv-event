package com.czetsuyatech.nerv.event.observability.tracing;

import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.core.outbox.OutboxEvent;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxService;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Explicit transactional outbox decorator. Its save call participates in the caller transaction.
 */
public final class TracingOutboxService implements OutboxService {

  private final OutboxService delegate;
  private final TraceContextStore store;
  private final Tracer tracer;
  private final Propagator propagator;

  public TracingOutboxService(
      OutboxService delegate,
      TraceContextStore store,
      Tracer tracer,
      Propagator propagator
  )
  {
    this.delegate = delegate;
    this.store = store;
    this.tracer = tracer;
    this.propagator = propagator;
  }

  @Override
  public OutboxEvent save(OutboxEvent event) {
    OutboxEvent saved = delegate.save(event);
    Span span = tracer.currentSpan();
    if (span != null && !span.isNoop()) {
      Map<String, String> fields = new LinkedHashMap<>();
      propagator.inject(
          span.context(),
          fields,
          Map::put
      );
      if (!fields.isEmpty()) {
        store.save(
            event.event().id(),
            fields
        );
      }
    }
    return saved;
  }

  @Override
  public List<OutboxEvent> claimPending(
      Instant at,
      int size
  ) {
    return delegate.claimPending(
        at,
        size
    );
  }

  @Override
  public boolean markPublished(
      OutboxId id,
      long claimVersion,
      BrokerPublishResult result
  ) {
    return delegate.markPublished(
        id,
        claimVersion,
        result
    );
  }

  @Override
  public boolean reschedule(
      OutboxId id,
      long claimVersion,
      int attempt,
      Instant next,
      String reason
  ) {
    return delegate.reschedule(
        id,
        claimVersion,
        attempt,
        next,
        reason
    );
  }

  @Override
  public boolean markFailed(
      OutboxId id,
      long claimVersion,
      int attempt,
      String reason
  ) {
    return delegate.markFailed(
        id,
        claimVersion,
        attempt,
        reason
    );
  }
}
