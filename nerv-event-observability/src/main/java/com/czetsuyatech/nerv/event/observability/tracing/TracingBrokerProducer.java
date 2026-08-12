package com.czetsuyatech.nerv.event.observability.tracing;

import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.util.Map;

/**
 * Restores EventId-associated context and creates one short broker delivery span per attempt.
 */
public final class TracingBrokerProducer implements BrokerProducer {

  private final BrokerProducer delegate;
  private final TraceContextStore store;
  private final Tracer tracer;
  private final Propagator propagator;

  public TracingBrokerProducer(
      BrokerProducer delegate,
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
  public BrokerId brokerId() {
    return delegate.brokerId();
  }

  @Override
  public BrokerPublishResult publish(BrokerMessage message) throws Exception {
    Map<String, String> stored = store.find(message.eventId()).orElse(Map.of());
    Span span = stored.isEmpty()
        ? tracer.nextSpan()
        : propagator.extract(
            stored,
            Map::get
        ).name("nerv.event.broker.publish").start();
    if (stored.isEmpty()) {
      span = span.name("nerv.event.broker.publish").start();
    }
    span.tag(
        "nerv.event.id",
        message.eventId().value()
    )
        .tag(
            "nerv.event.type",
            message.eventType()
        )
        .tag(
            "nerv.event.broker",
            brokerId().value()
        )
        .tag(
            "nerv.event.destination",
            message.target()
        );
    Map<String, String> outgoing = new java.util.LinkedHashMap<>();
    propagator.inject(
        span.context(),
        outgoing,
        Map::put
    );
    try (Tracer.SpanInScope ignored = tracer.withSpan(span);
        TraceContextCarrier.Scope carrier = TraceContextCarrier.open(outgoing)) {
      return delegate.publish(message);
    } catch (Exception exception) {
      span.error(exception);
      throw exception;
    } finally {
      span.end();
    }
  }
}
