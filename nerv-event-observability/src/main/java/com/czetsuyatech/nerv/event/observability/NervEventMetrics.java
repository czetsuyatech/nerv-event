package com.czetsuyatech.nerv.event.observability;

import com.czetsuyatech.nerv.event.core.inbox.InboxRetryResult;
import com.czetsuyatech.nerv.event.core.outbox.DispatchResult;
import com.czetsuyatech.nerv.event.spring.retention.RetentionResult;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Bounded-tag metric recorder. It never accepts event identifiers, owners, payloads, or errors.
 */
public final class NervEventMetrics implements ConsumerMetrics {

  private final MeterRegistry registry;
  private final AtomicInteger consumersInProgress = new AtomicInteger();

  public NervEventMetrics(MeterRegistry registry) {
    this.registry = registry;
    Gauge.builder(
        "nerv.event.consumer.in.progress",
        consumersInProgress,
        AtomicInteger::get
    ).register(registry);
  }

  public void outboxDispatch(
      DispatchResult r,
      Duration d
  ) {
    increment(
        "nerv.event.outbox.dispatch.claimed",
        r.claimed()
    );
    incrementResult(
        "nerv.event.outbox.dispatch.events",
        "published",
        r.published()
    );
    incrementResult(
        "nerv.event.outbox.dispatch.events",
        "retried",
        r.retried()
    );
    incrementResult(
        "nerv.event.outbox.dispatch.events",
        "failed",
        r.failed()
    );
    incrementResult(
        "nerv.event.outbox.dispatch.events",
        "unresolved",
        r.unresolved()
    );
    registry.timer("nerv.event.outbox.dispatch.duration").record(d);
    incrementResult(
        "nerv.event.outbox.dispatch.cycles",
        r.failed() == 0 && r.unresolved() == 0 && r.retried() == 0 ? "success" : "partial",
        1
    );
  }

  public void outboxDispatchFailure(Duration d) {
    registry.timer("nerv.event.outbox.dispatch.duration").record(d);
    incrementResult(
        "nerv.event.outbox.dispatch.cycles",
        "error",
        1
    );
  }

  public void inboxRetry(
      InboxRetryResult r,
      Duration d
  ) {
    increment(
        "nerv.event.inbox.retry.claimed",
        r.claimed()
    );
    incrementResult(
        "nerv.event.inbox.retry.events",
        "processed",
        r.processed()
    );
    incrementResult(
        "nerv.event.inbox.retry.events",
        "retry_pending",
        r.retryPending()
    );
    incrementResult(
        "nerv.event.inbox.retry.events",
        "failed",
        r.failed()
    );
    incrementResult(
        "nerv.event.inbox.retry.events",
        "unresolved",
        r.unresolved()
    );
    registry.timer("nerv.event.inbox.retry.duration").record(d);
  }

  public void inboxRetryFailure(Duration d) {
    registry.timer("nerv.event.inbox.retry.duration").record(d);
  }

  public void retention(
      RetentionResult r,
      Duration d
  ) {
    incrementRetention(
        "outbox",
        r.outboxDeleted()
    );
    incrementRetention(
        "inbox",
        r.inboxDeleted()
    );
    incrementRetention(
        "trace",
        r.traceContextsDeleted()
    );
    registry.timer("nerv.event.retention.duration").record(d);
    incrementResult(
        "nerv.event.retention.cycles",
        "success",
        1
    );
  }

  public void retentionFailure(Duration d) {
    registry.timer("nerv.event.retention.duration").record(d);
    incrementResult(
        "nerv.event.retention.cycles",
        "error",
        1
    );
  }

  public void received() {
    registry.counter("nerv.event.consumer.received").increment();
  }

  public void outcome(ConsumerOutcome outcome) {
    registry.counter(
        "nerv.event.consumer.events",
        "result",
        outcome.metricValue()
    ).increment();
  }

  public void handlerExecutionStarted() {
    consumersInProgress.incrementAndGet();
  }

  public void handlerExecutionCompleted(
      ConsumerHandlerResult result,
      Duration duration
  ) {
    consumersInProgress.decrementAndGet();
    registry.counter(
        "nerv.event.consumer.handler.executions",
        "result",
        result.metricValue()
    ).increment();
    registry.timer(
        "nerv.event.consumer.processing.duration",
        "result",
        result.metricValue()
    ).record(duration);
  }

  public void retryScheduled() {
    registry.counter("nerv.event.consumer.retries").increment();
  }

  public void brokerPublish(
      String broker,
      String result,
      Duration duration
  ) {
    registry.counter(
        "nerv.event.broker.publish",
        "broker",
        broker,
        "result",
        result
    ).increment();
    registry.timer(
        "nerv.event.broker.publish.duration",
        "broker",
        broker
    ).record(duration);
  }

  private void increment(
      String name,
      int value
  ) {
    if (value > 0) {
      registry.counter(name).increment(value);
    }
  }

  private void incrementResult(
      String name,
      String result,
      int value
  ) {
    if (value > 0) {
      registry.counter(
          name,
          "result",
          result
      ).increment(value);
    }
  }

  private void incrementRetention(
      String type,
      int value
  ) {
    if (value > 0) {
      registry.counter(
          "nerv.event.retention.deleted",
          "type",
          type
      ).increment(value);
    }
  }
}
