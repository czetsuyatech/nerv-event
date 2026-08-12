package com.czetsuyatech.nerv.event.observability;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Registers low-cardinality backlog gauges. Empty queues report zero seconds.
 */
public final class NervEventBacklogMetrics {

  public NervEventBacklogMetrics(
      MeterRegistry registry,
      Clock clock,
      Optional<OutboxOperationalMetrics> outbox,
      Optional<InboxOperationalMetrics> inbox
  )
  {
    outbox.ifPresent(source -> {
      for (OutboxStatus status : new OutboxStatus[]{OutboxStatus.PENDING, OutboxStatus.PROCESSING,
          OutboxStatus.FAILED}) {
        Gauge.builder(
            "nerv.event.outbox.events",
            source,
            s -> safeCount(() -> s.count(status))
        )
            .tag(
                "status",
                status.name()
            )
            .register(registry);
      }
      Gauge.builder(
          "nerv.event.outbox.oldest.pending.age",
          source,
          s -> age(
              clock,
              safeInstant(s::oldestPendingAt)
          )
      ).baseUnit("seconds").register(registry);
    });
    inbox.ifPresent(source -> {
      for (InboxStatus status : new InboxStatus[]{InboxStatus.RECEIVED, InboxStatus.PROCESSING,
          InboxStatus.RETRY_PENDING, InboxStatus.FAILED}) {
        Gauge.builder(
            "nerv.event.inbox.events",
            source,
            s -> safeCount(() -> s.count(status))
        )
            .tag(
                "status",
                status.name()
            )
            .register(registry);
      }
      Gauge.builder(
          "nerv.event.inbox.oldest.retry_pending.age",
          source,
          s -> age(
              clock,
              safeInstant(s::oldestRetryPendingAt)
          )
      ).baseUnit("seconds").register(registry);
    });
  }

  private static double safeCount(LongSupplier s) {
    try {
      return s.get();
    } catch (RuntimeException e) {
      return Double.NaN;
    }
  }

  private static Optional<Instant> safeInstant(InstantSupplier s) {
    try {
      return s.get();
    } catch (RuntimeException e) {
      return Optional.empty();
    }
  }

  private static double age(
      Clock clock,
      Optional<Instant> instant
  ) {
    return instant.map(
        i -> (double) Math.max(
            0,
            Duration.between(
                i,
                clock.instant()
            ).toSeconds()
        )
    ).orElse(0d);
  }

  private interface LongSupplier {

    long get();
  }

  private interface InstantSupplier {

    Optional<Instant> get();
  }
}
