package com.czetsuyatech.nerv.event.observability;

import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.observability.autoconfigure.NervEventObservabilityProperties;
import com.czetsuyatech.nerv.event.spring.dispatcher.OutboxDispatchScheduler;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerState;
import java.time.Clock;
import java.time.Duration;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

public final class NervEventOutboxHealthIndicator implements HealthIndicator {

  private final OutboxOperationalMetrics metrics;
  private final OutboxDispatchScheduler scheduler;
  private final Clock clock;
  private final NervEventObservabilityProperties.Health.Outbox thresholds;

  public NervEventOutboxHealthIndicator(
      OutboxOperationalMetrics metrics,
      OutboxDispatchScheduler scheduler,
      Clock clock,
      NervEventObservabilityProperties.Health.Outbox thresholds
  )
  {
    this.metrics = metrics;
    this.scheduler = scheduler;
    this.clock = clock;
    this.thresholds = thresholds;
  }

  @Override
  public Health health() {
    try {
      long pending = metrics.count(OutboxStatus.PENDING), processing = metrics.count(OutboxStatus.PROCESSING),
          failed = metrics.count(OutboxStatus.FAILED);
      long age = metrics.oldestPendingAt()
          .map(
              i -> Math.max(
                  0,
                  Duration.between(
                      i,
                      clock.instant()
                  ).toSeconds()
              )
          )
          .orElse(0L);
      boolean exceeded = (thresholds.getMaxPending() != null && pending > thresholds.getMaxPending())
          || (thresholds.getMaxOldestPendingAge() != null && age > thresholds.getMaxOldestPendingAge().toSeconds());
      boolean schedulerFailed = scheduler != null && scheduler.status().state() == SchedulerState.FAILED;
      Health.Builder b = schedulerFailed || exceeded
          ? Health.down()
          : Health.up();
      return b.withDetail(
          "pending",
          pending
      )
          .withDetail(
              "processing",
              processing
          )
          .withDetail(
              "failed",
              failed
          )
          .withDetail(
              "oldestPendingAge",
              age
          )
          .withDetail(
              "dispatcherRunning",
              scheduler == null || scheduler.isRunning()
          )
          .withDetail(
              "thresholdExceeded",
              exceeded
          )
          .build();
    } catch (RuntimeException ex) {
      return Health.down(ex).build();
    }
  }
}
