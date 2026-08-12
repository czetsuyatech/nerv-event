package com.czetsuyatech.nerv.event.observability;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.observability.autoconfigure.NervEventObservabilityProperties;
import com.czetsuyatech.nerv.event.spring.dispatcher.InboxRetryScheduler;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerState;
import java.time.Clock;
import java.time.Duration;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

public final class NervEventInboxHealthIndicator implements HealthIndicator {

  private final InboxOperationalMetrics metrics;
  private final InboxRetryScheduler scheduler;
  private final Clock clock;
  private final NervEventObservabilityProperties.Health.Inbox thresholds;

  public NervEventInboxHealthIndicator(
      InboxOperationalMetrics metrics,
      InboxRetryScheduler scheduler,
      Clock clock,
      NervEventObservabilityProperties.Health.Inbox thresholds
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
      long received = metrics.count(InboxStatus.RECEIVED), processing = metrics.count(InboxStatus.PROCESSING),
          retry = metrics.count(InboxStatus.RETRY_PENDING), failed = metrics.count(InboxStatus.FAILED);
      long age = metrics.oldestRetryPendingAt()
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
      boolean exceeded = (thresholds.getMaxRetryPending() != null && retry > thresholds.getMaxRetryPending())
          || (thresholds.getMaxOldestRetryAge() != null && age > thresholds.getMaxOldestRetryAge().toSeconds());
      boolean schedulerFailed = scheduler != null && scheduler.status().state() == SchedulerState.FAILED;
      Health.Builder b = schedulerFailed || exceeded
          ? Health.down()
          : Health.up();
      return b.withDetail(
          "received",
          received
      )
          .withDetail(
              "processing",
              processing
          )
          .withDetail(
              "retryPending",
              retry
          )
          .withDetail(
              "failed",
              failed
          )
          .withDetail(
              "oldestRetryPendingAge",
              age
          )
          .withDetail(
              "retrySchedulerRunning",
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
