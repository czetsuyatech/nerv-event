package com.czetsuyatech.nerv.event.observability;

import com.czetsuyatech.nerv.event.spring.retention.EventRetentionScheduler;
import com.czetsuyatech.nerv.event.spring.retention.RetentionResult;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * Reports retention progress without turning a transient cleanup problem into application DOWN.
 */
public final class NervEventRetentionHealthIndicator implements HealthIndicator {

  private final EventRetentionScheduler scheduler;

  public NervEventRetentionHealthIndicator(EventRetentionScheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public Health health() {
    Health.Builder builder = Health.up()
        .withDetail(
            "enabled",
            scheduler.isRunning()
        )
        .withDetail(
            "lastRun",
            scheduler.getLastCycleCompletedAt()
        )
        .withDetail(
            "lastSuccessfulRun",
            scheduler.getLastSuccessfulCycleAt()
        );
    RetentionResult result = scheduler.getLastResult();
    if (result != null) {
      Map<String, Integer> deleted = new LinkedHashMap<>();
      deleted.put(
          "outbox",
          result.outboxDeleted()
      );
      deleted.put(
          "inbox",
          result.inboxDeleted()
      );
      deleted.put(
          "trace",
          result.traceContextsDeleted()
      );
      builder.withDetail(
          "lastDeleted",
          deleted
      );
    }
    Throwable failure = scheduler.getLastFailure();
    if (failure != null) {
      builder.withDetail(
          "lastFailure",
          failure.getClass().getSimpleName()
      );
    }
    return builder.build();
  }
}
