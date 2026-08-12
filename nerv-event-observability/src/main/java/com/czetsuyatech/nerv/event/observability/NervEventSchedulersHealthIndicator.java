package com.czetsuyatech.nerv.event.observability;

import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerState;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerStatus;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerStatusProvider;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

public final class NervEventSchedulersHealthIndicator implements HealthIndicator {

  private final List<SchedulerStatusProvider> schedulers;
  private final Clock clock;
  private final Duration executionGrace;

  public NervEventSchedulersHealthIndicator(
      List<SchedulerStatusProvider> schedulers,
      Clock clock,
      Duration executionGrace
  )
  {
    this.schedulers = List.copyOf(schedulers);
    this.clock = clock;
    this.executionGrace = executionGrace;
  }

  @Override
  public Health health() {
    boolean down = false;
    Map<String, Object> details = new LinkedHashMap<>();
    for (SchedulerStatusProvider provider : schedulers) {
      SchedulerStatus status = provider.status();
      Instant now = clock.instant();
      boolean stale = stale(status, now);
      boolean unhealthy = unhealthy(status, stale);
      down |= unhealthy;
      Map<String, Object> scheduler = new LinkedHashMap<>();
      scheduler.put("state", status.state());
      scheduler.put("scheduled", status.scheduled());
      scheduler.put("workInProgress", status.workInProgress());
      put(scheduler, "lastSuccessfulCompletionAt", status.lastSuccessfulCompletionAt());
      put(scheduler, "lastCycleFailureAt", status.lastCycleFailureAt());
      put(scheduler, "lastSchedulingFailureAt", status.lastSchedulingFailureAt());
      put(scheduler, "nextExecutionAt", status.nextExecutionAt());
      scheduler.put("stale", stale);
      details.put(provider.schedulerType().tag(), scheduler);
    }
    Health.Builder health = down ? Health.down() : Health.up();
    details.forEach(health::withDetail);
    return health.build();
  }

  private boolean unhealthy(
      SchedulerStatus status,
      boolean stale
  ) {
    if (status.state() == SchedulerState.FAILED) {
      return true;
    }
    if (status.state() != SchedulerState.RUNNING) {
      return false;
    }
    if (!status.scheduled() && !status.workInProgress()) {
      return true;
    }
    return stale;
  }

  private boolean stale(
      SchedulerStatus status,
      Instant now
  ) {
    if (status.state() != SchedulerState.RUNNING) {
      return false;
    }
    if (status.workInProgress()) {
      return status.lastStartedAt() != null && now.isAfter(status.lastStartedAt().plus(executionGrace));
    }
    Instant deadline = status.nextExecutionAt();
    if (deadline == null) {
      Instant progress = status.lastSuccessfulCompletionAt() != null
          ? status.lastSuccessfulCompletionAt()
          : status.startedAt();
      deadline = progress == null ? null : progress.plus(status.maximumInterval());
    }
    return deadline != null && now.isAfter(deadline.plus(executionGrace));
  }

  private static void put(
      Map<String, Object> details,
      String name,
      Object value
  ) {
    if (value != null) {
      details.put(name, value);
    }
  }
}
