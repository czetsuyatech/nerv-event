package com.czetsuyatech.nerv.event.observability;

import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerState;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerStatus;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerStatusProvider;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.ToDoubleFunction;

/** Registers scheduler metrics using only the bounded scheduler and result tags. */
public final class NervEventSchedulerMetrics {

  public NervEventSchedulerMetrics(
      MeterRegistry registry,
      Clock clock,
      List<SchedulerStatusProvider> schedulers
  )
  {
    for (SchedulerStatusProvider scheduler : schedulers) {
      String tag = scheduler.schedulerType().tag();
      gauge(registry, "nerv.event.scheduler.state", scheduler, tag, s -> state(s.state()));
      gauge(registry, "nerv.event.scheduler.work.in.progress", scheduler, tag, s -> s.workInProgress() ? 1 : 0);
      gauge(
          registry,
          "nerv.event.scheduler.last.success.age",
          scheduler,
          tag,
          s -> age(clock, s.lastSuccessfulCompletionAt())
      );
      gauge(
          registry,
          "nerv.event.scheduler.last.failure.age",
          scheduler,
          tag,
          s -> age(clock, latest(s.lastCycleFailureAt(), s.lastSchedulingFailureAt()))
      );
      gauge(
          registry,
          "nerv.event.scheduler.next.execution.delay",
          scheduler,
          tag,
          s -> delay(clock, s.nextExecutionAt())
      );
      FunctionCounter.builder(
          "nerv.event.scheduler.cycles",
          scheduler,
          p -> p.status().successfulCycleCount()
      ).tag("scheduler", tag).tag("result", "success").register(registry);
      FunctionCounter.builder(
          "nerv.event.scheduler.cycles",
          scheduler,
          p -> p.status().failedCycleCount()
      ).tag("scheduler", tag).tag("result", "failure").register(registry);
      FunctionCounter.builder(
          "nerv.event.scheduler.scheduling.failures",
          scheduler,
          p -> p.status().schedulingFailureCount()
      ).tag("scheduler", tag).register(registry);
    }
  }

  private static void gauge(
      MeterRegistry registry,
      String name,
      SchedulerStatusProvider provider,
      String tag,
      ToDoubleFunction<SchedulerStatus> value
  ) {
    Gauge.builder(name, provider, p -> value.applyAsDouble(p.status())).tag("scheduler", tag).register(registry);
  }

  private static double state(SchedulerState state) {
    return switch (state) {
      case STOPPED -> 0;
      case STARTING -> 1;
      case RUNNING -> 2;
      case FAILED -> 3;
    };
  }

  private static double age(
      Clock clock,
      Instant instant
  ) {
    return instant == null ? 0 : Math.max(0, Duration.between(instant, clock.instant()).toSeconds());
  }

  private static double delay(
      Clock clock,
      Instant instant
  ) {
    return instant == null ? 0 : Math.max(0, Duration.between(clock.instant(), instant).toSeconds());
  }

  private static Instant latest(
      Instant first,
      Instant second
  ) {
    if (first == null) {
      return second;
    }
    return second == null || first.isAfter(second) ? first : second;
  }
}
