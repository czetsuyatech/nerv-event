package com.czetsuyatech.nerv.event.spring.retention;

import com.czetsuyatech.nerv.event.core.retention.EventRetention;
import com.czetsuyatech.nerv.event.core.retention.RetentionSidecarCleaner;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import com.czetsuyatech.nerv.event.spring.dispatcher.DispatcherPollingPolicy;
import com.czetsuyatech.nerv.event.spring.scheduler.ResilientSchedulerLifecycle;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerStatus;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerStatusProvider;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerType;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.TaskScheduler;

/**
 * <p>
 * Runs bounded successful-row retention with adaptive self-rescheduling.
 * </p>
 *
 * <p>
 * The scheduler deliberately owns no transaction. Each retention adapter call has an independent, short persistence
 * transaction; scheduling occurs only after the previous cycle completes, preventing local overlap.
 * </p>
 */
@Slf4j
public final class EventRetentionScheduler implements SmartLifecycle, SchedulerStatusProvider {

  private final EventRetention eventRetention;
  private final Optional<RetentionSidecarCleaner> sidecarCleaner;
  private final NervEventProperties.Retention properties;
  private final Clock clock;
  private final DispatcherPollingPolicy pollingPolicy;
  private final List<RetentionCycleListener> cycleListeners;
  private final ResilientSchedulerLifecycle lifecycle;

  @Getter
  private volatile Duration baseDelay;

  @Getter
  private volatile Duration actualScheduledDelay;

  @Getter
  private volatile Instant lastCycleStartedAt;

  @Getter
  private volatile Instant lastCycleCompletedAt;

  @Getter
  private volatile Instant lastSuccessfulCycleAt;

  @Getter
  private volatile RetentionResult lastResult;

  @Getter
  private volatile Throwable lastFailure;

  private EventRetentionScheduler(
      EventRetention eventRetention,
      Optional<RetentionSidecarCleaner> sidecarCleaner,
      NervEventProperties.Retention properties,
      Clock clock,
      TaskScheduler taskScheduler,
      DispatcherPollingPolicy pollingPolicy,
      List<RetentionCycleListener> cycleListeners
  )
  {
    this.eventRetention = Objects.requireNonNull(
        eventRetention,
        "repository must not be null"
    );
    this.sidecarCleaner = Objects.requireNonNull(
        sidecarCleaner,
        "sidecarCleaner must not be null"
    );
    this.properties = Objects.requireNonNull(
        properties,
        "properties must not be null"
    );
    this.clock = Objects.requireNonNull(
        clock,
        "clock must not be null"
    );
    Objects.requireNonNull(taskScheduler, "taskScheduler must not be null");
    this.pollingPolicy = Objects.requireNonNull(
        pollingPolicy,
        "pollingPolicy must not be null"
    );
    this.cycleListeners = List.copyOf(cycleListeners);
    this.lifecycle = new ResilientSchedulerLifecycle(
        SchedulerType.EVENT_RETENTION,
        clock,
        taskScheduler,
        properties.getPolling().getMaxInterval()
    );
  }

  public static EventRetentionScheduler create(
      EventRetention repository,
      Optional<RetentionSidecarCleaner> sidecarCleaner,
      NervEventProperties.Retention properties,
      Clock clock,
      TaskScheduler taskScheduler,
      DispatcherPollingPolicy pollingPolicy,
      List<RetentionCycleListener> cycleListeners
  ) {
    Objects.requireNonNull(
        properties,
        "retention properties must not be null"
    ).validate();
    return new EventRetentionScheduler(
        repository,
        sidecarCleaner,
        properties,
        clock,
        taskScheduler,
        pollingPolicy,
        cycleListeners
    );
  }

  @Override
  public void start() {
    boolean started = lifecycle.start(
        () -> {
          baseDelay = pollingPolicy.initialBaseDelay();
          return nextDelay();
        },
        this::retain
    );
    if (started) {
      log.info(
          "Event retention activated batchSize={} outboxEnabled={} outboxAge={} inboxEnabled={} inboxAge={} minInterval={} maxInterval={}",
          properties.getBatchSize(),
          properties.getOutbox().isEnabled(),
          properties.getOutbox().getAge(),
          properties.getInbox().isEnabled(),
          properties.getInbox().getAge(),
          properties.getPolling().getMinInterval(),
          properties.getPolling().getMaxInterval()
      );
    }
  }

  @Override
  public void stop() {
    lifecycle.stop();
    log.info("Event retention stopped");
  }

  @Override
  public boolean isRunning() {
    return lifecycle.isRunning();
  }

  /**
   * Executes one cycle; exposed for deterministic integration tests and operational invocation.
   */
  public void retain() {
    if (!lifecycle.beginWork()) {
      log.debug("Skipping retention during shutdown");
      return;
    }
    lastCycleStartedAt = clock.instant();
    long startedAtNanos = System.nanoTime();
    try {
      RetentionResult result = deleteEligibleRows(lastCycleStartedAt);
      Duration duration = Duration.ofNanos(System.nanoTime() - startedAtNanos);
      lastResult = result;
      lastFailure = null;
      lastSuccessfulCycleAt = clock.instant();
      lifecycle.cycleSucceeded();
      baseDelay = pollingPolicy.nextBaseDelay(
          baseDelay,
          result.totalDeleted() > 0
      );
      if (result.totalDeleted() > 0) {
        log.info(
            "Event retention completed outboxDeleted={} inboxDeleted={} traceDeleted={} durationMs={}",
            result.outboxDeleted(),
            result.inboxDeleted(),
            result.traceContextsDeleted(),
            duration.toMillis()
        );
      } else {
        log.debug(
            "Event retention found no eligible rows nextDelay={}",
            baseDelay
        );
      }
      notifySuccess(result, duration);
    } catch (Exception exception) {
      lifecycle.cycleFailed(exception);
      Duration duration = Duration.ofNanos(System.nanoTime() - startedAtNanos);
      lastFailure = exception;
      baseDelay = pollingPolicy.nextBaseDelay(
          baseDelay,
          false
      );
      log.error(
          "Event retention cleanup persistence failure",
          exception
      );
      notifyFailure(duration, exception);
    } finally {
      lastCycleCompletedAt = clock.instant();
      lifecycle.finishWork(this::nextDelay, this::retain);
    }
  }

  private RetentionResult deleteEligibleRows(Instant now) {
    int limit = properties.getBatchSize();
    int outboxDeleted = properties.getOutbox().isEnabled()
        ? eventRetention.deletePublishedBefore(
            now.minus(properties.getOutbox().getAge()),
            limit
        )
        : 0;
    int inboxDeleted = properties.getInbox().isEnabled()
        ? eventRetention.deleteProcessedBefore(
            now.minus(properties.getInbox().getAge()),
            limit
        )
        : 0;
    int traceDeleted = sidecarCleaner.map(cleaner -> cleaner.deleteUnreferenced(limit)).orElse(0);
    return new RetentionResult(
        outboxDeleted,
        inboxDeleted,
        traceDeleted
    );
  }

  private Duration nextDelay() {
    Duration scheduledDelay = pollingPolicy.jitteredDelay(baseDelay);
    actualScheduledDelay = scheduledDelay;
    log.debug(
        "Scheduling next event retention baseDelay={} scheduledDelay={}",
        baseDelay,
        scheduledDelay
    );
    return scheduledDelay;
  }

  private void notifySuccess(
      RetentionResult result,
      Duration duration
  ) {
    for (RetentionCycleListener listener : cycleListeners) {
      try {
        listener.onSuccess(result, duration);
      } catch (RuntimeException exception) {
        log.warn("Event retention success listener failed", exception);
      }
    }
  }

  private void notifyFailure(
      Duration duration,
      Exception failure
  ) {
    for (RetentionCycleListener listener : cycleListeners) {
      try {
        listener.onFailure(duration, failure);
      } catch (RuntimeException exception) {
        log.warn("Event retention failure listener failed", exception);
      }
    }
  }

  @Override
  public SchedulerType schedulerType() {
    return SchedulerType.EVENT_RETENTION;
  }

  @Override
  public SchedulerStatus status() {
    return lifecycle.status();
  }
}
