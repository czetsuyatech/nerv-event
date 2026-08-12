package com.czetsuyatech.nerv.event.spring.dispatcher;

import com.czetsuyatech.nerv.event.core.outbox.DispatchResult;
import com.czetsuyatech.nerv.event.core.outbox.OutboxDispatcher;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import com.czetsuyatech.nerv.event.spring.scheduler.ResilientSchedulerLifecycle;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerStatus;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerStatusProvider;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerType;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.TaskScheduler;

/**
 * <p>
 * Triggers broker-neutral outbox dispatch batches with adaptive self-rescheduling.
 * </p>
 *
 * <p>
 * This class deliberately has no transaction boundary. The core dispatcher and persistence adapter retain the short
 * transactions around claim and state transitions.
 * </p>
 */
@Slf4j
@RequiredArgsConstructor
public final class OutboxDispatchScheduler implements SmartLifecycle, SchedulerStatusProvider {

  @NonNull
  private final OutboxDispatcher outboxDispatcher;

  @NonNull
  private final NervEventProperties.Dispatcher properties;

  @NonNull
  private final String owner;

  @NonNull
  private final Clock clock;

  @NonNull
  private final DispatcherPollingPolicy pollingPolicy;

  @NonNull
  private final List<OutboxDispatchCycleListener> cycleListeners;

  @NonNull
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
  private volatile DispatchResult lastResult;

  @Getter
  private volatile Throwable lastFailure;

  @Override
  public void start() {
    boolean started = lifecycle.start(
        () -> {
          baseDelay = pollingPolicy.initialBaseDelay();
          return actualScheduledDelay();
        },
        this::dispatch
    );
    if (started) {
      logActivated();
    }
  }

  @Override
  public void stop() {
    lifecycle.stop();
    log.info("Outbox dispatcher stopped owner={}", owner);
  }

  @Override
  public boolean isRunning() {
    return lifecycle.isRunning();
  }

  public void dispatch() {
    if (!lifecycle.beginWork()) {
      log.debug(
          "Skipping outbox dispatch during shutdown owner={}",
          owner
      );
      return;
    }
    lastCycleStartedAt = clock.instant();
    long startedAtNanos = System.nanoTime();
    try {
      log.debug(
          "Outbox dispatch cycle started owner={} batchSize={} leaseDuration={} baseDelay={}",
          owner,
          properties.getBatchSize(),
          properties.getLeaseDuration(),
          baseDelay
      );
      DispatchResult result = outboxDispatcher.dispatch(properties.getBatchSize());
      Duration duration = Duration.ofNanos(System.nanoTime() - startedAtNanos);
      lastResult = result;
      lastFailure = null;
      lifecycle.cycleSucceeded();
      baseDelay = pollingPolicy.nextBaseDelay(
          baseDelay,
          result
      );
      if (result.claimed() > 0) {
        log.debug(
            "Outbox dispatcher activity detected owner={} claimed={} nextDelay={}",
            owner,
            result.claimed(),
            baseDelay
        );
      } else {
        log.debug(
            "Outbox dispatcher idle owner={} nextDelay={}",
            owner,
            baseDelay
        );
      }
      log.info(
          "Outbox dispatch completed owner={} claimed={} published={} retried={} failed={} unresolved={} durationMs={}",
          owner,
          result.claimed(),
          result.published(),
          result.retried(),
          result.failed(),
          result.unresolved(),
          duration.toMillis()
      );
      notifySuccess(result, duration);
    } catch (Exception exception) {
      lifecycle.cycleFailed(exception);
      Duration duration = Duration.ofNanos(System.nanoTime() - startedAtNanos);
      lastFailure = exception;
      baseDelay = pollingPolicy.nextBaseDelay(
          baseDelay,
          DispatchResult.empty()
      );
      log.warn(
          "Unexpected outbox dispatch cycle failure owner={}",
          owner,
          exception
      );
      notifyFailure(duration, exception);
    } finally {
      lastCycleCompletedAt = clock.instant();
      log.debug(
          "Outbox dispatch cycle completed owner={}",
          owner
      );
      lifecycle.finishWork(this::actualScheduledDelay, this::dispatch);
    }
  }

  public static OutboxDispatchScheduler create(
      OutboxDispatcher outboxDispatcher,
      NervEventProperties.Dispatcher properties,
      String owner,
      Clock clock,
      TaskScheduler taskScheduler,
      DispatcherPollingPolicy pollingPolicy
  ) {
    return create(
        outboxDispatcher,
        properties,
        owner,
        clock,
        taskScheduler,
        pollingPolicy,
        List.of()
    );
  }

  public static OutboxDispatchScheduler create(
      OutboxDispatcher outboxDispatcher,
      NervEventProperties.Dispatcher properties,
      String owner,
      Clock clock,
      TaskScheduler taskScheduler,
      DispatcherPollingPolicy pollingPolicy,
      List<OutboxDispatchCycleListener> cycleListeners
  ) {
    Objects.requireNonNull(
        properties,
        "dispatcher properties must not be null"
    ).validate();
    return new OutboxDispatchScheduler(
        outboxDispatcher,
        properties,
        owner,
        clock,
        pollingPolicy,
        List.copyOf(cycleListeners),
        new ResilientSchedulerLifecycle(
            SchedulerType.OUTBOX_DISPATCH,
            clock,
            taskScheduler,
            properties.getPolling().getMaxInterval()
        )
    );
  }

  private void notifySuccess(
      DispatchResult result,
      Duration duration
  ) {
    for (OutboxDispatchCycleListener listener : cycleListeners) {
      try {
        listener.onSuccess(result, duration);
      } catch (RuntimeException exception) {
        log.warn("Outbox dispatch success listener failed owner={}", owner, exception);
      }
    }
  }

  private void notifyFailure(
      Duration duration,
      Exception failure
  ) {
    for (OutboxDispatchCycleListener listener : cycleListeners) {
      try {
        listener.onFailure(duration, failure);
      } catch (RuntimeException exception) {
        log.warn("Outbox dispatch failure listener failed owner={}", owner, exception);
      }
    }
  }

  private Duration actualScheduledDelay() {
    Duration scheduledDelay = pollingPolicy.jitteredDelay(baseDelay);
    actualScheduledDelay = scheduledDelay;
    log.debug(
        "Scheduling next outbox dispatch owner={} baseDelay={} scheduledDelay={}",
        owner,
        baseDelay,
        scheduledDelay
    );
    return scheduledDelay;
  }

  @Override
  public SchedulerType schedulerType() {
    return SchedulerType.OUTBOX_DISPATCH;
  }

  @Override
  public SchedulerStatus status() {
    return lifecycle.status();
  }

  private void logActivated() {
    NervEventProperties.Dispatcher.Polling polling = properties.getPolling();
    log.info(
        "Outbox dispatcher activated owner={} batchSize={} minInterval={} maxInterval={} multiplier={} jitter={} leaseDuration={}",
        owner,
        properties.getBatchSize(),
        polling.getMinInterval(),
        polling.getMaxInterval(),
        polling.getMultiplier(),
        polling.getJitter(),
        properties.getLeaseDuration()
    );
  }

  public String owner() {
    return owner;
  }
}
