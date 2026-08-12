package com.czetsuyatech.nerv.event.spring.dispatcher;

import com.czetsuyatech.nerv.event.core.inbox.InboxRetryDispatcher;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryResult;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import com.czetsuyatech.nerv.event.spring.scheduler.ResilientSchedulerLifecycle;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerStatus;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerStatusProvider;
import com.czetsuyatech.nerv.event.spring.scheduler.SchedulerType;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.TaskScheduler;

/**
 * <p>
 * Runs due inbox retries with adaptive, self-rescheduled polling.
 * </p>
 *
 * <p>
 * Database claiming coordinates pods. This scheduler only prevents overlapping cycles in its own JVM and intentionally
 * has no transaction boundary.
 * </p>
 */
@Slf4j
@RequiredArgsConstructor
public final class InboxRetryScheduler implements SmartLifecycle, SchedulerStatusProvider {

  @NonNull
  private final InboxRetryDispatcher inboxRetryDispatcher;

  @NonNull
  private final NervEventProperties.Inbox.Dispatcher properties;

  @Getter
  @NonNull
  private final String owner;

  @NonNull
  private final Clock clock;

  @NonNull
  private final DispatcherPollingPolicy pollingPolicy;

  @NonNull
  private final List<InboxRetryCycleListener> cycleListeners;

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
  private volatile InboxRetryResult lastResult;

  @Getter
  private volatile Throwable lastFailure;

  @Override
  public void start() {
    boolean started = lifecycle.start(
        () -> {
          baseDelay = pollingPolicy.initialBaseDelay();
          return nextDelay();
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
    log.info("Inbox retry scheduler stopped owner={}", owner);
  }

  @Override
  public boolean isRunning() {
    return lifecycle.isRunning();
  }

  /**
   * Executes one cycle when active; the next cycle is queued only after this one finishes.
   */
  public void dispatch() {
    if (!lifecycle.beginWork()) {
      log.debug(
          "Skipping inbox retry dispatch during shutdown owner={}",
          owner
      );
      return;
    }
    lastCycleStartedAt = clock.instant();
    long startedAtNanos = System.nanoTime();
    try {
      InboxRetryResult result = inboxRetryDispatcher.dispatch(properties.getBatchSize());
      Duration duration = Duration.ofNanos(System.nanoTime() - startedAtNanos);
      lastResult = result;
      lastFailure = null;
      lifecycle.cycleSucceeded();
      baseDelay = pollingPolicy.nextBaseDelay(
          baseDelay,
          result.claimed() > 0
      );
      log.info(
          "Inbox retry completed owner={} claimed={} processed={} retryPending={} failed={} unresolved={} durationMs={}",
          owner,
          result.claimed(),
          result.processed(),
          result.retryPending(),
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
          false
      );
      log.warn(
          "Unexpected inbox retry dispatch cycle failure owner={}",
          owner,
          exception
      );
      notifyFailure(duration, exception);
    } finally {
      lastCycleCompletedAt = clock.instant();
      lifecycle.finishWork(this::nextDelay, this::dispatch);
    }
  }

  public static InboxRetryScheduler create(
      InboxRetryDispatcher inboxRetryDispatcher,
      NervEventProperties.Inbox.Dispatcher properties,
      String owner,
      Clock clock,
      TaskScheduler taskScheduler,
      DispatcherPollingPolicy pollingPolicy
  ) {
    return create(
        inboxRetryDispatcher,
        properties,
        owner,
        clock,
        taskScheduler,
        pollingPolicy,
        List.of()
    );
  }

  public static InboxRetryScheduler create(
      InboxRetryDispatcher inboxRetryDispatcher,
      NervEventProperties.Inbox.Dispatcher properties,
      String owner,
      Clock clock,
      TaskScheduler taskScheduler,
      DispatcherPollingPolicy pollingPolicy,
      List<InboxRetryCycleListener> cycleListeners
  ) {
    Objects.requireNonNull(
        properties,
        "inbox dispatcher properties must not be null"
    ).validate();
    return new InboxRetryScheduler(
        inboxRetryDispatcher,
        properties,
        owner,
        clock,
        pollingPolicy,
        List.copyOf(cycleListeners),
        new ResilientSchedulerLifecycle(
            SchedulerType.INBOX_RETRY,
            clock,
            taskScheduler,
            properties.getPolling().getMaxInterval()
        )
    );
  }

  private void notifySuccess(
      InboxRetryResult result,
      Duration duration
  ) {
    for (InboxRetryCycleListener listener : cycleListeners) {
      try {
        listener.onSuccess(result, duration);
      } catch (RuntimeException exception) {
        log.warn("Inbox retry success listener failed owner={}", owner, exception);
      }
    }
  }

  private void notifyFailure(
      Duration duration,
      Exception failure
  ) {
    for (InboxRetryCycleListener listener : cycleListeners) {
      try {
        listener.onFailure(duration, failure);
      } catch (RuntimeException exception) {
        log.warn("Inbox retry failure listener failed owner={}", owner, exception);
      }
    }
  }

  private Duration nextDelay() {
    Duration scheduledDelay = pollingPolicy.jitteredDelay(baseDelay);
    actualScheduledDelay = scheduledDelay;
    return scheduledDelay;
  }

  @Override
  public SchedulerType schedulerType() {
    return SchedulerType.INBOX_RETRY;
  }

  @Override
  public SchedulerStatus status() {
    return lifecycle.status();
  }

  private void logActivated() {
    NervEventProperties.Inbox.Dispatcher.Polling polling = properties.getPolling();
    log.info(
        "Inbox retry scheduler activated owner={} batchSize={} minInterval={} maxInterval={} multiplier={} jitter={} leaseDuration={}",
        owner,
        properties.getBatchSize(),
        polling.getMinInterval(),
        polling.getMaxInterval(),
        polling.getMultiplier(),
        polling.getJitter(),
        properties.getLeaseDuration()
    );
  }
}
