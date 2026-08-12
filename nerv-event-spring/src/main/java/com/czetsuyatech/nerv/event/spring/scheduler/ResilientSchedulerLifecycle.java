package com.czetsuyatech.nerv.event.spring.scheduler;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.TaskScheduler;

/** Thread-safe lifecycle state machine shared by all self-rescheduling NERV schedulers. */
@Slf4j
public final class ResilientSchedulerLifecycle {

  private final Object monitor = new Object();
  private final SchedulerType type;
  private final Clock clock;
  private final TaskScheduler taskScheduler;
  private final Duration maximumInterval;
  private SchedulerState state = SchedulerState.STOPPED;
  private ScheduledFuture<?> scheduledFuture;
  private boolean workInProgress;
  private Instant startedAt;
  private Instant lastStartedAt;
  private Instant lastSuccessfulCompletionAt;
  private Instant lastCycleFailureAt;
  private String lastCycleFailure;
  private Instant lastSchedulingFailureAt;
  private String lastSchedulingFailure;
  private Instant nextExecutionAt;
  private long successfulCycleCount;
  private long failedCycleCount;
  private long schedulingFailureCount;

  public ResilientSchedulerLifecycle(
      SchedulerType type,
      Clock clock,
      TaskScheduler taskScheduler,
      Duration maximumInterval
  )
  {
    this.type = Objects.requireNonNull(type);
    this.clock = Objects.requireNonNull(clock);
    this.taskScheduler = Objects.requireNonNull(taskScheduler);
    this.maximumInterval = Objects.requireNonNull(maximumInterval);
  }

  public boolean start(
      Supplier<Duration> delaySupplier,
      Runnable task
  ) {
    synchronized (monitor) {
      if (state == SchedulerState.RUNNING || state == SchedulerState.STARTING || workInProgress) {
        return false;
      }
      state = SchedulerState.STARTING;
      startedAt = clock.instant();
      try {
        scheduleLocked(Objects.requireNonNull(delaySupplier.get()), task, "initial execution");
      } catch (RuntimeException failure) {
        recordSchedulingFailureLocked(failure, "initial execution preparation", null);
      }
      return true;
    }
  }

  public void stop() {
    synchronized (monitor) {
      state = SchedulerState.STOPPED;
      if (scheduledFuture != null) {
        scheduledFuture.cancel(false);
      }
      scheduledFuture = null;
      nextExecutionAt = null;
    }
  }

  public boolean beginWork() {
    synchronized (monitor) {
      if (state != SchedulerState.RUNNING || workInProgress) {
        return false;
      }
      if (scheduledFuture != null && !scheduledFuture.isDone()) {
        scheduledFuture.cancel(false);
      }
      scheduledFuture = null;
      nextExecutionAt = null;
      workInProgress = true;
      lastStartedAt = clock.instant();
      return true;
    }
  }

  public void cycleSucceeded() {
    synchronized (monitor) {
      successfulCycleCount++;
      lastSuccessfulCompletionAt = clock.instant();
    }
  }

  public void cycleFailed(Throwable failure) {
    synchronized (monitor) {
      failedCycleCount++;
      lastCycleFailureAt = clock.instant();
      lastCycleFailure = summarize(failure);
    }
  }

  public void finishWork(
      Supplier<Duration> delaySupplier,
      Runnable task
  ) {
    synchronized (monitor) {
      workInProgress = false;
      if (state != SchedulerState.RUNNING) {
        return;
      }
      try {
        scheduleLocked(Objects.requireNonNull(delaySupplier.get()), task, "successor execution");
      } catch (RuntimeException failure) {
        recordSchedulingFailureLocked(failure, "successor execution preparation", null);
      }
    }
  }

  public boolean isRunning() {
    synchronized (monitor) {
      reconcileRunningInvariantLocked();
      return state == SchedulerState.RUNNING;
    }
  }

  public SchedulerStatus status() {
    synchronized (monitor) {
      reconcileRunningInvariantLocked();
      boolean scheduled = scheduledFuture != null && !scheduledFuture.isDone() && !scheduledFuture.isCancelled();
      return new SchedulerStatus(
          state,
          scheduled,
          workInProgress,
          startedAt,
          lastStartedAt,
          lastSuccessfulCompletionAt,
          lastCycleFailureAt,
          lastCycleFailure,
          lastSchedulingFailureAt,
          lastSchedulingFailure,
          scheduled ? nextExecutionAt : null,
          successfulCycleCount,
          failedCycleCount,
          schedulingFailureCount,
          maximumInterval
      );
    }
  }

  private void reconcileRunningInvariantLocked() {
    boolean scheduled = scheduledFuture != null && !scheduledFuture.isDone() && !scheduledFuture.isCancelled();
    if (state != SchedulerState.RUNNING || scheduled || workInProgress) {
      return;
    }
    IllegalStateException failure = new IllegalStateException("scheduled execution completed without starting work");
    schedulingFailureCount++;
    lastSchedulingFailureAt = clock.instant();
    lastSchedulingFailure = summarize(failure);
    scheduledFuture = null;
    nextExecutionAt = null;
    state = SchedulerState.FAILED;
    log.error(
        "Scheduler {} lost its scheduled execution; scheduler transitioned to FAILED and explicit restart is required",
        type.tag(),
        failure
    );
  }

  private void scheduleLocked(
      Duration delay,
      Runnable task,
      String operation
  ) {
    Instant executionAt = clock.instant().plus(delay);
    try {
      ScheduledFuture<?> future = taskScheduler.schedule(task, executionAt);
      if (future == null) {
        throw new IllegalStateException("TaskScheduler returned no ScheduledFuture");
      }
      scheduledFuture = future;
      nextExecutionAt = executionAt;
      state = SchedulerState.RUNNING;
    } catch (RuntimeException failure) {
      recordSchedulingFailureLocked(failure, operation, delay);
    }
  }

  private void recordSchedulingFailureLocked(
      RuntimeException failure,
      String operation,
      Duration delay
  ) {
    schedulingFailureCount++;
    lastSchedulingFailureAt = clock.instant();
    lastSchedulingFailure = summarize(failure);
    scheduledFuture = null;
    nextExecutionAt = null;
    state = SchedulerState.FAILED;
    log.error(
        "Scheduler {} could not schedule {}; scheduler transitioned to FAILED and explicit restart is required delay={}",
        type.tag(),
        operation,
        delay,
        failure
    );
  }

  private static String summarize(Throwable failure) {
    String message = failure.getMessage();
    return failure.getClass().getSimpleName() + (message == null || message.isBlank() ? "" : ": " + message);
  }
}
