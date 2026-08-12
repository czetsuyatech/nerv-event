package com.czetsuyatech.nerv.event.spring.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.TaskScheduler;

class ResilientSchedulerLifecycleTest {

  private static final Clock CLOCK = Clock.fixed(Instant.EPOCH, ZoneOffset.UTC);

  @Test
  void schedulingFailureDuringStartTransitionsToFailedAndCanBeExplicitlyRestarted() {
    AtomicInteger calls = new AtomicInteger();
    ResilientSchedulerLifecycle lifecycle = lifecycle(taskScheduler(calls, 1));

    lifecycle.start(() -> Duration.ofSeconds(1), () -> {
    });

    assertThat(lifecycle.status().state()).isEqualTo(SchedulerState.FAILED);
    assertThat(lifecycle.status().scheduled()).isFalse();
    assertThat(lifecycle.status().schedulingFailureCount()).isEqualTo(1);
    assertThat(lifecycle.status().lastSchedulingFailure()).contains("rejected");

    lifecycle.start(() -> Duration.ofSeconds(1), () -> {
    });
    assertThat(lifecycle.status().state()).isEqualTo(SchedulerState.RUNNING);
    assertThat(lifecycle.status().scheduled()).isTrue();
  }

  @Test
  void successfulCycleRemainsSuccessfulWhenSuccessorSchedulingFails() {
    AtomicInteger calls = new AtomicInteger();
    ResilientSchedulerLifecycle lifecycle = lifecycle(taskScheduler(calls, 2));
    lifecycle.start(() -> Duration.ofSeconds(1), () -> {
    });

    assertThat(lifecycle.beginWork()).isTrue();
    lifecycle.cycleSucceeded();
    lifecycle.finishWork(() -> Duration.ofSeconds(2), () -> {
    });

    SchedulerStatus status = lifecycle.status();
    assertThat(status.state()).isEqualTo(SchedulerState.FAILED);
    assertThat(status.successfulCycleCount()).isEqualTo(1);
    assertThat(status.failedCycleCount()).isZero();
    assertThat(status.schedulingFailureCount()).isEqualTo(1);
  }

  @Test
  void successorDelayFailureTransitionsToFailedAndClearsWorkInProgress() {
    AtomicInteger calls = new AtomicInteger();
    ResilientSchedulerLifecycle lifecycle = lifecycle(taskScheduler(calls, -1));
    lifecycle.start(() -> Duration.ofSeconds(1), () -> {
    });
    assertThat(lifecycle.beginWork()).isTrue();
    lifecycle.cycleSucceeded();

    lifecycle.finishWork(
        () -> {
          throw new IllegalStateException("delay unavailable");
        },
        () -> {
        }
    );

    SchedulerStatus status = lifecycle.status();
    assertThat(status.state()).isEqualTo(SchedulerState.FAILED);
    assertThat(status.workInProgress()).isFalse();
    assertThat(status.scheduled()).isFalse();
    assertThat(status.schedulingFailureCount()).isEqualTo(1);
    assertThat(status.lastSchedulingFailure()).contains("delay unavailable");
    assertThat(calls).hasValue(1);
  }

  @Test
  void stopDuringWorkPreventsSuccessorAndRepeatedStartSchedulesOnlyOnce() {
    AtomicInteger calls = new AtomicInteger();
    ResilientSchedulerLifecycle lifecycle = lifecycle(taskScheduler(calls, -1));
    lifecycle.start(() -> Duration.ofSeconds(1), () -> {
    });
    lifecycle.start(() -> Duration.ofSeconds(1), () -> {
    });
    assertThat(calls).hasValue(1);

    assertThat(lifecycle.beginWork()).isTrue();
    lifecycle.stop();
    assertThat(lifecycle.start(() -> Duration.ofSeconds(1), () -> {
    })).isFalse();
    lifecycle.finishWork(() -> Duration.ofSeconds(1), () -> {
    });

    assertThat(calls).hasValue(1);
    assertThat(lifecycle.status().state()).isEqualTo(SchedulerState.STOPPED);
    assertThat(lifecycle.status().scheduled()).isFalse();
    assertThat(lifecycle.status().nextExecutionAt()).isNull();
  }

  @Test
  void concurrentStartInitializesAndSchedulesExactlyOnce() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger initializations = new AtomicInteger();
    ResilientSchedulerLifecycle lifecycle = lifecycle(taskScheduler(calls, -1));
    CountDownLatch start = new CountDownLatch(1);
    List<Thread> threads = new ArrayList<>();
    for (int index = 0; index < 16; index++) {
      threads.add(
          Thread.ofVirtual().start(() -> {
            try {
              start.await();
            } catch (InterruptedException exception) {
              Thread.currentThread().interrupt();
              throw new IllegalStateException(exception);
            }
            lifecycle.start(
                () -> {
                  initializations.incrementAndGet();
                  return Duration.ofSeconds(1);
                },
                () -> {
                }
            );
          })
      );
    }

    start.countDown();
    for (Thread thread : threads) {
      thread.join();
    }

    assertThat(initializations).hasValue(1);
    assertThat(calls).hasValue(1);
    assertThat(lifecycle.status().state()).isEqualTo(SchedulerState.RUNNING);
  }

  private static ResilientSchedulerLifecycle lifecycle(TaskScheduler scheduler) {
    return new ResilientSchedulerLifecycle(
        SchedulerType.OUTBOX_DISPATCH,
        CLOCK,
        scheduler,
        Duration.ofSeconds(30)
    );
  }

  private static TaskScheduler taskScheduler(
      AtomicInteger calls,
      int rejectCall
  ) {
    return (TaskScheduler) Proxy.newProxyInstance(
        ResilientSchedulerLifecycleTest.class.getClassLoader(),
        new Class<?>[]{TaskScheduler.class},
        (proxy, method, arguments) -> {
          if (!method.getName().equals("schedule")) {
            return null;
          }
          if (calls.incrementAndGet() == rejectCall) {
            throw new IllegalStateException("rejected");
          }
          return new ActiveFuture();
        }
    );
  }

  private static final class ActiveFuture implements ScheduledFuture<Object> {
    private boolean cancelled;

    @Override
    public long getDelay(TimeUnit unit) {
      return 0;
    }

    @Override
    public int compareTo(Delayed other) {
      return 0;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      cancelled = true;
      return true;
    }

    @Override
    public boolean isCancelled() {
      return cancelled;
    }

    @Override
    public boolean isDone() {
      return cancelled;
    }

    @Override
    public Object get() {
      return null;
    }

    @Override
    public Object get(
        long timeout,
        TimeUnit unit
    ) {
      return null;
    }
  }
}
