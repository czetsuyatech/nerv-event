package com.czetsuyatech.nerv.event.spring.retention;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.retention.EventRetention;
import com.czetsuyatech.nerv.event.core.retention.RetentionSidecarCleaner;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import com.czetsuyatech.nerv.event.spring.dispatcher.DispatcherPollingPolicy;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.TaskScheduler;

class EventRetentionSchedulerTest {

  private static final Instant NOW = Instant.parse("2026-08-18T12:00:00Z");

  @Test
  void usesInjectedClockCutoffsAndResetsThenBacksOffBasedOnDeletionActivity() {
    Recording repository = new Recording();
    EventRetentionScheduler scheduler = scheduler(
        repository,
        limit -> 0
    );

    scheduler.retain();

    assertThat(repository.outboxCutoff).isEqualTo(NOW.minus(Duration.ofDays(30)));
    assertThat(repository.inboxCutoff).isEqualTo(NOW.minus(Duration.ofDays(30)));
    assertThat(repository.limit).isEqualTo(7);
    assertThat(scheduler.getLastResult()).isEqualTo(
        new RetentionResult(
            2,
            0,
            0
        )
    );
    assertThat(scheduler.getBaseDelay()).isEqualTo(Duration.ofSeconds(2));

    repository.outboxDeleted = 0;
    scheduler.retain();

    assertThat(scheduler.getBaseDelay()).isEqualTo(Duration.ofSeconds(4));
    scheduler.stop();
  }

  @Test
  void skipsConfiguredCategoriesAndSurvivesAFailureOnTheNextCycle() {
    Recording repository = new Recording();
    NervEventProperties.Retention properties = properties();
    properties.getOutbox().setEnabled(false);
    properties.getInbox().setEnabled(false);
    EventRetentionScheduler scheduler = EventRetentionScheduler.create(
        repository,
        Optional.empty(),
        properties,
        Clock.fixed(
            NOW,
            ZoneOffset.UTC
        ),
        noOpTaskScheduler(),
        pollingPolicy(),
        java.util.List.of()
    );
    scheduler.start();

    scheduler.retain();
    assertThat(repository.outboxCutoff).isNull();
    assertThat(repository.inboxCutoff).isNull();

    properties.getOutbox().setEnabled(true);
    repository.failure = new IllegalStateException("database unavailable");
    scheduler.retain();
    assertThat(scheduler.getLastFailure()).isNotNull();

    repository.failure = null;
    scheduler.retain();
    assertThat(scheduler.getLastFailure()).isNull();
    scheduler.stop();
  }

  @Test
  void hasNoTransactionAnnotationBecauseAdaptersOwnTheShortTransactions() {
    assertThat(EventRetentionScheduler.class.getAnnotations())
        .noneMatch(
            annotation -> annotation.annotationType()
                .getName()
                .equals("org.springframework.transaction.annotation.Transactional")
        );
  }

  private static EventRetentionScheduler scheduler(
      Recording repository,
      RetentionSidecarCleaner sidecarCleaner
  ) {
    EventRetentionScheduler scheduler = EventRetentionScheduler.create(
        repository,
        Optional.of(sidecarCleaner),
        properties(),
        Clock.fixed(
            NOW,
            ZoneOffset.UTC
        ),
        noOpTaskScheduler(),
        pollingPolicy(),
        java.util.List.of()
    );
    scheduler.start();
    return scheduler;
  }

  private static NervEventProperties.Retention properties() {
    NervEventProperties.Retention properties = new NervEventProperties.Retention();
    properties.setBatchSize(7);
    properties.getPolling().setMinInterval(Duration.ofSeconds(2));
    properties.getPolling().setMaxInterval(Duration.ofSeconds(8));
    return properties;
  }

  private static DispatcherPollingPolicy pollingPolicy() {
    return new DispatcherPollingPolicy(
        Duration.ofSeconds(2),
        Duration.ofSeconds(8),
        2.0,
        0.0,
        () -> 0.5
    );
  }

  private static TaskScheduler noOpTaskScheduler() {
    return (TaskScheduler) Proxy.newProxyInstance(
        EventRetentionSchedulerTest.class.getClassLoader(),
        new Class<?>[]{TaskScheduler.class},
        (
            proxy,
            method,
            arguments) -> method.getName().equals("schedule") ? new NoOpScheduledFuture() : null
    );
  }

  private static final class Recording implements EventRetention {
    private Instant outboxCutoff;
    private Instant inboxCutoff;
    private int limit;
    private int outboxDeleted = 2;
    private RuntimeException failure;

    @Override
    public int deletePublishedBefore(
        Instant cutoff,
        int batchSize
    ) {
      if (failure != null)
        throw failure;
      outboxCutoff = cutoff;
      limit = batchSize;
      return outboxDeleted;
    }

    @Override
    public int deleteProcessedBefore(
        Instant cutoff,
        int batchSize
    ) {
      inboxCutoff = cutoff;
      limit = batchSize;
      return 0;
    }
  }

  private static final class NoOpScheduledFuture implements ScheduledFuture<Object> {
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
      return true;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return false;
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
