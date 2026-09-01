package com.czetsuyatech.nerv.event.spring.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.EventHandlerRegistry;
import com.czetsuyatech.nerv.event.core.inbox.InboxEvent;
import com.czetsuyatech.nerv.event.core.inbox.InboxRegistration;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryDispatcher;
import com.czetsuyatech.nerv.event.core.inbox.InboxRetryPolicy;
import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.serialization.EventDeserializer;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.TaskScheduler;

class InboxRetrySchedulerTest {

  @Test
  void startsAtTheMinimumIntervalBacksOffWhenIdleAndResetsAfterClaimingWork() {
    RecordingInboxService repository = new RecordingInboxService();
    InboxRetryScheduler scheduler = scheduler(
        repository,
        "orders-api-abc12345:inbox-retry"
    );

    assertThat(scheduler.getBaseDelay()).isEqualTo(Duration.ofSeconds(1));
    scheduler.dispatch();
    assertThat(scheduler.getBaseDelay()).isEqualTo(Duration.ofSeconds(2));

    repository.claimedEvents = List.of(retryEvent());
    scheduler.dispatch();

    assertThat(scheduler.getBaseDelay()).isEqualTo(Duration.ofSeconds(1));
    assertThat(repository.claimOwners).containsOnly("orders-api-abc12345:inbox-retry");
  }

  @Test
  void capsIdleBackoffAndScheduledDelayAfterJitter() {
    RecordingInboxService repository = new RecordingInboxService();
    NervEventProperties.Inbox.Dispatcher properties = new NervEventProperties.Inbox.Dispatcher();
    properties.getPolling().setMaxInterval(Duration.ofSeconds(3));
    InboxRetryScheduler scheduler = scheduler(
        repository,
        properties,
        "orders-api-abc12345:inbox-retry",
        () -> 1.0
    );

    scheduler.dispatch();
    scheduler.dispatch();
    scheduler.dispatch();

    assertThat(scheduler.getBaseDelay()).isEqualTo(Duration.ofSeconds(3));
    assertThat(scheduler.getActualScheduledDelay()).isEqualTo(Duration.ofSeconds(3));
  }

  @Test
  void preventsLocalOverlapAndSurvivesACycleFailure() throws Exception {
    BlockingInboxService repository = new BlockingInboxService();
    InboxRetryScheduler scheduler = scheduler(
        repository,
        "orders-api-abc12345:inbox-retry"
    );

    Thread firstCycle = Thread.ofVirtual().start(scheduler::dispatch);
    assertThat(
        repository.claimStarted.await(
            5,
            TimeUnit.SECONDS
        )
    ).isTrue();
    scheduler.dispatch();
    assertThat(repository.claimCount.get()).isEqualTo(1);
    repository.releaseClaim.countDown();
    firstCycle.join();

    FailingThenSuccessfulInboxService failingRepository = new FailingThenSuccessfulInboxService();
    InboxRetryScheduler failingScheduler = scheduler(
        failingRepository,
        "orders-api-abc12345:inbox-retry"
    );
    failingScheduler.dispatch();
    assertThat(failingScheduler.getLastFailure()).isNotNull();
    failingScheduler.dispatch();
    assertThat(failingScheduler.getLastFailure()).isNull();
  }

  @Test
  void stopsReschedulingDuringShutdownAndRetainsItsStableOwner() {
    RecordingInboxService repository = new RecordingInboxService();
    InboxRetryScheduler scheduler = scheduler(
        repository,
        "orders-api-abc12345:inbox-retry"
    );

    scheduler.stop();
    scheduler.dispatch();

    assertThat(scheduler.isRunning()).isFalse();
    assertThat(scheduler.getOwner()).isEqualTo("orders-api-abc12345:inbox-retry");
    assertThat(repository.claimOwners).isEmpty();
  }

  private static InboxRetryScheduler scheduler(
      RecordingInboxService repository,
      String owner
  ) {
    return scheduler(
        repository,
        new NervEventProperties.Inbox.Dispatcher(),
        owner,
        () -> 0.5
    );
  }

  private static InboxRetryScheduler scheduler(
      RecordingInboxService repository,
      NervEventProperties.Inbox.Dispatcher properties,
      String owner,
      java.util.function.DoubleSupplier random
  ) {
    Clock clock = Clock.fixed(
        Instant.EPOCH,
        ZoneOffset.UTC
    );
    InboxRetryDispatcher dispatcher = new InboxRetryDispatcher(
        repository,
        new NoOpConsumerDispatcher(),
        new InboxRetryPolicy() {
          @Override
          public boolean canRetry(int attemptCount) {
            return false;
          }

          @Override
          public Instant nextAttemptAt(
              int attemptCount,
              Instant failedAt
          ) {
            return failedAt;
          }
        },
        clock,
        owner,
        properties.getLeaseDuration()
    );
    InboxRetryScheduler scheduler = InboxRetryScheduler.create(
        dispatcher,
        properties,
        owner,
        clock,
        noOpTaskScheduler(),
        new DispatcherPollingPolicy(
            properties.getPolling().getMinInterval(),
            properties.getPolling().getMaxInterval(),
            properties.getPolling().getMultiplier(),
            properties.getPolling().getJitter(),
            random
        )
    );
    scheduler.start();
    return scheduler;
  }

  private static TaskScheduler noOpTaskScheduler() {
    return (TaskScheduler) Proxy.newProxyInstance(
        InboxRetrySchedulerTest.class.getClassLoader(),
        new Class<?>[]{TaskScheduler.class},
        (
            proxy,
            method,
            arguments) -> method.getName().equals("schedule") ? new NoOpScheduledFuture() : null
    );
  }

  private static InboxEvent retryEvent() {
    return InboxEvent.builder()
        .eventId(new EventId("event-1"))
        .eventType("order.created")
        .timestamp(Instant.EPOCH)
        .source("orders")
        .payload(
            new SerializedPayload(
                "{}",
                "application/json"
            )
        )
        .status(InboxStatus.RETRY_PENDING)
        .attemptCount(1)
        .receivedAt(Instant.EPOCH)
        .availableAt(Instant.EPOCH)
        .build();
  }

  private static final class NoOpConsumerDispatcher extends ConsumerDispatcher {
    private NoOpConsumerDispatcher() {
      super(
          new EventHandlerRegistry(List.of()),
          new NoOpEventDeserializer(),
          List.of()
      );
    }

    @Override
    public void dispatch(com.czetsuyatech.nerv.event.core.consumer.ConsumerMessage message) {
    }
  }

  private static final class NoOpEventDeserializer implements EventDeserializer {
    @Override
    public <T> T deserialize(
        SerializedPayload payload,
        Class<T> payloadType
    ) {
      throw new UnsupportedOperationException();
    }
  }

  private static class RecordingInboxService implements InboxService {
    private volatile List<InboxEvent> claimedEvents = List.of();
    private final List<String> claimOwners = new java.util.ArrayList<>();

    @Override
    public InboxRegistration register(InboxEvent event) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Optional<InboxEvent> find(EventId eventId) {
      return Optional.empty();
    }

    @Override
    public Optional<InboxEvent> claim(
        EventId eventId,
        Instant now,
        String owner,
        Duration leaseDuration
    ) {
      return Optional.empty();
    }

    @Override
    public List<InboxEvent> claimPendingRetries(
        int limit,
        Instant now,
        String owner,
        Duration leaseDuration
    ) {
      claimOwners.add(owner);
      return claimedEvents;
    }

    @Override
    public void markProcessed(
        EventId eventId,
        String owner,
        Instant processedAt
    ) {
    }

    @Override
    public void markRetryPending(
        EventId eventId,
        String owner,
        int attemptCount,
        Instant failedAt,
        Instant availableAt,
        String error
    ) {
    }

    @Override
    public void markFailed(
        EventId eventId,
        String owner,
        int attemptCount,
        Instant failedAt,
        String error
    ) {
    }
  }

  private static final class BlockingInboxService extends RecordingInboxService {
    private final AtomicInteger claimCount = new AtomicInteger();
    private final CountDownLatch claimStarted = new CountDownLatch(1);
    private final CountDownLatch releaseClaim = new CountDownLatch(1);

    @Override
    public List<InboxEvent> claimPendingRetries(
        int limit,
        Instant now,
        String owner,
        Duration leaseDuration
    ) {
      claimCount.incrementAndGet();
      claimStarted.countDown();
      try {
        if (!releaseClaim.await(
            5,
            TimeUnit.SECONDS
        )) {
          throw new IllegalStateException("test dispatch cycle was not released");
        }
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(exception);
      }
      return List.of();
    }
  }

  private static final class FailingThenSuccessfulInboxService extends RecordingInboxService {
    private final AtomicInteger attempts = new AtomicInteger();

    @Override
    public List<InboxEvent> claimPendingRetries(
        int limit,
        Instant now,
        String owner,
        Duration leaseDuration
    ) {
      if (attempts.incrementAndGet() == 1) {
        throw new IllegalStateException("database unavailable");
      }
      return List.of();
    }
  }

  private static final class NoOpScheduledFuture implements ScheduledFuture<Object> {
    @Override
    public long getDelay(TimeUnit unit) {
      return 0;
    }

    @Override
    public int compareTo(java.util.concurrent.Delayed other) {
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
