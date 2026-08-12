package com.czetsuyatech.nerv.event.spring.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducerRegistry;
import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.core.outbox.OutboxDispatcher;
import com.czetsuyatech.nerv.event.core.outbox.OutboxEvent;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxService;
import com.czetsuyatech.nerv.event.core.retry.RetryPolicy;
import com.czetsuyatech.nerv.event.core.routing.DestinationRoute;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.spring.autoconfigure.NervEventProperties;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.TaskScheduler;

class OutboxDispatchSchedulerTest {

  @Test
  void invokesTheDispatcherWithTheConfiguredBatchSizeAndRetainsItsOwner() {
    RecordingService repository = new RecordingService();
    NervEventProperties.Dispatcher properties = dispatcherProperties(37);
    OutboxDispatchScheduler scheduler = scheduler(
        repository,
        properties,
        "orders-abc12345"
    );

    scheduler.dispatch();
    scheduler.dispatch();

    assertThat(repository.batchSizes).containsExactly(
        37,
        37
    );
    assertThat(scheduler.owner()).isEqualTo("orders-abc12345");
    assertThat(scheduler.getLastResult()).isNotNull();
    assertThat(scheduler.getLastCycleStartedAt()).isNotNull();
    assertThat(scheduler.getLastCycleCompletedAt()).isNotNull();
    assertThat(scheduler.getBaseDelay()).isEqualTo(java.time.Duration.ofSeconds(4));

    scheduler.stop();
    scheduler.dispatch();
    assertThat(repository.batchSizes).containsExactly(
        37,
        37
    );
  }

  @Test
  void skipsAnOverlappingLocalCycle() throws Exception {
    BlockingService repository = new BlockingService();
    OutboxDispatchScheduler scheduler = scheduler(
        repository,
        dispatcherProperties(1),
        "orders-abc12345"
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
  }

  @Test
  void catchesASchedulerLevelFailureAndAllowsTheNextCycleToRun() {
    FailingThenSuccessfulService repository = new FailingThenSuccessfulService();
    OutboxDispatchScheduler scheduler = scheduler(
        repository,
        dispatcherProperties(1),
        "orders-abc12345"
    );

    scheduler.dispatch();
    assertThat(scheduler.getLastFailure()).isNotNull();

    scheduler.dispatch();

    assertThat(repository.claimCount.get()).isEqualTo(2);
    assertThat(scheduler.getLastFailure()).isNull();
    assertThat(scheduler.getLastResult()).isNotNull();
  }

  @Test
  void hasNoTransactionAnnotation() {
    assertThat(OutboxDispatchScheduler.class.getAnnotations())
        .noneMatch(
            annotation -> annotation.annotationType()
                .getName()
                .equals("org.springframework.transaction.annotation.Transactional")
        );
  }

  private static NervEventProperties.Dispatcher dispatcherProperties(int batchSize) {
    NervEventProperties.Dispatcher properties = new NervEventProperties.Dispatcher();
    properties.setBatchSize(batchSize);
    return properties;
  }

  private static OutboxDispatchScheduler scheduler(
      OutboxService repository,
      NervEventProperties.Dispatcher properties,
      String owner
  ) {
    OutboxDispatchScheduler scheduler = OutboxDispatchScheduler.create(
        new OutboxDispatcher(
            repository,
            destination -> new DestinationRoute(
                new BrokerId("test"),
                "orders"
            ),
            event -> new SerializedPayload(
                "{\"id\":\"event-1\"}",
                "application/json"
            ),
            new BrokerProducerRegistry(List.of(successfulProducer())),
            retryPolicy(),
            Clock.fixed(
                Instant.EPOCH,
                ZoneOffset.UTC
            )
        ),
        properties,
        owner,
        Clock.fixed(
            Instant.EPOCH,
            ZoneOffset.UTC
        ),
        noOpTaskScheduler(),
        new DispatcherPollingPolicy(
            properties.getPolling().getMinInterval(),
            properties.getPolling().getMaxInterval(),
            properties.getPolling().getMultiplier(),
            0.0,
            () -> 0.5
        )
    );
    scheduler.start();
    return scheduler;
  }

  private static TaskScheduler noOpTaskScheduler() {
    return (TaskScheduler) Proxy.newProxyInstance(
        OutboxDispatchSchedulerTest.class.getClassLoader(),
        new Class<?>[]{TaskScheduler.class},
        (
            proxy,
            method,
            arguments) -> method.getName().equals("schedule") ? new NoOpScheduledFuture() : null
    );
  }

  private static RetryPolicy retryPolicy() {
    return new RetryPolicy() {
      @Override
      public boolean allowsRetry(int failedAttemptCount) {
        return true;
      }

      @Override
      public Instant nextEligibleAt(
          int failedAttemptCount,
          Instant failedAt
      ) {
        return failedAt.plusSeconds(1);
      }
    };
  }

  private static BrokerProducer successfulProducer() {
    return new BrokerProducer() {
      @Override
      public BrokerId brokerId() {
        return new BrokerId("test");
      }

      @Override
      public BrokerPublishResult publish(BrokerMessage message) {
        return new BrokerPublishResult("published");
      }
    };
  }

  private static class RecordingService implements OutboxService {
    private final List<Integer> batchSizes = new java.util.ArrayList<>();

    @Override
    public OutboxEvent save(OutboxEvent event) {
      return event;
    }

    @Override
    public List<OutboxEvent> claimPending(
        Instant eligibleAt,
        int batchSize
    ) {
      batchSizes.add(batchSize);
      return List.of();
    }

    @Override
    public void markPublished(
        OutboxId id,
        BrokerPublishResult result
    ) {
    }

    @Override
    public void reschedule(
        OutboxId id,
        int attemptCount,
        Instant nextAttemptAt,
        String failureReason
    ) {
    }

    @Override
    public void markFailed(
        OutboxId id,
        int attemptCount,
        String failureReason
    ) {
    }
  }

  private static final class BlockingService extends RecordingService {
    private final AtomicInteger claimCount = new AtomicInteger();
    private final CountDownLatch claimStarted = new CountDownLatch(1);
    private final CountDownLatch releaseClaim = new CountDownLatch(1);

    @Override
    public List<OutboxEvent> claimPending(
        Instant eligibleAt,
        int batchSize
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

  private static final class FailingThenSuccessfulService extends RecordingService {
    private final AtomicInteger claimCount = new AtomicInteger();

    @Override
    public List<OutboxEvent> claimPending(
        Instant eligibleAt,
        int batchSize
    ) {
      if (claimCount.incrementAndGet() == 1) {
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
