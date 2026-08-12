package com.czetsuyatech.nerv.event.core.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducerRegistry;
import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.core.retry.RetryPolicy;
import com.czetsuyatech.nerv.event.core.routing.DestinationRoute;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.Destination;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventMessage;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class OutboxDispatcherTest {

  @Test
  void basesRetryDelayOnTheTimeOfFailure() {
    Instant batchStart = Instant.parse("2026-08-15T00:00:00Z");
    Instant failureTime = Instant.parse("2026-08-15T00:01:00Z");
    RecordingOutboxService repository = new RecordingOutboxService(List.of(outboxEvent(0)));

    OutboxDispatcher dispatcher = dispatcher(
        repository,
        new RetryPolicy() {
          @Override
          public boolean allowsRetry(int failedAttemptCount) {
            return true;
          }

          @Override
          public Instant nextEligibleAt(
              int failedAttemptCount,
              Instant failedAt
          ) {
            return failedAt.plusSeconds(30);
          }
        },
        new SequenceClock(
            batchStart,
            failureTime
        )
    );

    DispatchResult result = dispatcher.dispatch(1);

    assertThat(repository.rescheduledAttemptCount).isEqualTo(1);
    assertThat(repository.nextAttemptAt).isEqualTo(failureTime.plusSeconds(30));
    assertThat(result).isEqualTo(
        new DispatchResult(
            1,
            0,
            1,
            0,
            0
        )
    );
  }

  @Test
  void marksAnEventFailedWhenItsAttemptCountCannotBeIncremented() {
    OutboxEvent event = outboxEvent(Integer.MAX_VALUE);
    RecordingOutboxService repository = new RecordingOutboxService(List.of(event));

    OutboxDispatcher dispatcher = dispatcher(
        repository,
        new RetryPolicy() {
          @Override
          public boolean allowsRetry(int failedAttemptCount) {
            throw new AssertionError("retry policy must not be consulted after attempt count overflow");
          }

          @Override
          public Instant nextEligibleAt(
              int failedAttemptCount,
              Instant failedAt
          ) {
            throw new AssertionError("retry policy must not be consulted after attempt count overflow");
          }
        },
        Clock.fixed(
            Instant.parse("2026-08-15T00:00:00Z"),
            ZoneId.of("UTC")
        )
    );

    DispatchResult result = dispatcher.dispatch(1);

    assertThat(repository.failedAttemptCount).isEqualTo(Integer.MAX_VALUE);
    assertThat(repository.failureReason).isEqualTo("broker unavailable");
    assertThat(result).isEqualTo(
        new DispatchResult(
            1,
            0,
            0,
            1,
            0
        )
    );
  }

  @Test
  void returnsAnEmptyResultWhenNoEventsAreClaimed() {
    RecordingOutboxService repository = new RecordingOutboxService(List.of());

    DispatchResult result = dispatcher(
        repository,
        retryPolicy(true),
        Clock.systemUTC()
    ).dispatch(1);

    assertThat(result).isEqualTo(DispatchResult.empty());
  }

  @Test
  void countsAnUnresolvedEventWhenThePublishedTransitionFails() {
    RecordingOutboxService repository = new RecordingOutboxService(List.of(outboxEvent(0))) {
      @Override
      public void markPublished(
          OutboxId id,
          BrokerPublishResult result
      ) {
        throw new IllegalStateException("database unavailable");
      }
    };
    OutboxDispatcher dispatcher = new OutboxDispatcher(
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
        retryPolicy(true),
        Clock.systemUTC()
    );

    assertThat(dispatcher.dispatch(1)).isEqualTo(
        new DispatchResult(
            1,
            0,
            0,
            0,
            1
        )
    );
  }

  @Test
  void countsMixedOutcomesWithoutStoppingTheBatch() {
    RecordingOutboxService repository = new RecordingOutboxService(
        List.of(
            outboxEvent(0),
            outboxEvent(0),
            outboxEvent(0)
        )
    );
    AtomicInteger publishAttempts = new AtomicInteger();
    AtomicInteger retryDecisions = new AtomicInteger();
    BrokerProducer producer = new BrokerProducer() {
      @Override
      public BrokerId brokerId() {
        return new BrokerId("test");
      }

      @Override
      public BrokerPublishResult publish(BrokerMessage message) {
        if (publishAttempts.incrementAndGet() == 1) {
          return new BrokerPublishResult("published");
        }
        throw new IllegalStateException("broker unavailable");
      }
    };
    RetryPolicy retryPolicy = new RetryPolicy() {
      @Override
      public boolean allowsRetry(int failedAttemptCount) {
        return retryDecisions.incrementAndGet() == 1;
      }

      @Override
      public Instant nextEligibleAt(
          int failedAttemptCount,
          Instant failedAt
      ) {
        return failedAt.plusSeconds(30);
      }
    };

    DispatchResult result = dispatcher(
        repository,
        retryPolicy,
        Clock.systemUTC(),
        producer
    ).dispatch(3);

    assertThat(result).isEqualTo(
        new DispatchResult(
            3,
            1,
            1,
            1,
            0
        )
    );
    assertThat(publishAttempts).hasValue(3);
  }

  @Test
  void continuesAfterAnUnresolvedEvent() {
    AtomicInteger publishedTransitions = new AtomicInteger();
    RecordingOutboxService repository = new RecordingOutboxService(
        List.of(
            outboxEvent(0),
            outboxEvent(0)
        )
    ) {
      @Override
      public void markPublished(
          OutboxId id,
          BrokerPublishResult result
      ) {
        if (publishedTransitions.incrementAndGet() == 1) {
          throw new IllegalStateException("database unavailable");
        }
      }
    };

    DispatchResult result = dispatcher(
        repository,
        retryPolicy(true),
        Clock.systemUTC(),
        successfulProducer()
    ).dispatch(2);

    assertThat(result).isEqualTo(
        new DispatchResult(
            2,
            1,
            0,
            0,
            1
        )
    );
    assertThat(publishedTransitions).hasValue(2);
  }

  @Test
  void mapsEventMetadataAndResolvedTargetToTheBrokerMessage() {
    RecordingOutboxService repository = new RecordingOutboxService(List.of(outboxEvent(0)));
    BrokerMessage[] publishedMessage = new BrokerMessage[1];
    BrokerProducer producer = new BrokerProducer() {
      @Override
      public BrokerId brokerId() {
        return new BrokerId("test");
      }

      @Override
      public BrokerPublishResult publish(BrokerMessage message) {
        publishedMessage[0] = message;
        return new BrokerPublishResult("published");
      }
    };

    dispatcher(
        repository,
        retryPolicy(true),
        Clock.systemUTC(),
        producer
    ).dispatch(1);

    assertThat(publishedMessage[0]).isEqualTo(
        new BrokerMessage(
            new EventId("event-1"),
            "order.created",
            Instant.parse("2026-08-15T00:00:00Z"),
            "orders",
            null,
            "orders",
            new SerializedPayload(
                "{\"id\":\"event-1\"}",
                "application/json"
            )
        )
    );
  }

  private static RetryPolicy retryPolicy(boolean allowsRetry) {
    return new RetryPolicy() {
      @Override
      public boolean allowsRetry(int failedAttemptCount) {
        return allowsRetry;
      }

      @Override
      public Instant nextEligibleAt(
          int failedAttemptCount,
          Instant failedAt
      ) {
        return failedAt.plusSeconds(30);
      }
    };
  }

  private static OutboxDispatcher dispatcher(
      RecordingOutboxService repository,
      RetryPolicy retryPolicy,
      Clock clock
  ) {
    return dispatcher(
        repository,
        retryPolicy,
        clock,
        failingProducer()
    );
  }

  private static OutboxDispatcher dispatcher(
      RecordingOutboxService repository,
      RetryPolicy retryPolicy,
      Clock clock,
      BrokerProducer producer
  ) {
    return new OutboxDispatcher(
        repository,
        destination -> new DestinationRoute(
            new BrokerId("test"),
            "orders"
        ),
        event -> new SerializedPayload(
            "{\"id\":\"event-1\"}",
            "application/json"
        ),
        new BrokerProducerRegistry(List.of(producer)),
        retryPolicy,
        clock
    );
  }

  private static BrokerProducer failingProducer() {
    return new BrokerProducer() {
      @Override
      public BrokerId brokerId() {
        return new BrokerId("test");
      }

      @Override
      public BrokerPublishResult publish(BrokerMessage message) throws Exception {
        throw new IllegalStateException("broker unavailable");
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

  private static OutboxEvent outboxEvent(int attemptCount) {
    return new OutboxEvent(
        new OutboxId("outbox-1"),
        new EventMessage<>(
            new EventId("event-1"),
            "order.created",
            Instant.parse("2026-08-15T00:00:00Z"),
            "orders",
            null,
            "payload"
        ),
        new Destination("orders"),
        attemptCount,
        Instant.parse("2026-08-15T00:00:00Z"),
        OutboxStatus.PROCESSING
    );
  }

  private static class RecordingOutboxService implements OutboxService {
    private final List<OutboxEvent> claimedEvents;
    private int rescheduledAttemptCount;
    private Instant nextAttemptAt;
    private int failedAttemptCount;
    private String failureReason;

    private RecordingOutboxService(List<OutboxEvent> claimedEvents) {
      this.claimedEvents = claimedEvents;
    }

    @Override
    public OutboxEvent save(OutboxEvent event) {
      return event;
    }

    @Override
    public List<OutboxEvent> claimPending(
        Instant eligibleAt,
        int batchSize
    ) {
      return claimedEvents;
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
      rescheduledAttemptCount = attemptCount;
      this.nextAttemptAt = nextAttemptAt;
    }

    @Override
    public void markFailed(
        OutboxId id,
        int attemptCount,
        String failureReason
    ) {
      failedAttemptCount = attemptCount;
      this.failureReason = failureReason;
    }
  }

  private static final class SequenceClock extends Clock {
    private final Deque<Instant> instants;

    private SequenceClock(Instant... instants) {
      this.instants = new ArrayDeque<>(List.of(instants));
    }

    @Override
    public ZoneId getZone() {
      return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return instants.removeFirst();
    }
  }
}
