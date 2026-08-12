package com.czetsuyatech.nerv.event.core.inbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.consumer.ConsumerDispatcher;
import com.czetsuyatech.nerv.event.core.consumer.ConsumerMessage;
import com.czetsuyatech.nerv.event.core.consumer.EventHandlerRegistry;
import com.czetsuyatech.nerv.event.core.serialization.EventDeserializer;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.exception.EventRetryableException;
import com.czetsuyatech.nerv.event.model.EventId;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class InboxRetryDispatcherTest {

  private static final Instant NOW = Instant.parse("2026-08-17T00:00:00Z");
  private static final String OWNER = "orders-api-abc12345:inbox-retry";

  @Test
  void returnsAnEmptyResultWhenNoRetriesAreDue() {
    RecordingInboxService repository = new RecordingInboxService(List.of());

    InboxRetryResult result = dispatcher(
        repository,
        message -> {
        }
    ).dispatch(10);

    assertThat(result).isEqualTo(InboxRetryResult.empty());
    assertThat(repository.claimOwner).isEqualTo(OWNER);
  }

  @Test
  void reconstructsTheOriginalConsumerMessageAndMarksSuccessfulRetryProcessed() {
    InboxEvent event = retryEvent(
        "event-1",
        1
    );
    RecordingInboxService repository = new RecordingInboxService(List.of(event));
    RecordingConsumerDispatcher consumerDispatcher = new RecordingConsumerDispatcher(message -> {
    });

    InboxRetryResult result = dispatcher(
        repository,
        consumerDispatcher
    ).dispatch(10);

    assertThat(result).isEqualTo(
        new InboxRetryResult(
            1,
            1,
            0,
            0,
            0
        )
    );
    assertThat(consumerDispatcher.messages).containsExactly(
        new ConsumerMessage(
            event.eventId(),
            event.eventType(),
            event.timestamp(),
            event.source(),
            event.correlationId(),
            event.payload()
        )
    );
    assertThat(repository.processedEventIds).containsExactly(event.eventId());
    assertThat(repository.processedOwners).containsExactly(OWNER);
  }

  @Test
  void schedulesAnotherRetryAfterAHandlerFailureUsingTheIncrementedAttemptCount() {
    InboxEvent event = retryEvent(
        "event-1",
        1
    );
    RecordingInboxService repository = new RecordingInboxService(List.of(event));
    Instant availableAt = NOW.plusSeconds(30);
    InboxRetryPolicy policy = retryPolicy(
        true,
        availableAt
    );

    InboxRetryResult result = dispatcher(
        repository,
        new RecordingConsumerDispatcher(message -> {
          throw new EventRetryableException("handler unavailable");
        }),
        policy
    ).dispatch(10);

    assertThat(result).isEqualTo(
        new InboxRetryResult(
            1,
            0,
            1,
            0,
            0
        )
    );
    assertThat(repository.retryPendingAttemptCounts).containsExactly(2);
    assertThat(repository.retryPendingAvailableAts).containsExactly(availableAt);
    assertThat(repository.retryPendingOwners).containsExactly(OWNER);
  }

  @Test
  void marksTheEventTerminallyFailedWhenAutomaticRetriesAreExhausted() {
    InboxEvent event = retryEvent(
        "event-1",
        4
    );
    RecordingInboxService repository = new RecordingInboxService(List.of(event));

    InboxRetryResult result = dispatcher(
        repository,
        new RecordingConsumerDispatcher(message -> {
          throw new EventRetryableException("handler unavailable");
        }),
        retryPolicy(
            false,
            NOW.plusSeconds(30)
        )
    ).dispatch(10);

    assertThat(result).isEqualTo(
        new InboxRetryResult(
            1,
            0,
            0,
            1,
            0
        )
    );
    assertThat(repository.failedAttemptCounts).containsExactly(5);
    assertThat(repository.failedOwners).containsExactly(OWNER);
  }

  @Test
  void treatsEveryFinalStatePersistenceFailureAsUnresolved() {
    RecordingInboxService processedFailure = new RecordingInboxService(
        List.of(
            retryEvent(
                "processed",
                0
            )
        )
    ) {
      @Override
      public void markProcessed(
          EventId eventId,
          String owner,
          Instant processedAt
      ) {
        throw new IllegalStateException("database unavailable");
      }
    };
    RecordingInboxService retryFailure = new RecordingInboxService(
        List.of(
            retryEvent(
                "retry",
                0
            )
        )
    ) {
      @Override
      public void markRetryPending(
          EventId eventId,
          String owner,
          int attemptCount,
          Instant failedAt,
          Instant availableAt,
          String error
      ) {
        throw new IllegalStateException("database unavailable");
      }
    };
    RecordingInboxService failedFailure = new RecordingInboxService(
        List.of(
            retryEvent(
                "failed",
                0
            )
        )
    ) {
      @Override
      public void markFailed(
          EventId eventId,
          String owner,
          int attemptCount,
          Instant failedAt,
          String error
      ) {
        throw new IllegalStateException("database unavailable");
      }
    };

    assertThat(
        dispatcher(
            processedFailure,
            message -> {
            }
        ).dispatch(1)
    )
        .isEqualTo(
            new InboxRetryResult(
                1,
                0,
                0,
                0,
                1
            )
        );
    assertThat(
        dispatcher(
            retryFailure,
            message -> {
              throw new EventRetryableException("handler unavailable");
            }
        ).dispatch(1)
    ).isEqualTo(
        new InboxRetryResult(
            1,
            0,
            0,
            0,
            1
        )
    );
    assertThat(
        dispatcher(
            failedFailure,
            message -> {
              throw new EventRetryableException("handler unavailable");
            },
            retryPolicy(
                false,
                NOW.plusSeconds(30)
            )
        ).dispatch(1)
    ).isEqualTo(
        new InboxRetryResult(
            1,
            0,
            0,
            0,
            1
        )
    );
  }

  @Test
  void isolatesFailuresAndKeepsTheClaimedInvariantForAMixedBatch() {
    RecordingInboxService repository = new RecordingInboxService(
        List.of(
            retryEvent(
                "processed",
                0
            ),
            retryEvent(
                "retry",
                0
            ),
            retryEvent(
                "failed",
                1
            ),
            retryEvent(
                "unresolved",
                0
            )
        )
    ) {
      @Override
      public void markProcessed(
          EventId eventId,
          String owner,
          Instant processedAt
      ) {
        if (eventId.value().equals("unresolved")) {
          throw new IllegalStateException("database unavailable");
        }
        super.markProcessed(
            eventId,
            owner,
            processedAt
        );
      }
    };
    AtomicInteger invocations = new AtomicInteger();
    InboxRetryPolicy policy = new InboxRetryPolicy() {
      @Override
      public boolean canRetry(int attemptCount) {
        return attemptCount == 1;
      }

      @Override
      public Instant nextAttemptAt(
          int attemptCount,
          Instant failedAt
      ) {
        return failedAt.plusSeconds(30);
      }
    };

    InboxRetryResult result = dispatcher(
        repository,
        message -> {
          int invocation = invocations.incrementAndGet();
          if (invocation == 2 || invocation == 3) {
            throw new EventRetryableException("handler unavailable");
          }
        },
        policy
    ).dispatch(10);

    assertThat(result).isEqualTo(
        new InboxRetryResult(
            4,
            1,
            1,
            1,
            1
        )
    );
    assertThat(invocations).hasValue(4);
    assertThat(result.claimed()).isEqualTo(
        result.processed() + result.retryPending() + result.failed() + result.unresolved()
    );
  }

  private static InboxRetryDispatcher dispatcher(
      RecordingInboxService repository,
      ThrowingConsumer action
  ) {
    return dispatcher(
        repository,
        new RecordingConsumerDispatcher(action)
    );
  }

  private static InboxRetryDispatcher dispatcher(
      RecordingInboxService repository,
      RecordingConsumerDispatcher consumerDispatcher
  ) {
    return dispatcher(
        repository,
        consumerDispatcher,
        retryPolicy(
            true,
            NOW.plusSeconds(30)
        )
    );
  }

  private static InboxRetryDispatcher dispatcher(
      RecordingInboxService repository,
      ThrowingConsumer action,
      InboxRetryPolicy policy
  ) {
    return dispatcher(
        repository,
        new RecordingConsumerDispatcher(action),
        policy
    );
  }

  private static InboxRetryDispatcher dispatcher(
      RecordingInboxService repository,
      RecordingConsumerDispatcher consumerDispatcher,
      InboxRetryPolicy policy
  ) {
    return new InboxRetryDispatcher(
        repository,
        consumerDispatcher,
        policy,
        Clock.fixed(
            NOW,
            ZoneOffset.UTC
        ),
        OWNER,
        Duration.ofSeconds(30)
    );
  }

  private static InboxRetryPolicy retryPolicy(
      boolean canRetry,
      Instant availableAt
  ) {
    return new InboxRetryPolicy() {
      @Override
      public boolean canRetry(int attemptCount) {
        return canRetry;
      }

      @Override
      public Instant nextAttemptAt(
          int attemptCount,
          Instant failedAt
      ) {
        return availableAt;
      }
    };
  }

  private static InboxEvent retryEvent(
      String eventId,
      int attemptCount
  ) {
    return InboxEvent.builder()
        .eventId(new EventId(eventId))
        .eventType("order.created")
        .timestamp(NOW.minusSeconds(10))
        .source("orders")
        .correlationId("correlation-" + eventId)
        .payload(
            new SerializedPayload(
                "{\"id\":\"" + eventId + "\"}",
                "application/json"
            )
        )
        .status(InboxStatus.RETRY_PENDING)
        .attemptCount(attemptCount)
        .receivedAt(NOW.minusSeconds(10))
        .availableAt(NOW)
        .build();
  }

  @FunctionalInterface
  private interface ThrowingConsumer {
    void accept(ConsumerMessage message);
  }

  private static final class RecordingConsumerDispatcher extends ConsumerDispatcher {
    private final ThrowingConsumer action;
    private final List<ConsumerMessage> messages = new ArrayList<>();

    private RecordingConsumerDispatcher(ThrowingConsumer action) {
      super(
          new EventHandlerRegistry(List.of()),
          new NoOpEventDeserializer()
      );
      this.action = action;
    }

    @Override
    public void dispatch(ConsumerMessage message) {
      messages.add(message);
      action.accept(message);
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
    private final List<InboxEvent> claimedEvents;
    private String claimOwner;
    private final List<EventId> processedEventIds = new ArrayList<>();
    private final List<String> processedOwners = new ArrayList<>();
    private final List<Integer> retryPendingAttemptCounts = new ArrayList<>();
    private final List<Instant> retryPendingAvailableAts = new ArrayList<>();
    private final List<String> retryPendingOwners = new ArrayList<>();
    private final List<Integer> failedAttemptCounts = new ArrayList<>();
    private final List<String> failedOwners = new ArrayList<>();

    private RecordingInboxService(List<InboxEvent> claimedEvents) {
      this.claimedEvents = claimedEvents;
    }

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
      claimOwner = owner;
      return claimedEvents;
    }

    @Override
    public void markProcessed(
        EventId eventId,
        String owner,
        Instant processedAt
    ) {
      processedEventIds.add(eventId);
      processedOwners.add(owner);
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
      retryPendingAttemptCounts.add(attemptCount);
      retryPendingAvailableAts.add(availableAt);
      retryPendingOwners.add(owner);
    }

    @Override
    public void markFailed(
        EventId eventId,
        String owner,
        int attemptCount,
        Instant failedAt,
        String error
    ) {
      failedAttemptCounts.add(attemptCount);
      failedOwners.add(owner);
    }
  }
}
