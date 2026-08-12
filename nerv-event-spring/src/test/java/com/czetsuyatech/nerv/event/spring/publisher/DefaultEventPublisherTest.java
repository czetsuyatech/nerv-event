package com.czetsuyatech.nerv.event.spring.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.core.outbox.OutboxEvent;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxService;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.model.Destination;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventMessage;
import com.czetsuyatech.nerv.event.model.EventPublication;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DefaultEventPublisherTest {
  private static final Instant NOW = Instant.parse("2026-08-16T12:00:00Z");

  @Test
  void createsAPendingOutboxEventFromThePublication() {
    AtomicReference<OutboxEvent> savedEvent = new AtomicReference<>();
    DefaultEventPublisher publisher = new DefaultEventPublisher(
        new RecordingOutboxService(savedEvent),
        () -> new OutboxId("outbox-1"),
        Clock.fixed(
            NOW,
            ZoneOffset.UTC
        )
    );
    EventMessage<String> event = new EventMessage<>(
        new EventId("event-1"),
        "com.example.OrderCreated",
        Instant.parse("2026-08-16T11:00:00Z"),
        "orders-service",
        "correlation-1",
        "payload"
    );
    EventPublication<String> publication = new EventPublication<>(
        event,
        new Destination("orders")
    );

    EventId publishedEventId = publisher.publish(publication);

    assertThat(publishedEventId).isEqualTo(event.id());
    assertThat(savedEvent.get()).satisfies(outboxEvent -> {
      assertThat(outboxEvent.id()).isEqualTo(new OutboxId("outbox-1"));
      assertThat(outboxEvent.event()).isSameAs(event);
      assertThat(outboxEvent.destination()).isEqualTo(new Destination("orders"));
      assertThat(outboxEvent.status()).isEqualTo(OutboxStatus.PENDING);
      assertThat(outboxEvent.attemptCount()).isZero();
      assertThat(outboxEvent.nextAttemptAt()).isEqualTo(NOW);
    });
  }

  @Test
  void propagatesPersistenceFailuresWithoutReplacingThem() {
    IllegalStateException failure = new IllegalStateException("transaction required");
    DefaultEventPublisher publisher = new DefaultEventPublisher(
        new FailingOutboxService(failure),
        () -> new OutboxId("outbox-1"),
        Clock.fixed(
            NOW,
            ZoneOffset.UTC
        )
    );

    assertThatThrownBy(() -> publisher.publish(publication()))
        .isSameAs(failure);
  }

  private static EventPublication<String> publication() {
    return new EventPublication<>(
        new EventMessage<>(
            new EventId("event-1"),
            "com.example.OrderCreated",
            NOW,
            "orders-service",
            "correlation-1",
            "payload"
        ),
        new Destination("orders")
    );
  }

  private static class RecordingOutboxService implements OutboxService {
    private final AtomicReference<OutboxEvent> savedEvent;

    private RecordingOutboxService(AtomicReference<OutboxEvent> savedEvent) {
      this.savedEvent = savedEvent;
    }

    @Override
    public OutboxEvent save(OutboxEvent event) {
      savedEvent.set(event);
      return event;
    }

    @Override
    public List<OutboxEvent> claimPending(
        Instant eligibleAt,
        int batchSize
    ) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void markPublished(
        OutboxId id,
        BrokerPublishResult result
    ) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void reschedule(
        OutboxId id,
        int attemptCount,
        Instant nextAttemptAt,
        String failureReason
    ) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void markFailed(
        OutboxId id,
        int attemptCount,
        String failureReason
    ) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class FailingOutboxService extends RecordingOutboxService {
    private final RuntimeException failure;

    private FailingOutboxService(RuntimeException failure) {
      super(new AtomicReference<>());
      this.failure = failure;
    }

    @Override
    public OutboxEvent save(OutboxEvent event) {
      throw failure;
    }
  }
}
