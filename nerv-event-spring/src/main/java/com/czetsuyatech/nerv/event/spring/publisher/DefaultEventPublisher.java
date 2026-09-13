package com.czetsuyatech.nerv.event.spring.publisher;

import com.czetsuyatech.nerv.event.core.outbox.OutboxEvent;
import com.czetsuyatech.nerv.event.core.outbox.OutboxIdGenerator;
import com.czetsuyatech.nerv.event.core.outbox.OutboxService;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventPublication;
import com.czetsuyatech.nerv.event.publisher.EventPublisher;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Publishes application events by adding their logical delivery intent to the transactional outbox.
 */
@Slf4j
@RequiredArgsConstructor
public final class DefaultEventPublisher implements EventPublisher {

  @NonNull
  private final OutboxService outboxService;

  @NonNull
  private final OutboxIdGenerator outboxIdGenerator;

  @NonNull
  private final Clock clock;

  @Override
  public <T> EventId publish(EventPublication<T> publication) {
    Objects.requireNonNull(
        publication,
        "publication must not be null"
    );
    Instant availableAt = clock.instant();
    OutboxEvent outboxEvent = new OutboxEvent(
        outboxIdGenerator.nextId(),
        publication.event(),
        publication.destination(),
        0,
        availableAt,
        OutboxStatus.PENDING,
        publication.orderingKey(),
        null,
        0
    );
    log.debug(
        "Outbox event prepared outboxId={} eventId={} eventType={} destination={} correlationId={} orderingKey={}",
        outboxEvent.id().value(),
        publication.event().id().value(),
        publication.event().type(),
        publication.destination().name(),
        publication.event().correlationId(),
        publication.orderingKey()
    );
    outboxService.save(outboxEvent);
    log.debug(
        "Outbox event persisted outboxId={} eventId={} eventType={} destination={} correlationId={}",
        outboxEvent.id().value(),
        publication.event().id().value(),
        publication.event().type(),
        publication.destination().name(),
        publication.event().correlationId()
    );
    return publication.event().id();
  }
}
