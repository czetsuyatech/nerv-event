package com.czetsuyatech.nerv.event.persistence.jpa.mapper;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.outbox.OutboxEvent;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.model.Destination;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventMessage;
import com.czetsuyatech.nerv.event.persistence.application.mapper.OutboxEventMapper;
import com.czetsuyatech.nerv.event.persistence.application.mapper.OutboxEventMapperImpl;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class OutboxEventEntityMapperTest {

  private static final Instant EVENT_TIMESTAMP = Instant.parse("2026-08-16T12:00:00Z");
  private static final Instant AVAILABLE_AT = EVENT_TIMESTAMP.plusSeconds(30);
  private final OutboxEventMapper mapper = new OutboxEventMapperImpl();

  @Test
  void mapsCoreOutboxEnvelopeToJpaEntityWithoutPayloadOrLifecycleMetadata() {
    OutboxEvent event = outboxEvent(
        "correlation-1",
        "payload"
    );

    OutboxEventEntity mapped = mapper.toJpa(event);

    assertThat(mapped.getId()).isEqualTo("outbox-1");
    assertThat(mapped.getEventId()).isEqualTo("event-1");
    assertThat(mapped.getEventType()).isEqualTo("com.example.OrderCreated");
    assertThat(mapped.getSource()).isEqualTo("orders-service");
    assertThat(mapped.getCorrelationId()).isEqualTo("correlation-1");
    assertThat(mapped.getEventTimestamp()).isEqualTo(EVENT_TIMESTAMP);
    assertThat(mapped.getDestination()).isEqualTo("orders.created");
    assertThat(mapped.getStatus()).isEqualTo(OutboxStatus.PENDING);
    assertThat(mapped.getAttemptCount()).isZero();
    assertThat(mapped.getAvailableAt()).isEqualTo(AVAILABLE_AT);
    assertThat(mapped.getPayload()).isNull();
    assertThat(mapped.getLockedAt()).isNull();
    assertThat(mapped.getLockedBy()).isNull();
    assertThat(mapped.getClaimVersion()).isZero();
    assertThat(mapped.getLastError()).isNull();
    assertThat(mapped.getCreatedAt()).isNull();
    assertThat(mapped.getUpdatedAt()).isNull();
    assertThat(mapped.getPublishedAt()).isNull();
    assertThat(mapped.getVersion()).isZero();
  }

  @Test
  void mapsJpaEntityToCoreOutboxEnvelopeWithNullableOptionalMetadata() {
    OutboxEventEntity entity = new OutboxEventEntity();
    entity.setId("outbox-1");
    entity.setEventId("event-1");
    entity.setEventType("com.example.OrderCreated");
    entity.setSource("orders-service");
    entity.setCorrelationId(null);
    entity.setEventTimestamp(EVENT_TIMESTAMP);
    entity.setDestination("orders.created");
    entity.setStatus(OutboxStatus.PROCESSING);
    entity.setAttemptCount(2);
    entity.setAvailableAt(AVAILABLE_AT);
    entity.setLockedAt(null);
    entity.setLockedBy(null);
    entity.setClaimVersion(7);
    entity.setLastError(null);
    entity.setPublishedAt(null);

    EventMessage<Object> mappedEvent = mapper.toEventMessage(
        entity,
        "decoded payload"
    );
    OutboxEvent mapped = mapper.toCore(
        entity,
        mappedEvent
    );

    assertThat(mapped.id()).isEqualTo(new OutboxId("outbox-1"));
    assertThat(mapped.event().id()).isEqualTo(new EventId("event-1"));
    assertThat(mapped.event().type()).isEqualTo("com.example.OrderCreated");
    assertThat(mapped.event().source()).isEqualTo("orders-service");
    assertThat(mapped.event().correlationId()).isNull();
    assertThat(mapped.event().timestamp()).isEqualTo(EVENT_TIMESTAMP);
    assertThat(mapped.event().payload()).isEqualTo("decoded payload");
    assertThat(mapped.destination()).isEqualTo(new Destination("orders.created"));
    assertThat(mapped.status()).isEqualTo(OutboxStatus.PROCESSING);
    assertThat(mapped.lockedBy()).isNull();
    assertThat(mapped.attemptCount()).isEqualTo(2);
    assertThat(mapped.nextAttemptAt()).isEqualTo(AVAILABLE_AT);
    assertThat(mapped.claimVersion()).isEqualTo(7);
  }

  @Test
  void mapsValueObjectsToAndFromTheirPersistedStringValues() {
    assertThat(mapper.toString(new OutboxId("outbox-1"))).isEqualTo("outbox-1");
    assertThat(mapper.toOutboxId("outbox-1")).isEqualTo(new OutboxId("outbox-1"));
    assertThat(mapper.toString(new EventId("event-1"))).isEqualTo("event-1");
    assertThat(mapper.toEventId("event-1")).isEqualTo(new EventId("event-1"));
    assertThat(mapper.toString(new Destination("orders.created"))).isEqualTo("orders.created");
    assertThat(mapper.toDestination("orders.created")).isEqualTo(new Destination("orders.created"));
  }

  private static OutboxEvent outboxEvent(
      String correlationId,
      Object payload
  ) {
    return new OutboxEvent(
        new OutboxId("outbox-1"),
        new EventMessage<>(
            new EventId("event-1"),
            "com.example.OrderCreated",
            EVENT_TIMESTAMP,
            "orders-service",
            correlationId,
            payload
        ),
        new Destination("orders.created"),
        0,
        AVAILABLE_AT,
        OutboxStatus.PENDING
    );
  }
}
