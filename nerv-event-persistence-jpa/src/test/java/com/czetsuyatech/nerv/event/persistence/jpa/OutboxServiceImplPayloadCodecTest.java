package com.czetsuyatech.nerv.event.persistence.jpa;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.core.outbox.OutboxEvent;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.model.Destination;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventMessage;
import com.czetsuyatech.nerv.event.persistence.application.mapper.OutboxEventMapperImpl;
import com.czetsuyatech.nerv.event.persistence.service.impl.OutboxClaimStrategy;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.application.dto.OutboxPayloadCodec;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.OutboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.service.impl.OutboxServiceImpl;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class OutboxServiceImplPayloadCodecTest {

  private static final Instant NOW = Instant.parse("2026-08-16T12:00:00Z");
  private static final Clock CLOCK = Clock.fixed(
      NOW,
      ZoneOffset.UTC
  );

  @Test
  void saveEncodesTheArbitraryPayloadThroughThePayloadCodec() {
    AtomicReference<Object> encodedPayload = new AtomicReference<>();
    AtomicReference<OutboxEventEntity> savedEntity = new AtomicReference<>();
    OutboxPayloadCodec codec = new OutboxPayloadCodec() {
      @Override
      public String serialize(Object payload) {
        encodedPayload.set(payload);
        return "{\"orderId\":\"123\"}";
      }

      @Override
      public Object deserialize(String payload) {
        throw new UnsupportedOperationException();
      }
    };
    Map<String, String> payload = Map.of(
        "orderId",
        "123"
    );

    repository(
        recordingRepository(savedEntity),
        codec,
        noClaims()
    ).save(outboxEvent(payload));

    assertThat(encodedPayload.get()).isSameAs(payload);
    assertThat(savedEntity.get().getPayload()).isEqualTo("{\"orderId\":\"123\"}");
  }

  @Test
  void claimDecodesPersistedJsonThroughThePayloadCodec() {
    AtomicReference<String> decodedPayload = new AtomicReference<>();
    OutboxPayloadCodec codec = new OutboxPayloadCodec() {
      @Override
      public String serialize(Object payload) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object deserialize(String payload) {
        decodedPayload.set(payload);
        return Map.of(
            "orderId",
            "123"
        );
      }
    };
    OutboxEventEntity entity = jpaEvent("{\"orderId\":\"123\"}");

    List<OutboxEvent> claimed = repository(
        unusedRepository(),
        codec,
        (
            claimedAt,
            batchSize,
            owner,
            leaseDuration) -> List.of(entity)
    ).claimPending(
        NOW,
        1
    );

    assertThat(decodedPayload.get()).isEqualTo("{\"orderId\":\"123\"}");
    assertThat(claimed).singleElement().satisfies(event -> {
      assertThat(event.event().payload()).isEqualTo(
          Map.of(
              "orderId",
              "123"
          )
      );
      assertThat(event.id()).isEqualTo(new OutboxId("outbox-1"));
      assertThat(event.event().id()).isEqualTo(new EventId("event-1"));
      assertThat(event.destination()).isEqualTo(new Destination("orders.created"));
    });
  }

  @Test
  void payloadEncodeFailuresArePropagatedWithoutSavingAnEntity() {
    AtomicReference<OutboxEventEntity> savedEntity = new AtomicReference<>();
    OutboxPayloadCodec codec = failingCodec(
        "encode failed",
        true
    );

    assertThatThrownBy(
        () -> repository(
            recordingRepository(savedEntity),
            codec,
            noClaims()
        ).save(outboxEvent("payload"))
    )
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("encode failed");

    assertThat(savedEntity.get()).isNull();
  }

  @Test
  void payloadDecodeFailuresArePropagatedWhenClaimedEventsAreMapped() {
    OutboxPayloadCodec codec = failingCodec(
        "decode failed",
        false
    );

    assertThatThrownBy(
        () -> repository(
            unusedRepository(),
            codec,
            (
                claimedAt,
                batchSize,
                owner,
                leaseDuration) -> List.of(jpaEvent("{}"))
        ).claimPending(
            NOW,
            1
        )
    )
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("decode failed");
  }

  private static OutboxServiceImpl repository(
      OutboxEventRepository entityRepository,
      OutboxPayloadCodec payloadCodec,
      OutboxClaimStrategy claimStrategy
  ) {
    return new OutboxServiceImpl(
        entityRepository,
        new OutboxEventMapperImpl(),
        payloadCodec,
        claimStrategy,
        "worker-a",
        Duration.ofMinutes(1),
        CLOCK
    );
  }

  private static OutboxEventRepository recordingRepository(
      AtomicReference<OutboxEventEntity> savedEntity
  ) {
    return (OutboxEventRepository) Proxy.newProxyInstance(
        OutboxEventRepository.class.getClassLoader(),
        new Class<?>[]{OutboxEventRepository.class},
        (
            proxy,
            method,
            arguments) -> {
          if (method.getName().equals("save")) {
            OutboxEventEntity entity = (OutboxEventEntity) arguments[0];
            savedEntity.set(entity);
            return entity;
          }
          throw new UnsupportedOperationException(method.getName());
        }
    );
  }

  private static OutboxEventRepository unusedRepository() {
    return recordingRepository(new AtomicReference<>());
  }

  private static OutboxClaimStrategy noClaims() {
    return (
        claimedAt,
        batchSize,
        owner,
        leaseDuration) -> List.of();
  }

  private static OutboxPayloadCodec failingCodec(
      String message,
      boolean failOnEncode
  ) {
    return new OutboxPayloadCodec() {
      @Override
      public String serialize(Object payload) {
        if (failOnEncode) {
          throw new IllegalArgumentException(message);
        }
        throw new UnsupportedOperationException();
      }

      @Override
      public Object deserialize(String payload) {
        if (!failOnEncode) {
          throw new IllegalArgumentException(message);
        }
        throw new UnsupportedOperationException();
      }
    };
  }

  private static OutboxEvent outboxEvent(Object payload) {
    return new OutboxEvent(
        new OutboxId("outbox-1"),
        new EventMessage<>(
            new EventId("event-1"),
            "com.example.OrderCreated",
            NOW,
            "orders-service",
            null,
            payload
        ),
        new Destination("orders.created"),
        0,
        NOW,
        OutboxStatus.PENDING
    );
  }

  private static OutboxEventEntity jpaEvent(String payload) {
    OutboxEventEntity entity = new OutboxEventEntity();
    entity.setId("outbox-1");
    entity.setEventId("event-1");
    entity.setEventType("com.example.OrderCreated");
    entity.setSource("orders-service");
    entity.setEventTimestamp(NOW);
    entity.setDestination("orders.created");
    entity.setPayload(payload);
    entity.setStatus(OutboxStatus.PROCESSING);
    entity.setAttemptCount(1);
    entity.setAvailableAt(NOW);
    return entity;
  }
}
