package com.czetsuyatech.nerv.event.spring.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventMessage;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

class JacksonEventSerializerTest {

  @Test
  void writesOnlyThePayloadAsJson() {
    JacksonEventSerializer serializer = new JacksonEventSerializer(new ObjectMapper());

    assertThat(
        serializer.serialize(
            EventMessage.builder()
                .id(new EventId("event-1"))
                .type("order.created")
                .timestamp(Instant.parse("2026-08-20T00:00:00Z"))
                .source("orders")
                .payload(new OrderCreated(42))
                .build()
        )
    )
        .isEqualTo(
            new SerializedPayload(
                "{\"orderId\":42}",
                "application/json"
            )
        );
  }

  private record OrderCreated(int orderId) {
  }
}
