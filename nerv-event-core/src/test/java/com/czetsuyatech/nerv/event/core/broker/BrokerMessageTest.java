package com.czetsuyatech.nerv.event.core.broker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class BrokerMessageTest {

  @Test
  void retainsGenericEventMetadataAndTheTextualPayload() {
    SerializedPayload payload = new SerializedPayload(
        "{\"orderId\":42}",
        "application/json"
    );

    BrokerMessage message = new BrokerMessage(
        new EventId("event-1"),
        "order.created",
        Instant.parse("2026-08-15T00:00:00Z"),
        "orders",
        "correlation-1",
        "orders-topic",
        payload
    );

    assertThat(message.eventId()).isEqualTo(new EventId("event-1"));
    assertThat(message.eventType()).isEqualTo("order.created");
    assertThat(message.timestamp()).isEqualTo(Instant.parse("2026-08-15T00:00:00Z"));
    assertThat(message.source()).isEqualTo("orders");
    assertThat(message.correlationId()).isEqualTo("correlation-1");
    assertThat(message.target()).isEqualTo("orders-topic");
    assertThat(message.payload()).isSameAs(payload);
  }

  @Test
  void rejectsBlankPayloadContentType() {
    assertThatThrownBy(
        () -> new SerializedPayload(
            "{}",
            " "
        )
    )
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("contentType must not be blank");
  }
}
