package com.czetsuyatech.nerv.event.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class EventMessageTest {

  private static final EventId ID = new EventId("evt-1");
  private static final Instant TIMESTAMP = Instant.parse("2026-08-14T00:00:00Z");

  @Test
  void buildsAnImmutableMessageAndAllowsNoCorrelationId() {
    EventMessage<String> message = EventMessage.<String>builder()
        .id(ID)
        .type("order.created")
        .timestamp(TIMESTAMP)
        .source("orders")
        .payload("payload")
        .build();

    assertThat(message.id()).isEqualTo(ID);
    assertThat(message.correlationId()).isNull();
    assertThat(message.payload()).isEqualTo("payload");
  }

  @Test
  void rejectsMissingRequiredReferences() {
    assertThatNullPointerException()
        .isThrownBy(() -> new EventMessage<String>(null, "type", TIMESTAMP, "source", null, "payload"));
    assertThatNullPointerException()
        .isThrownBy(() -> new EventMessage<String>(ID, null, TIMESTAMP, "source", null, "payload"));
    assertThatNullPointerException()
        .isThrownBy(() -> new EventMessage<String>(ID, "type", null, "source", null, "payload"));
    assertThatNullPointerException()
        .isThrownBy(() -> new EventMessage<String>(ID, "type", TIMESTAMP, null, null, "payload"));
    assertThatNullPointerException()
        .isThrownBy(() -> new EventMessage<String>(ID, "type", TIMESTAMP, "source", null, null));
  }

  @Test
  void rejectsBlankRequiredText() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new EventMessage<String>(ID, " ", TIMESTAMP, "source", null, "payload"));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new EventMessage<String>(ID, "type", TIMESTAMP, "", null, "payload"));
  }
}
