package com.czetsuyatech.nerv.event.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class EventPublicationTest {

  private static final EventMessage<String> EVENT = new EventMessage<>(
      new EventId("evt-1"),
      "order.created",
      Instant.parse("2026-08-14T00:00:00Z"),
      "orders",
      null,
      "payload"
  );

  private static final Destination DESTINATION = new Destination("orders");

  @Test
  void buildsPublication() {
    EventPublication<String> publication = EventPublication.<String>builder()
        .event(EVENT)
        .destination(DESTINATION)
        .build();

    assertThat(publication.event()).isSameAs(EVENT);
    assertThat(publication.destination()).isEqualTo(DESTINATION);
  }

  @Test
  void rejectsMissingRequiredReferences() {
    assertThatNullPointerException().isThrownBy(() -> new EventPublication<String>(null, DESTINATION));
    assertThatNullPointerException().isThrownBy(() -> new EventPublication<>(EVENT, null));
  }
}
