package com.czetsuyatech.nerv.event;

import static org.assertj.core.api.Assertions.assertThat;

import com.czetsuyatech.nerv.event.consumer.EventHandler;
import com.czetsuyatech.nerv.event.model.Destination;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventMessage;
import com.czetsuyatech.nerv.event.model.EventPublication;
import com.czetsuyatech.nerv.event.publisher.EventPublisher;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class ApiContractTest {

  private final EventMessage<String> event = new EventMessage<>(
      new EventId("evt-1"),
      "order.created",
      Instant.parse("2026-08-14T00:00:00Z"),
      "orders",
      null,
      "payload"
  );

  @Test
  void publisherContractIsGenericAndReturnsTheEventId() {
    EventPublisher publisher = new EventPublisher() {
      @Override
      public <T> EventId publish(EventPublication<T> publication) {
        return publication.event().id();
      }
    };

    assertThat(publisher.publish(new EventPublication<>(event, new Destination("orders")))).isEqualTo(event.id());
  }

  @Test
  void handlerContractDescribesAndReceivesItsPayloadType() {
    RecordingHandler handler = new RecordingHandler();

    handler.handle(event);

    assertThat(handler.eventType()).isEqualTo("order.created");
    assertThat(handler.payloadType()).isEqualTo(String.class);
    assertThat(handler.received).isSameAs(event);
  }

  private static final class RecordingHandler implements EventHandler<String> {
    private EventMessage<String> received;

    @Override
    public String eventType() {
      return "order.created";
    }

    @Override
    public Class<String> payloadType() {
      return String.class;
    }

    @Override
    public void handle(EventMessage<String> event) {
      received = event;
    }
  }
}
