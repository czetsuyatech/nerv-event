package com.czetsuyatech.nerv.event.core.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.consumer.EventHandler;
import com.czetsuyatech.nerv.event.model.EventMessage;
import java.util.List;
import org.junit.jupiter.api.Test;

class EventHandlerRegistryTest {

  @Test
  void registersAndResolvesAHandlerByEventType() {
    TestHandler orderHandler = new TestHandler("order.created");
    EventHandlerRegistry registry = new EventHandlerRegistry(List.of(orderHandler));

    assertThat(registry.handlerFor("order.created")).isSameAs(orderHandler);
  }

  @Test
  void registersHandlersForDifferentEventTypes() {
    TestHandler orderHandler = new TestHandler("order.created");
    TestHandler paymentHandler = new TestHandler("payment.captured");
    EventHandlerRegistry registry = new EventHandlerRegistry(
        List.of(
            orderHandler,
            paymentHandler
        )
    );

    assertThat(registry.handlerFor("order.created")).isSameAs(orderHandler);
    assertThat(registry.handlerFor("payment.captured")).isSameAs(paymentHandler);
  }

  @Test
  void rejectsUnknownEventTypesExplicitly() {
    EventHandlerRegistry registry = new EventHandlerRegistry(List.of());

    assertThatThrownBy(() -> registry.handlerFor("order.created"))
        .isInstanceOf(EventHandlerNotFoundException.class)
        .hasMessage("No EventHandler registered for event type 'order.created'");
  }

  @Test
  void failsDuringConstructionForDuplicateEventTypes() {
    assertThatThrownBy(
        () -> new EventHandlerRegistry(
            List.of(
                new FirstOrderHandler(),
                new SecondOrderHandler()
            )
        )
    )
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Multiple EventHandlers registered for event type 'order.created'")
        .hasMessageContaining(FirstOrderHandler.class.getName())
        .hasMessageContaining(SecondOrderHandler.class.getName());
  }

  @Test
  void acceptsAnEmptyRegistry() {
    assertThat(new EventHandlerRegistry(List.of())).isNotNull();
  }

  private static class TestHandler implements EventHandler<String> {
    private final String eventType;

    private TestHandler(String eventType) {
      this.eventType = eventType;
    }

    @Override
    public String eventType() {
      return eventType;
    }

    @Override
    public Class<String> payloadType() {
      return String.class;
    }

    @Override
    public void handle(EventMessage<String> event) {
    }
  }

  private static final class FirstOrderHandler extends TestHandler {
    private FirstOrderHandler() {
      super("order.created");
    }
  }

  private static final class SecondOrderHandler extends TestHandler {
    private SecondOrderHandler() {
      super("order.created");
    }
  }
}
