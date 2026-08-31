package com.czetsuyatech.nerv.event.core.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.consumer.EventHandler;
import com.czetsuyatech.nerv.event.consumer.EventHandlerInterceptor;
import com.czetsuyatech.nerv.event.core.serialization.EventDeserializer;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.exception.EventRetryableException;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventMessage;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ConsumerDispatcherTest {

  @Test
  void reconstructsAndDispatchesTheOriginalEventExactlyOnce() {
    RecordingHandler handler = new RecordingHandler();
    RecordingDeserializer deserializer = new RecordingDeserializer("deserialized-order");
    ConsumerMessage message = message("correlation-1");
    ConsumerDispatcher dispatcher = new ConsumerDispatcher(
        new EventHandlerRegistry(List.of(handler)),
        deserializer,
        List.of()
    );

    dispatcher.dispatch(message);

    assertThat(deserializer.payload).isSameAs(message.payload());
    assertThat(deserializer.payloadType).isEqualTo(String.class);
    assertThat(handler.invocations).hasValue(1);
    assertThat(handler.received).isEqualTo(
        new EventMessage<>(
            message.eventId(),
            message.eventType(),
            message.timestamp(),
            message.source(),
            message.correlationId(),
            "deserialized-order"
        )
    );
  }

  @Test
  void propagatesUnknownHandlerFailures() {
    ConsumerDispatcher dispatcher = new ConsumerDispatcher(
        new EventHandlerRegistry(List.of()),
        new RecordingDeserializer("unused"),
        List.of()
    );

    assertThatThrownBy(() -> dispatcher.dispatch(message(null)))
        .isInstanceOf(EventHandlerNotFoundException.class)
        .hasMessageContaining("order.created");
  }

  @Test
  void addsContextAndPreservesTheCauseForDeserializationFailures() {
    IllegalArgumentException deserializationFailure = new IllegalArgumentException("invalid JSON");
    EventDeserializer deserializer = new EventDeserializer() {
      @Override
      public <T> T deserialize(
          SerializedPayload payload,
          Class<T> payloadType
      ) {
        throw deserializationFailure;
      }
    };
    ConsumerDispatcher dispatcher = new ConsumerDispatcher(
        new EventHandlerRegistry(List.of(new RecordingHandler())),
        deserializer,
        List.of()
    );

    assertThatThrownBy(() -> dispatcher.dispatch(message(null)))
        .isInstanceOf(EventDeserializationException.class)
        .hasMessageContaining("eventId=event-1")
        .hasMessageContaining("eventType=order.created")
        .hasMessageContaining("payloadType=java.lang.String")
        .hasMessageContaining("contentType=application/json")
        .cause()
        .isSameAs(deserializationFailure);
  }

  @Test
  void propagatesHandlerFailuresWithoutRetrying() {
    IllegalStateException handlerFailure = new IllegalStateException("business failure");
    AtomicInteger invocations = new AtomicInteger();
    EventHandler<String> handler = new EventHandler<>() {
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
        invocations.incrementAndGet();
        throw handlerFailure;
      }
    };
    ConsumerDispatcher dispatcher = new ConsumerDispatcher(
        new EventHandlerRegistry(List.of(handler)),
        new RecordingDeserializer("deserialized-order"),
        List.of()
    );

    assertThatThrownBy(() -> dispatcher.dispatch(message(null))).isSameAs(handlerFailure);
    assertThat(invocations).hasValue(1);
  }

  @Test
  void wrapsTheHandlerInConfiguredInterceptorOrder() {
    List<String> calls = new ArrayList<>();
    RecordingHandler handler = new RecordingHandler() {
      @Override
      public void handle(EventMessage<String> event) {
        calls.add("handler");
        super.handle(event);
      }
    };
    ConsumerDispatcher dispatcher = new ConsumerDispatcher(
        new EventHandlerRegistry(List.of(handler)),
        new RecordingDeserializer("deserialized-order"),
        List.of(
            around("first", calls),
            around("second", calls)
        )
    );

    dispatcher.dispatch(message(null));

    assertThat(calls).containsExactly(
        "first-before",
        "second-before",
        "handler",
        "second-after",
        "first-after"
    );
    assertThat(handler.invocations).hasValue(1);
  }

  @Test
  void runsInterceptorCleanupWhenTheHandlerFails() {
    List<String> calls = new ArrayList<>();
    IllegalStateException failure = new IllegalStateException("business failure");
    EventHandler<String> handler = handlerThatThrows(
        calls,
        failure
    );
    ConsumerDispatcher dispatcher = new ConsumerDispatcher(
        new EventHandlerRegistry(List.of(handler)),
        new RecordingDeserializer("deserialized-order"),
        List.of(around("context", calls))
    );

    assertThatThrownBy(() -> dispatcher.dispatch(message(null))).isSameAs(failure);
    assertThat(calls).containsExactly(
        "context-before",
        "handler",
        "context-after"
    );
  }

  @Test
  void doesNotInvokeTheHandlerWhenAnInterceptorFailsBeforeProceeding() {
    AtomicInteger handlerInvocations = new AtomicInteger();
    IllegalStateException failure = new IllegalStateException("context unavailable");
    ConsumerDispatcher dispatcher = new ConsumerDispatcher(
        new EventHandlerRegistry(List.of(handler(handlerInvocations))),
        new RecordingDeserializer("deserialized-order"),
        List.of((event, chain) -> {
          throw failure;
        })
    );

    assertThatThrownBy(() -> dispatcher.dispatch(message(null))).isSameAs(failure);
    assertThat(handlerInvocations).hasValue(0);
  }

  @Test
  void propagatesInterceptorFailuresWithoutReplacingThemWithMissingProceedFailure() {
    AtomicInteger handlerInvocations = new AtomicInteger();
    EventRetryableException failure = new EventRetryableException("context unavailable");
    ConsumerDispatcher dispatcher = new ConsumerDispatcher(
        new EventHandlerRegistry(List.of(handler(handlerInvocations))),
        new RecordingDeserializer("deserialized-order"),
        List.of((event, chain) -> {
          throw failure;
        })
    );

    assertThatThrownBy(() -> dispatcher.dispatch(message(null))).isSameAs(failure);
    assertThat(handlerInvocations).hasValue(0);
  }

  @Test
  void failsExplicitlyWhenAnInterceptorDoesNotProceed() {
    AtomicInteger handlerInvocations = new AtomicInteger();
    ConsumerDispatcher dispatcher = new ConsumerDispatcher(
        new EventHandlerRegistry(List.of(handler(handlerInvocations))),
        new RecordingDeserializer("deserialized-order"),
        List.of((event, chain) -> {
        })
    );

    assertThatThrownBy(() -> dispatcher.dispatch(message(null)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("completed without invoking chain.proceed()");
    assertThat(handlerInvocations).hasValue(0);
  }

  @Test
  void invokesTheHandlerAtMostOnceWhenAnInterceptorProceedsTwice() {
    AtomicInteger handlerInvocations = new AtomicInteger();
    ConsumerDispatcher dispatcher = new ConsumerDispatcher(
        new EventHandlerRegistry(List.of(handler(handlerInvocations))),
        new RecordingDeserializer("deserialized-order"),
        List.of((event, chain) -> {
          chain.proceed();
          chain.proceed();
        })
    );

    assertThatThrownBy(() -> dispatcher.dispatch(message(null)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("must not be called more than once");
    assertThat(handlerInvocations).hasValue(1);
  }

  private static EventHandlerInterceptor around(
      String name,
      List<String> calls
  ) {
    return (event, chain) -> {
      calls.add(name + "-before");
      try {
        chain.proceed();
      } finally {
        calls.add(name + "-after");
      }
    };
  }

  private static EventHandler<String> handler(AtomicInteger invocations) {
    return new EventHandler<>() {
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
        invocations.incrementAndGet();
      }
    };
  }

  private static EventHandler<String> handlerThatThrows(
      List<String> calls,
      RuntimeException failure
  ) {
    return new EventHandler<>() {
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
        calls.add("handler");
        throw failure;
      }
    };
  }

  private static ConsumerMessage message(String correlationId) {
    return new ConsumerMessage(
        new EventId("event-1"),
        "order.created",
        Instant.parse("2026-08-15T00:00:00Z"),
        "orders",
        correlationId,
        new SerializedPayload(
            "{\"orderId\":42}",
            "application/json"
        )
    );
  }

  private static final class RecordingDeserializer implements EventDeserializer {
    private final String result;
    private SerializedPayload payload;
    private Class<?> payloadType;

    private RecordingDeserializer(String result) {
      this.result = result;
    }

    @Override
    public <T> T deserialize(
        SerializedPayload payload,
        Class<T> payloadType
    ) {
      this.payload = payload;
      this.payloadType = payloadType;
      return payloadType.cast(result);
    }
  }

  private static class RecordingHandler implements EventHandler<String> {
    private final AtomicInteger invocations = new AtomicInteger();
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
      invocations.incrementAndGet();
      received = event;
    }
  }
}
