package com.czetsuyatech.nerv.event.core.consumer;

import com.czetsuyatech.nerv.event.consumer.EventHandler;
import com.czetsuyatech.nerv.event.consumer.EventHandlerChain;
import com.czetsuyatech.nerv.event.consumer.EventHandlerInterceptor;
import com.czetsuyatech.nerv.event.core.serialization.EventDeserializer;
import com.czetsuyatech.nerv.event.model.EventMessage;
import java.util.List;
import java.util.Objects;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Dispatches a broker-neutral incoming event to its one application handler.
 */
@Slf4j
@RequiredArgsConstructor
public class ConsumerDispatcher {

  @NonNull
  private final EventHandlerRegistry eventHandlerRegistry;

  @NonNull
  private final EventDeserializer eventDeserializer;

  @NonNull
  private final List<EventHandlerInterceptor> interceptors;

  public void dispatch(ConsumerMessage message) {
    Objects.requireNonNull(
        message,
        "message must not be null"
    );
    EventHandler<?> handler = eventHandlerRegistry.handlerFor(message.eventType());
    log.debug(
        "Dispatching consumed event eventId={} eventType={} handler={} correlationId={}",
        message.eventId().value(),
        message.eventType(),
        handler.getClass().getName(),
        message.correlationId()
    );
    dispatchToHandler(
        castHandler(handler),
        message
    );
    log.debug(
        "Consumed event handled eventId={} eventType={} handler={}",
        message.eventId().value(),
        message.eventType(),
        handler.getClass().getName()
    );
  }

  private <T> void dispatchToHandler(
      EventHandler<T> handler,
      ConsumerMessage message
  ) {
    Class<T> payloadType = Objects.requireNonNull(
        handler.payloadType(),
        "handler payloadType must not be null"
    );
    T payload = deserialize(
        message,
        payloadType
    );
    EventMessage<T> event = new EventMessage<>(
        message.eventId(),
        message.eventType(),
        message.timestamp(),
        message.source(),
        message.correlationId(),
        payload
    );
    new InterceptorChain(
        event,
        interceptors,
        () -> handler.handle(event),
        0
    ).proceed();
  }

  private <T> T deserialize(
      ConsumerMessage message,
      Class<T> payloadType
  ) {
    try {
      return eventDeserializer.deserialize(
          message.payload(),
          payloadType
      );
    } catch (RuntimeException exception) {
      throw new EventDeserializationException(
          message.eventId(),
          message.eventType(),
          payloadType,
          message.payload().contentType(),
          exception
      );
    }
  }

  /**
   * <p>
   * The handler's explicit payload type is used immediately for deserialization before this same handler receives the
   * reconstructed EventMessage, making this single cast safe.
   * </p>
   */
  @SuppressWarnings("unchecked")
  private static <T> EventHandler<T> castHandler(EventHandler<?> handler) {
    return (EventHandler<T>) handler;
  }

  private static final class InterceptorChain implements EventHandlerChain {
    private final EventMessage<?> event;
    private final List<EventHandlerInterceptor> interceptors;
    private final Runnable handler;
    private final int interceptorIndex;
    private boolean proceeded;

    private InterceptorChain(
        EventMessage<?> event,
        List<EventHandlerInterceptor> interceptors,
        Runnable handler,
        int interceptorIndex
    )
    {
      this.event = event;
      this.interceptors = interceptors;
      this.handler = handler;
      this.interceptorIndex = interceptorIndex;
    }

    @Override
    public void proceed() {
      if (proceeded) {
        throw new IllegalStateException("EventHandlerChain.proceed() must not be called more than once");
      }
      proceeded = true;
      if (interceptorIndex == interceptors.size()) {
        handler.run();
        return;
      }
      InterceptorChain next = new InterceptorChain(
          event,
          interceptors,
          handler,
          interceptorIndex + 1
      );
      EventHandlerInterceptor interceptor = interceptors.get(interceptorIndex);
      interceptor
          .intercept(
              event,
              next
          );
      if (!next.proceeded) {
        throw new IllegalStateException(
            "EventHandlerInterceptor completed without invoking chain.proceed(): "
                + interceptor.getClass().getName()
        );
      }
    }
  }
}
