package com.czetsuyatech.nerv.event.core.consumer;

import com.czetsuyatech.nerv.event.consumer.EventHandler;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolves the one application handler registered for each logical event type.
 */
@Slf4j
public final class EventHandlerRegistry {

  private final Map<String, EventHandler<?>> handlers;

  public EventHandlerRegistry(Collection<? extends EventHandler<?>> handlers) {
    Objects.requireNonNull(
        handlers,
        "handlers must not be null"
    );
    Map<String, EventHandler<?>> registeredHandlers = new LinkedHashMap<>();
    for (EventHandler<?> handler : handlers) {
      EventHandler<?> nonNullHandler = Objects.requireNonNull(
          handler,
          "handler must not be null"
      );
      String eventType = Objects.requireNonNull(
          nonNullHandler.eventType(),
          "handler eventType must not be null"
      );
      if (eventType.isBlank()) {
        throw new IllegalArgumentException("handler eventType must not be blank");
      }
      EventHandler<?> existing = registeredHandlers.putIfAbsent(
          eventType,
          nonNullHandler
      );
      if (existing != null) {
        throw new IllegalStateException(
            "Multiple EventHandlers registered for event type '" + eventType + "': "
                + existing.getClass().getName() + ", " + nonNullHandler.getClass().getName()
        );
      }
      log.trace(
          "Registered event handler eventType={} handler={}",
          eventType,
          nonNullHandler.getClass().getName()
      );
    }
    this.handlers = Map.copyOf(registeredHandlers);
  }

  public EventHandler<?> handlerFor(String eventType) {
    Objects.requireNonNull(
        eventType,
        "eventType must not be null"
    );
    EventHandler<?> handler = handlers.get(eventType);
    if (handler == null) {
      throw new EventHandlerNotFoundException(eventType);
    }
    log.trace(
        "Resolved event handler eventType={} handler={}",
        eventType,
        handler.getClass().getName()
    );
    return handler;
  }
}
