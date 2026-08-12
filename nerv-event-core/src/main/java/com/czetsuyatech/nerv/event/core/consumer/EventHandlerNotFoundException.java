package com.czetsuyatech.nerv.event.core.consumer;

import com.czetsuyatech.nerv.event.exception.NervEventException;

/**
 * Raised when no application handler owns an incoming event type.
 */
public class EventHandlerNotFoundException extends NervEventException {

  public EventHandlerNotFoundException(String eventType) {
    super("No EventHandler registered for event type '" + eventType + "'");
  }
}
