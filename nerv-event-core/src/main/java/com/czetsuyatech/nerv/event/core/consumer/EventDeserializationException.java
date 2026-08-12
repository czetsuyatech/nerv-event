package com.czetsuyatech.nerv.event.core.consumer;

import com.czetsuyatech.nerv.event.exception.NervEventException;
import com.czetsuyatech.nerv.event.model.EventId;

/**
 * Identifies an encoded event that could not be reconstructed for its handler.
 */
public class EventDeserializationException extends NervEventException {

  public EventDeserializationException(
      EventId eventId,
      String eventType,
      Class<?> payloadType,
      String contentType,
      Throwable cause
  )
  {
    super(
        "Unable to deserialize event eventId=" + eventId.value()
            + " eventType=" + eventType
            + " payloadType=" + payloadType.getName()
            + " contentType=" + contentType,
        cause
    );
  }
}
