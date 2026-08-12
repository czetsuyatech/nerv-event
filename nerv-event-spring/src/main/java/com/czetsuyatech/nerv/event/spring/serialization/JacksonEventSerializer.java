package com.czetsuyatech.nerv.event.spring.serialization;

import com.czetsuyatech.nerv.event.core.serialization.EventSerializer;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventMessage;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * JSON implementation of the generic event serialization SPI.
 */
@RequiredArgsConstructor
public class JacksonEventSerializer implements EventSerializer {

  @NonNull
  private final ObjectMapper objectMapper;

  @Override
  public SerializedPayload serialize(EventMessage<?> event) {
    try {
      return new SerializedPayload(
          objectMapper.writeValueAsString(event.payload()),
          "application/json"
      );
    } catch (JacksonException exception) {
      throw new IllegalArgumentException(
          "Unable to serialize event payload",
          exception
      );
    }
  }
}
