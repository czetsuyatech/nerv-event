package com.czetsuyatech.nerv.event.spring.serialization;

import com.czetsuyatech.nerv.event.core.serialization.EventDeserializer;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import java.util.Objects;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * JSON implementation of the generic event deserialization SPI.
 */
@RequiredArgsConstructor
public class JacksonEventDeserializer implements EventDeserializer {

  @NonNull
  private final ObjectMapper objectMapper;

  @Override
  public <T> T deserialize(
      SerializedPayload payload,
      Class<T> payloadType
  ) {
    Objects.requireNonNull(
        payload,
        "payload must not be null"
    );
    Objects.requireNonNull(
        payloadType,
        "payloadType must not be null"
    );
    try {
      return objectMapper.readValue(
          payload.value(),
          payloadType
      );
    } catch (JacksonException exception) {
      throw new IllegalArgumentException(
          "Unable to deserialize event payload",
          exception
      );
    }
  }
}
