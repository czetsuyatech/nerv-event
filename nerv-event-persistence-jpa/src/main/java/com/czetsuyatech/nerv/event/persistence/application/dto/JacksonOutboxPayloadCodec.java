package com.czetsuyatech.nerv.event.persistence.application.dto;

import java.util.Objects;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * JSON implementation for the text representation stored by the JPA outbox adapter.
 */
@RequiredArgsConstructor
public final class JacksonOutboxPayloadCodec implements OutboxPayloadCodec {

  @NonNull
  private final ObjectMapper objectMapper;

  @Override
  public String serialize(Object payload) {
    Objects.requireNonNull(
        payload,
        "payload must not be null"
    );
    try {
      return objectMapper.writeValueAsString(payload);
    } catch (JacksonException exception) {
      throw new IllegalArgumentException(
          "Unable to serialize outbox payload as JSON",
          exception
      );
    }
  }

  @Override
  public Object deserialize(String payload) {
    Objects.requireNonNull(
        payload,
        "payload must not be null"
    );
    try {
      return objectMapper.readValue(
          payload,
          Object.class
      );
    } catch (JacksonException exception) {
      throw new IllegalArgumentException(
          "Unable to deserialize outbox payload JSON",
          exception
      );
    }
  }
}
