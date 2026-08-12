package com.czetsuyatech.nerv.event.core.serialization;

/**
 * Reconstructs an event payload from its encoded representation.
 */
public interface EventDeserializer {

  <T> T deserialize(
      SerializedPayload payload,
      Class<T> payloadType
  );
}
