package com.czetsuyatech.nerv.event.persistence.application.dto;

/**
 * Converts event payload objects to and from their durable outbox representation.
 */
public interface OutboxPayloadCodec {

  String serialize(Object payload);

  Object deserialize(String payload);
}
