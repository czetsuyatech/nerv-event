package com.czetsuyatech.nerv.event.consumer;

import com.czetsuyatech.nerv.event.model.EventMessage;

/**
 * <p>
 * Handles one broker-neutral event payload type.
 * </p>
 * <p>
 * NERV selects handlers by {@link #eventType()} and deserializes the payload using {@link #payloadType()}. Broker
 * adapters register durable Inbox state before calling this method. Implementations should make external side effects
 * idempotent because broker redelivery remains possible under at-least-once delivery semantics.
 * </p>
 *
 * @param <T>
 *          payload type
 */
public interface EventHandler<T> {

  String eventType();

  Class<T> payloadType();

  void handle(EventMessage<T> event);
}
