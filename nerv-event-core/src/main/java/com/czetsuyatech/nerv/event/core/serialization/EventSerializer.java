package com.czetsuyatech.nerv.event.core.serialization;

import com.czetsuyatech.nerv.event.model.EventMessage;

/**
 * Serializes a complete event envelope for broker delivery.
 */
public interface EventSerializer {

  SerializedPayload serialize(EventMessage<?> event);
}
