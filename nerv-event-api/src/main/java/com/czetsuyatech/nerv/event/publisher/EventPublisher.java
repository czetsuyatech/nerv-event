package com.czetsuyatech.nerv.event.publisher;

import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventPublication;

/**
 * <p>
 * Persists an event publication for asynchronous broker delivery.
 * </p>
 * <p>
 * Applications normally invoke this interface inside the same transaction as their business update. The implementation
 * records an Outbox entry; a separate dispatcher contacts the broker later, so a successful return does not mean the
 * event has already been delivered.
 * </p>
 */
public interface EventPublisher {

  /**
   * Records the supplied event and logical destination in the Outbox.
   *
   * @return the logical event identifier retained for tracing and operational lookup
   */
  <T> EventId publish(EventPublication<T> publication);
}
