package com.czetsuyatech.nerv.event.core.outbox;

/**
 * Generates identifiers for durable outbox records.
 */
@FunctionalInterface
public interface OutboxIdGenerator {

  OutboxId nextId();
}
