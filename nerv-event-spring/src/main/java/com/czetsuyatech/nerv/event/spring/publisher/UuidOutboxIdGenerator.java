package com.czetsuyatech.nerv.event.spring.publisher;

import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxIdGenerator;
import java.util.UUID;

/**
 * Default Spring-wired generator for outbox record identifiers.
 */
public final class UuidOutboxIdGenerator implements OutboxIdGenerator {

  @Override
  public OutboxId nextId() {
    return new OutboxId(UUID.randomUUID().toString());
  }
}
