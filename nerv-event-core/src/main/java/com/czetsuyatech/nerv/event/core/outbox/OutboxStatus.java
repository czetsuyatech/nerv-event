package com.czetsuyatech.nerv.event.core.outbox;

/**
 * Lifecycle states for an outbox event.
 */
public enum OutboxStatus {
  PENDING, PROCESSING, PUBLISHED, FAILED
}
