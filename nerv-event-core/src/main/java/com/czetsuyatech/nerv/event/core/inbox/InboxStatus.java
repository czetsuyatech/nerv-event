package com.czetsuyatech.nerv.event.core.inbox;

/**
 * Durable inbound processing state.
 */
public enum InboxStatus {
  /**
   * Event has been received durably but has not yet been processed.
   */
  RECEIVED,

  /**
   * Event is currently owned by a processor.
   */
  PROCESSING,

  /**
   * A failed handler attempt is scheduled for another automatic attempt.
   */
  RETRY_PENDING,

  /**
   * Event handling completed successfully.
   */
  PROCESSED,

  /**
   * Automatic handler attempts are exhausted.
   */
  FAILED
}
