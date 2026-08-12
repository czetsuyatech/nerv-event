package com.czetsuyatech.nerv.event.spring.retention;

/**
 * Immutable summary of one bounded retention cycle.
 */
public record RetentionResult(
    int outboxDeleted,
    int inboxDeleted,
    int traceContextsDeleted
)
{

  public RetentionResult {
    if (outboxDeleted < 0 || inboxDeleted < 0 || traceContextsDeleted < 0) {
      throw new IllegalArgumentException("retention deletion counts must not be negative");
    }
  }

  public int totalDeleted() {
    return outboxDeleted + inboxDeleted + traceContextsDeleted;
  }

  public static RetentionResult empty() {
    return new RetentionResult(
        0,
        0,
        0
    );
  }
}
