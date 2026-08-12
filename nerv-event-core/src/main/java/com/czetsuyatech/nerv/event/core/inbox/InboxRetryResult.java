package com.czetsuyatech.nerv.event.core.inbox;

/**
 * Immutable outcome of one inbox retry dispatch cycle.
 */
public record InboxRetryResult(
    int claimed,
    int processed,
    int retryPending,
    int failed,
    int unresolved
)
{
  public InboxRetryResult {
    requireNonNegative(
        claimed,
        "claimed"
    );
    requireNonNegative(
        processed,
        "processed"
    );
    requireNonNegative(
        retryPending,
        "retryPending"
    );
    requireNonNegative(
        failed,
        "failed"
    );
    requireNonNegative(
        unresolved,
        "unresolved"
    );
    if (claimed != processed + retryPending + failed + unresolved) {
      throw new IllegalArgumentException(
          "claimed must equal processed + retryPending + failed + unresolved"
      );
    }
  }

  public static InboxRetryResult empty() {
    return new InboxRetryResult(
        0,
        0,
        0,
        0,
        0
    );
  }

  private static void requireNonNegative(
      int value,
      String fieldName
  ) {
    if (value < 0) {
      throw new IllegalArgumentException(fieldName + " must not be negative");
    }
  }
}
