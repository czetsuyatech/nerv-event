package com.czetsuyatech.nerv.event.core.outbox;

import lombok.Builder;

/**
 * Immutable outcome of one outbox dispatch batch.
 */
@Builder
public record DispatchResult(
    int claimed,
    int published,
    int retried,
    int failed,
    int unresolved
)
{

  public DispatchResult {
    if (claimed < 0 || published < 0 || retried < 0 || failed < 0 || unresolved < 0) {
      throw new IllegalArgumentException("dispatch result counters must not be negative");
    }
    if (claimed != (long) published + retried + failed + unresolved) {
      throw new IllegalArgumentException("claimed events must equal the sum of their outcomes");
    }
  }

  public static DispatchResult empty() {
    return new DispatchResult(
        0,
        0,
        0,
        0,
        0
    );
  }
}
