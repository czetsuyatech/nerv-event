package com.czetsuyatech.nerv.event.core.retention;

import java.time.Instant;

/**
 * <p>
 * Persistence port for bounded removal of terminal, successfully handled durable events.
 * </p>
 *
 * <p>
 * Implementations must keep the success-state predicate in the delete itself. In particular, this port must never be
 * implemented as an age-only delete.
 * </p>
 */
public interface EventRetention {

  /**
   * Deletes at most {@code limit} outbox rows that are PUBLISHED at or before {@code cutoff}.
   */
  int deletePublishedBefore(
      Instant cutoff,
      int limit
  );

  /**
   * Deletes at most {@code limit} inbox rows that are PROCESSED at or before {@code cutoff}.
   */
  int deleteProcessedBefore(
      Instant cutoff,
      int limit
  );
}
