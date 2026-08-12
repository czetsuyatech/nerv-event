package com.czetsuyatech.nerv.event.observability;

import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import java.time.Instant;
import java.util.Optional;

/**
 * Read-only, efficient operational queries supplied by a persistence integration.
 */
public interface OutboxOperationalMetrics {

  long count(OutboxStatus status);

  Optional<Instant> oldestPendingAt();
}
