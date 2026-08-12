package com.czetsuyatech.nerv.event.observability;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import java.time.Instant;
import java.util.Optional;

/**
 * Read-only, efficient operational queries supplied by a persistence integration.
 */
public interface InboxOperationalMetrics {

  long count(InboxStatus status);

  Optional<Instant> oldestRetryPendingAt();
}
