package com.czetsuyatech.nerv.event.persistence.service.impl;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.observability.InboxOperationalMetrics;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.InboxEventRepository;
import java.time.Instant;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * JPA-only operational projections; it never materializes inbox payload entities.
 */
@Component
@RequiredArgsConstructor
public class InboxOperationalMetricsImpl implements InboxOperationalMetrics {

  private final InboxEventRepository repository;

  public long count(InboxStatus status) {
    return repository.countByStatus(status);
  }

  public Optional<Instant> oldestRetryPendingAt() {
    return Optional.ofNullable(repository.oldestFailedAtByStatus(InboxStatus.RETRY_PENDING));
  }
}
