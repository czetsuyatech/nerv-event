package com.czetsuyatech.nerv.event.persistence.service.impl;

import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.observability.OutboxOperationalMetrics;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.OutboxEventRepository;
import java.time.Instant;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * JPA-only operational projections; it never materializes outbox payload entities.
 */
@Component
@RequiredArgsConstructor
public class OutboxOperationalMetricsImpl implements OutboxOperationalMetrics {

  private final OutboxEventRepository repository;

  public long count(OutboxStatus status) {
    return repository.countByStatus(status);
  }

  public Optional<Instant> oldestPendingAt() {
    return Optional.ofNullable(repository.oldestCreatedAtByStatus(OutboxStatus.PENDING));
  }
}
