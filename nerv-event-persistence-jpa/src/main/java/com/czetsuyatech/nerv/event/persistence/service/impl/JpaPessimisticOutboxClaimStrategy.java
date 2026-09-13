package com.czetsuyatech.nerv.event.persistence.service.impl;

import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.OutboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;

/**
 * <p>
 * Uses a pessimistic database row lock to atomically lease eligible outbox rows.
 * </p>
 *
 * <p>
 * This is intentionally separate from ordinary CRUD operations because claiming requires database locking semantics
 * that must remain atomic across application instances.
 * </p>
 */
@Slf4j
@RequiredArgsConstructor
public final class JpaPessimisticOutboxClaimStrategy implements OutboxClaimStrategy {

  @NonNull
  private final OutboxEventRepository entityRepository;

  @Override
  public List<OutboxEventEntity> claim(
      Instant claimedAt,
      int batchSize,
      String owner,
      Duration leaseDuration
  ) {
    Objects.requireNonNull(
        claimedAt,
        "claimedAt must not be null"
    );
    Objects.requireNonNull(
        owner,
        "owner must not be null"
    );
    Objects.requireNonNull(
        leaseDuration,
        "leaseDuration must not be null"
    );
    Instant expiredLeaseAt = claimedAt.minus(leaseDuration);
    log.trace(
        "Evaluating outbox claim owner={} claimedAt={} expiredLeaseAt={} batchSize={}",
        owner,
        claimedAt,
        expiredLeaseAt,
        batchSize
    );
    List<OutboxEventEntity> claimedEvents = entityRepository.findClaimableForUpdate(
        OutboxStatus.PENDING,
        OutboxStatus.PROCESSING,
        claimedAt,
        expiredLeaseAt,
        List.of(OutboxStatus.PENDING, OutboxStatus.PROCESSING),
        PageRequest.of(
            0,
            batchSize
        )
    );
    for (OutboxEventEntity claimedEvent : claimedEvents) {
      if (claimedEvent.getStatus() == OutboxStatus.PROCESSING) {
        log.warn(
            "Reclaiming expired outbox lease outboxId={} eventId={} previousOwner={} lockedAt={}",
            claimedEvent.getId(),
            claimedEvent.getEventId(),
            claimedEvent.getLockedBy(),
            claimedEvent.getLockedAt()
        );
      }
      claimedEvent.setStatus(OutboxStatus.PROCESSING);
      claimedEvent.setLockedAt(claimedAt);
      claimedEvent.setLockedBy(owner);
      claimedEvent.setClaimVersion(Math.incrementExact(claimedEvent.getClaimVersion()));
      claimedEvent.setUpdatedAt(claimedAt);
      log.debug(
          "Claimed outbox event outboxId={} eventId={} eventType={} destination={} correlationId={} owner={} claimVersion={} attemptCount={}",
          claimedEvent.getId(),
          claimedEvent.getEventId(),
          claimedEvent.getEventType(),
          claimedEvent.getDestination(),
          claimedEvent.getCorrelationId(),
          owner,
          claimedEvent.getClaimVersion(),
          claimedEvent.getAttemptCount()
      );
    }
    entityRepository.saveAllAndFlush(claimedEvents);
    return claimedEvents;
  }
}
