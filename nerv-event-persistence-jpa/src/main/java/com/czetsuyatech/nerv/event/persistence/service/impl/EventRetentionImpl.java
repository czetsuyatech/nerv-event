package com.czetsuyatech.nerv.event.persistence.service.impl;

import com.czetsuyatech.nerv.event.core.retention.EventRetention;
import com.czetsuyatech.nerv.event.core.retention.RetentionSidecarCleaner;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.TraceContextEntityRepository;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.InboxEventRepository;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.OutboxEventRepository;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * <p>
 * JPA retention adapter using bounded, state-guarded bulk deletes.
 * </p>
 *
 * <p>
 * The event deletes select identifiers only; they never materialize event payloads. The status and cutoff predicates
 * are repeated by each delete, so a concurrent worker can only delete a terminal successful row once. Every operation
 * uses its own short transaction.
 * </p>
 */
@Repository
@Slf4j
@RequiredArgsConstructor
public class EventRetentionImpl implements EventRetention, RetentionSidecarCleaner {

  private final OutboxEventRepository outboxRepository;
  private final InboxEventRepository inboxRepository;
  private final TraceContextEntityRepository traceContextRepository;

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public int deletePublishedBefore(
      Instant cutoff,
      int limit
  ) {
    validate(
        cutoff,
        limit
    );
    int deleted = outboxRepository.deletePublishedBefore(
        cutoff,
        limit
    );
    log.debug(
        "Deleted retained outbox rows cutoff={} limit={} deleted={}",
        cutoff,
        limit,
        deleted
    );
    return deleted;
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public int deleteProcessedBefore(
      Instant cutoff,
      int limit
  ) {
    validate(
        cutoff,
        limit
    );
    int deleted = inboxRepository.deleteProcessedBefore(
        cutoff,
        limit
    );
    log.debug(
        "Deleted retained inbox rows cutoff={} limit={} deleted={}",
        cutoff,
        limit,
        deleted
    );
    return deleted;
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public int deleteUnreferenced(int limit) {
    validateLimit(limit);
    List<String> candidateEventIds = traceContextRepository.findUnreferencedEventIds(
        PageRequest.of(
            0,
            limit
        )
    );
    if (candidateEventIds.isEmpty()) {
      return 0;
    }
    int deleted = traceContextRepository.deleteUnreferencedByEventIds(candidateEventIds);
    log.debug(
        "Deleted unreferenced retention sidecars limit={} deleted={}",
        limit,
        deleted
    );
    return deleted;
  }

  private static void validate(
      Instant cutoff,
      int limit
  ) {
    Objects.requireNonNull(
        cutoff,
        "cutoff must not be null"
    );
    validateLimit(limit);
  }

  private static void validateLimit(int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be greater than zero");
    }
  }
}
