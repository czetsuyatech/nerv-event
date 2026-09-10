package com.czetsuyatech.nerv.event.persistence.service.impl;

import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import com.czetsuyatech.nerv.event.core.outbox.OutboxEvent;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxService;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.application.mapper.OutboxEventMapper;
import com.czetsuyatech.nerv.event.persistence.application.dto.OutboxPayloadCodec;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.OutboxEventRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * <p>
 * Spring-managed JPA implementation of the core outbox persistence port.
 * </p>
 *
 * <p>
 * This class must be invoked through its Spring proxy. {@link #save(OutboxEvent)} participates in the caller's
 * transaction, while claim and state-transition methods execute in independent, short transactions.
 * </p>
 */
@Slf4j
public class OutboxServiceImpl implements OutboxService {

  public static final int MAX_LAST_ERROR_LENGTH = 2048;
  private final OutboxEventRepository entityRepository;
  private final OutboxEventMapper mapper;
  private final OutboxPayloadCodec payloadCodec;
  private final OutboxClaimStrategy claimStrategy;
  private final String owner;
  private final Duration leaseDuration;
  private final Clock clock;

  public OutboxServiceImpl(
      OutboxEventRepository entityRepository,
      OutboxEventMapper mapper,
      OutboxPayloadCodec payloadCodec,
      OutboxClaimStrategy claimStrategy,
      String owner,
      Duration leaseDuration,
      Clock clock
  )
  {
    this.entityRepository = Objects.requireNonNull(
        entityRepository,
        "entityRepository must not be null"
    );
    this.mapper = Objects.requireNonNull(
        mapper,
        "mapper must not be null"
    );
    this.payloadCodec = Objects.requireNonNull(
        payloadCodec,
        "payloadCodec must not be null"
    );
    this.claimStrategy = Objects.requireNonNull(
        claimStrategy,
        "claimStrategy must not be null"
    );
    this.owner = requireNonBlank(
        owner,
        "owner"
    );
    this.leaseDuration = Objects.requireNonNull(
        leaseDuration,
        "leaseDuration must not be null"
    );
    if (leaseDuration.isNegative() || leaseDuration.isZero()) {
      throw new IllegalArgumentException("leaseDuration must be greater than zero");
    }
    this.clock = Objects.requireNonNull(
        clock,
        "clock must not be null"
    );
  }

  @Override
  @Transactional(propagation = Propagation.MANDATORY)
  public OutboxEvent save(OutboxEvent event) {
    Objects.requireNonNull(
        event,
        "event must not be null"
    );
    OutboxEventEntity entity = mapper.toJpa(event);
    try {
      entity.setPayload(payloadCodec.serialize(event.event().payload()));
    } catch (RuntimeException exception) {
      log.error(
          "Unable to serialize outbox event payload outboxId={} eventId={} eventType={} destination={} correlationId={} errorType={}",
          event.id().value(),
          event.event().id().value(),
          event.event().type(),
          event.destination().name(),
          event.event().correlationId(),
          exception.getClass().getSimpleName()
      );
      throw exception;
    }
    Instant persistedAt = clock.instant();
    entity.setCreatedAt(persistedAt);
    entity.setUpdatedAt(persistedAt);
    entityRepository.save(entity);
    log.debug(
        "Outbox event added to current transaction outboxId={} eventId={} eventType={} destination={} correlationId={} attemptCount={}",
        event.id().value(),
        event.event().id().value(),
        event.event().type(),
        event.destination().name(),
        event.event().correlationId(),
        event.attemptCount()
    );
    return event;
  }

  /**
   * <p>
   * Claims eligible pending rows and expired processing leases with a JPA pessimistic write lock. The locked rows are
   * moved to PROCESSING before the short transaction commits.
   * </p>
   */
  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public List<OutboxEvent> claimPending(
      Instant eligibleAt,
      int batchSize
  ) {
    Objects.requireNonNull(
        eligibleAt,
        "eligibleAt must not be null"
    );
    if (batchSize <= 0) {
      throw new IllegalArgumentException("batchSize must be greater than zero");
    }
    log.debug(
        "Starting outbox claim owner={} eligibleAt={} batchSize={}",
        owner,
        eligibleAt,
        batchSize
    );
    List<OutboxEvent> claimedEvents = claimStrategy.claim(
        eligibleAt,
        batchSize,
        owner,
        leaseDuration
    ).stream().map(this::toCore).toList();
    log.debug(
        "Completed outbox claim owner={} claimed={}",
        owner,
        claimedEvents.size()
    );
    return claimedEvents;
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public boolean markPublished(
      OutboxId id,
      long claimVersion,
      BrokerPublishResult result
  ) {
    Objects.requireNonNull(
        result,
        "result must not be null"
    );
    Instant transitionAt = clock.instant();
    boolean transitioned = entityRepository.markPublished(
        outboxIdValue(id),
        owner,
        claimVersion,
        OutboxStatus.PUBLISHED,
        OutboxStatus.PROCESSING,
        transitionAt,
        transitionAt
    ) == 1;
    if (transitioned) {
      log.debug(
          "Outbox event marked published outboxId={} owner={} claimVersion={}",
          id.value(),
          owner,
          claimVersion
      );
    }
    return transitioned;
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public boolean reschedule(
      OutboxId id,
      long claimVersion,
      int attemptCount,
      Instant nextAttemptAt,
      String failureReason
  ) {
    validateAttemptCount(attemptCount);
    Objects.requireNonNull(
        nextAttemptAt,
        "nextAttemptAt must not be null"
    );
    boolean transitioned = entityRepository.reschedule(
        outboxIdValue(id),
        owner,
        claimVersion,
        OutboxStatus.PENDING,
        OutboxStatus.PROCESSING,
        attemptCount,
        nextAttemptAt,
        clock.instant(),
        truncateFailureReason(failureReason)
    ) == 1;
    if (transitioned) {
      log.debug(
          "Outbox event rescheduled outboxId={} owner={} claimVersion={} attemptCount={} nextAttemptAt={}",
          id.value(),
          owner,
          claimVersion,
          attemptCount,
          nextAttemptAt
      );
    }
    return transitioned;
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public boolean markFailed(
      OutboxId id,
      long claimVersion,
      int attemptCount,
      String failureReason
  ) {
    validateAttemptCount(attemptCount);
    boolean transitioned = entityRepository.markFailed(
        outboxIdValue(id),
        owner,
        claimVersion,
        OutboxStatus.FAILED,
        OutboxStatus.PROCESSING,
        attemptCount,
        clock.instant(),
        truncateFailureReason(failureReason)
    ) == 1;
    if (transitioned) {
      log.debug(
          "Outbox event marked failed outboxId={} owner={} claimVersion={} attemptCount={}",
          id.value(),
          owner,
          claimVersion,
          attemptCount
      );
    }
    return transitioned;
  }

  private static String outboxIdValue(OutboxId id) {
    Objects.requireNonNull(
        id,
        "id must not be null"
    );
    return id.value();
  }

  private OutboxEvent toCore(OutboxEventEntity entity) {
    Object payload;
    try {
      payload = payloadCodec.deserialize(entity.getPayload());
    } catch (RuntimeException exception) {
      log.error(
          "Unable to deserialize persisted outbox event payload outboxId={} eventId={} eventType={} destination={} correlationId={} errorType={}",
          entity.getId(),
          entity.getEventId(),
          entity.getEventType(),
          entity.getDestination(),
          entity.getCorrelationId(),
          exception.getClass().getSimpleName()
      );
      throw exception;
    }
    return mapper.toCore(
        entity,
        mapper.toEventMessage(
            entity,
            payload
        )
    );
  }

  private static String requireNonBlank(
      String value,
      String name
  ) {
    Objects.requireNonNull(
        value,
        name + " must not be null"
    );
    if (value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be blank");
    }
    return value;
  }

  private static void validateAttemptCount(int attemptCount) {
    if (attemptCount < 0) {
      throw new IllegalArgumentException("attemptCount must not be negative");
    }
  }

  private static String truncateFailureReason(String failureReason) {
    Objects.requireNonNull(
        failureReason,
        "failureReason must not be null"
    );
    return failureReason.length() <= MAX_LAST_ERROR_LENGTH
        ? failureReason
        : failureReason.substring(
            0,
            MAX_LAST_ERROR_LENGTH
        );
  }
}
