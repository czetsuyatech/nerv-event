package com.czetsuyatech.nerv.event.persistence.service.impl;

import com.czetsuyatech.nerv.event.core.inbox.InboxEvent;
import com.czetsuyatech.nerv.event.core.inbox.InboxRegistration;
import com.czetsuyatech.nerv.event.core.inbox.InboxService;
import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.exception.EventStateTransitionException;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.persistence.application.mapper.InboxEventMapper;
import com.czetsuyatech.nerv.event.persistence.service.InboxRegistrationWriter;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.InboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.InboxEventRepository;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * JPA inbox port using short transactions and pessimistic row locks for claiming.
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class InboxServiceImpl implements InboxService {

  public static final int MAX_LAST_ERROR_LENGTH = 2048;
  @NonNull
  private final InboxEventRepository entityRepository;

  @NonNull
  private final InboxRegistrationWriter registrationWriter;

  @NonNull
  private final InboxEventMapper mapper;

  @Override
  public InboxRegistration register(InboxEvent event) {
    Objects.requireNonNull(
        event,
        "event must not be null"
    );
    try {
      registrationWriter.insert(mapper.toJpa(event));
      log.debug(
          "Inbound event registered eventId={} eventType={} status={} correlationId={}",
          event.eventId().value(),
          event.eventType(),
          event.status(),
          event.correlationId()
      );
      return new InboxRegistration(
          true,
          event
      );
    } catch (RuntimeException exception) {
      if (!isDuplicateKeyViolation(exception)) {
        throw exception;
      }
      InboxEvent existing = findExistingAfterDuplicate(
          event.eventId(),
          exception
      );
      log.debug(
          "Duplicate inbound event detected eventId={} eventType={} status={} correlationId={}",
          existing.eventId().value(),
          existing.eventType(),
          existing.status(),
          existing.correlationId()
      );
      return new InboxRegistration(
          false,
          existing
      );
    }
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
  public Optional<InboxEvent> find(EventId eventId) {
    return entityRepository.findById(eventIdValue(eventId)).map(mapper::toCore);
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public Optional<InboxEvent> claim(
      EventId eventId,
      Instant now,
      String owner,
      Duration leaseDuration
  ) {
    String eventIdValue = eventIdValue(eventId);
    validateClaimArguments(
        now,
        owner,
        leaseDuration
    );
    InboxEventEntity event = entityRepository.findByEventIdForUpdate(eventIdValue).orElse(null);
    if (event == null || !isClaimable(
        event,
        now,
        leaseDuration
    )) {
      if (event != null && event.getStatus() == InboxStatus.PROCESSING) {
        log.warn(
            "Active inbox lease prevents duplicate claim eventId={} owner={} processingBy={}",
            eventIdValue,
            owner,
            event.getProcessingBy()
        );
      }
      return Optional.empty();
    }
    boolean reclaimed = event.getStatus() == InboxStatus.PROCESSING;
    String previousOwner = event.getProcessingBy();
    claim(
        event,
        now,
        owner
    );
    entityRepository.saveAndFlush(event);
    if (reclaimed) {
      log.warn(
          "Expired inbox lease reclaimed eventId={} eventType={} previousOwner={} owner={}",
          event.getEventId(),
          event.getEventType(),
          previousOwner,
          owner
      );
    }
    log.debug(
        "Inbound event claimed for processing eventId={} eventType={} owner={} attemptCount={}",
        event.getEventId(),
        event.getEventType(),
        owner,
        event.getAttemptCount()
    );
    return Optional.of(mapper.toCore(event));
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public List<InboxEvent> claimPendingRetries(
      int limit,
      Instant now,
      String owner,
      Duration leaseDuration
  ) {
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be greater than zero");
    }
    validateClaimArguments(
        now,
        owner,
        leaseDuration
    );
    List<InboxEventEntity> events = entityRepository.findRetryPendingForUpdate(
        InboxStatus.RETRY_PENDING,
        now,
        PageRequest.of(
            0,
            limit
        )
    );
    events.forEach(
        event -> claim(
            event,
            now,
            owner
        )
    );
    entityRepository.saveAllAndFlush(events);
    List<InboxEvent> claimed = events.stream().map(mapper::toCore).toList();
    claimed.forEach(
        event -> log.debug(
            "Inbox pending retry claimed eventId={} eventType={} owner={} attemptCount={}",
            event.eventId().value(),
            event.eventType(),
            owner,
            event.attemptCount()
        )
    );
    return claimed;
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void markProcessed(
      EventId eventId,
      String owner,
      Instant processedAt
  ) {
    String eventIdValue = eventIdValue(eventId);
    requireNonBlank(
        owner,
        "owner"
    );
    Objects.requireNonNull(
        processedAt,
        "processedAt must not be null"
    );
    assertTransitioned(
        entityRepository.markProcessed(
            eventIdValue,
            owner,
            InboxStatus.PROCESSED,
            InboxStatus.PROCESSING,
            processedAt
        ),
        eventId,
        owner,
        InboxStatus.PROCESSING,
        InboxStatus.PROCESSED
    );
    log.debug(
        "Inbox event marked processed eventId={} owner={}",
        eventIdValue,
        owner
    );
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void markRetryPending(
      EventId eventId,
      String owner,
      int attemptCount,
      Instant failedAt,
      Instant availableAt,
      String error
  ) {
    String eventIdValue = eventIdValue(eventId);
    requireNonBlank(
        owner,
        "owner"
    );
    if (attemptCount < 0) {
      throw new IllegalArgumentException("attemptCount must not be negative");
    }
    Objects.requireNonNull(
        failedAt,
        "failedAt must not be null"
    );
    Objects.requireNonNull(
        availableAt,
        "availableAt must not be null"
    );
    assertTransitioned(
        entityRepository.markRetryPending(
            eventIdValue,
            owner,
            InboxStatus.RETRY_PENDING,
            InboxStatus.PROCESSING,
            attemptCount,
            failedAt,
            availableAt,
            truncateError(error)
        ),
        eventId,
        owner,
        InboxStatus.PROCESSING,
        InboxStatus.RETRY_PENDING
    );
    log.debug(
        "Inbox event scheduled for retry eventId={} eventType={} attemptCount={} availableAt={} owner={}",
        eventIdValue,
        eventType(eventIdValue),
        attemptCount,
        availableAt,
        owner
    );
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void markFailed(
      EventId eventId,
      String owner,
      int attemptCount,
      Instant failedAt,
      String error
  ) {
    String eventIdValue = eventIdValue(eventId);
    requireNonBlank(
        owner,
        "owner"
    );
    if (attemptCount < 0) {
      throw new IllegalArgumentException("attemptCount must not be negative");
    }
    Objects.requireNonNull(
        failedAt,
        "failedAt must not be null"
    );
    assertTransitioned(
        entityRepository.markFailed(
            eventIdValue,
            owner,
            InboxStatus.FAILED,
            InboxStatus.PROCESSING,
            attemptCount,
            failedAt,
            truncateError(error)
        ),
        eventId,
        owner,
        InboxStatus.PROCESSING,
        InboxStatus.FAILED
    );
    log.debug(
        "Inbox event marked failed eventId={} eventType={} attemptCount={} owner={}",
        eventIdValue,
        eventType(eventIdValue),
        attemptCount,
        owner
    );
  }

  private InboxEvent findExistingAfterDuplicate(
      EventId eventId,
      RuntimeException exception
  ) {
    return find(eventId).orElseThrow(
        () -> new IllegalStateException(
            "Inbox uniqueness violation did not yield an existing event: " + eventId.value(),
            exception
        )
    );
  }

  private static boolean isDuplicateKeyViolation(Throwable exception) {
    Throwable current = exception;
    while (current != null) {
      if (current instanceof DataIntegrityViolationException) {
        return true;
      }
      if (current instanceof SQLException sqlException
          && "23505".equals(sqlException.getSQLState())) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private static boolean isClaimable(
      InboxEventEntity event,
      Instant now,
      Duration leaseDuration
  ) {
    if (event.getStatus() == InboxStatus.RECEIVED) {
      return true;
    }
    return event.getStatus() == InboxStatus.PROCESSING
        && event.getProcessingAt() != null
        && !event.getProcessingAt().plus(leaseDuration).isAfter(now);
  }

  private static void claim(
      InboxEventEntity event,
      Instant now,
      String owner
  ) {
    event.setStatus(InboxStatus.PROCESSING);
    event.setProcessingAt(now);
    event.setProcessingBy(owner);
    event.setAvailableAt(null);
    event.setUpdatedAt(now);
  }

  private static void validateClaimArguments(
      Instant now,
      String owner,
      Duration leaseDuration
  ) {
    Objects.requireNonNull(
        now,
        "now must not be null"
    );
    requireNonBlank(
        owner,
        "owner"
    );
    Objects.requireNonNull(
        leaseDuration,
        "leaseDuration must not be null"
    );
    if (leaseDuration.isNegative() || leaseDuration.isZero()) {
      throw new IllegalArgumentException("leaseDuration must be greater than zero");
    }
  }

  private static String eventIdValue(EventId eventId) {
    return Objects.requireNonNull(
        eventId,
        "eventId must not be null"
    ).value();
  }

  private String eventType(String eventId) {
    return entityRepository.findById(eventId)
        .map(InboxEventEntity::getEventType)
        .orElse("unknown");
  }

  private static void assertTransitioned(
      int updatedRows,
      EventId eventId,
      String owner,
      InboxStatus expectedStatus,
      InboxStatus requestedStatus
  ) {
    if (updatedRows != 1) {
      log.warn(
          "Inbox state transition rejected due to stale state or wrong owner eventId={} owner={} expectedStatus={} requestedStatus={} updatedRows={}",
          eventId.value(),
          owner,
          expectedStatus,
          requestedStatus,
          updatedRows
      );
      throw new EventStateTransitionException(
          eventId,
          expectedStatus,
          requestedStatus,
          owner
      );
    }
  }

  private static String truncateError(String error) {
    Objects.requireNonNull(
        error,
        "error must not be null"
    );
    return error.length() <= MAX_LAST_ERROR_LENGTH
        ? error
        : error.substring(
            0,
            MAX_LAST_ERROR_LENGTH
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
}
