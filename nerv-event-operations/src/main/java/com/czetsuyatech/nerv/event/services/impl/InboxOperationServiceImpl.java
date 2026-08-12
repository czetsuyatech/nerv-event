package com.czetsuyatech.nerv.event.services.impl;

import com.czetsuyatech.nerv.event.application.dto.InboxEventView;
import com.czetsuyatech.nerv.event.application.dto.InboxQuery;
import com.czetsuyatech.nerv.event.application.dto.InboxSearchResult;
import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.exception.ManualRetryRejectedException;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.InboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.repository.InboxSearchProjection;
import com.czetsuyatech.nerv.event.persistence.repository.InboxOperationRepository;
import com.czetsuyatech.nerv.event.persistence.repository.OperationsMetrics;
import com.czetsuyatech.nerv.event.persistence.specification.InboxSpecifications;
import com.czetsuyatech.nerv.event.services.InboxOperationService;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * JPA implementation kept behind the public operations API.
 */
@Slf4j
@RequiredArgsConstructor
public class InboxOperationServiceImpl implements InboxOperationService {

  @NonNull
  private final InboxOperationRepository repository;

  @NonNull
  private final Clock clock;

  @NonNull
  private final OperationsMetrics metrics;

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
  public Optional<InboxEventView> find(EventId eventId) {
    Objects.requireNonNull(
        eventId,
        "eventId must not be null"
    );
    return repository.findById(eventId.value())
        .map(
            entity -> toView(
                entity,
                true
            )
        );
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
  public InboxSearchResult search(InboxQuery query) {
    Objects.requireNonNull(
        query,
        "query must not be null"
    );
    Page<InboxSearchProjection> page = repository.findBy(
        InboxSpecifications.from(query),
        result -> result
            .as(InboxSearchProjection.class)
            .page(
                PageRequest.of(
                    query.pageNumber(),
                    query.pageSize(),
                    Sort.by(
                        Sort.Order.desc("updatedAt"),
                        Sort.Order.desc("eventId")
                    )
                )
            )
    );
    return new InboxSearchResult(
        page.getContent().stream().map(InboxOperationServiceImpl::toView).toList(),
        page.getNumber(),
        page.getSize(),
        page.getTotalElements(),
        page.getTotalPages()
    );
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public InboxEventView retryFailed(EventId eventId) {
    Objects.requireNonNull(
        eventId,
        "eventId must not be null"
    );
    Instant now = clock.instant();
    try {
      int changed = repository.retryFailed(
          eventId.value(),
          InboxStatus.FAILED,
          InboxStatus.RETRY_PENDING,
          now,
          now
      );
      if (changed != 1) {
        reject(eventId);
      }
      InboxEventEntity event = repository.findById(eventId.value())
          .orElseThrow(() -> new IllegalStateException("Inbox row disappeared after manual retry: " + eventId.value()));
      InboxEventView view = toView(
          event,
          true
      );
      metrics.manualRetry(
          "inbox",
          "success"
      );
      log.info(
          "Inbox manual retry requested eventId={} previousStatus={} attemptCount={}",
          view.eventId().value(),
          InboxStatus.FAILED,
          view.attemptCount()
      );
      return view;
    } catch (ManualRetryRejectedException exception) {
      throw exception;
    } catch (RuntimeException exception) {
      metrics.manualRetry(
          "inbox",
          "failure"
      );
      log.error(
          "Unable to perform inbox manual retry eventId={} errorType={}",
          eventId.value(),
          exception.getClass().getSimpleName(),
          exception
      );
      throw exception;
    }
  }

  private void reject(EventId eventId) {
    String detail = repository.findById(eventId.value())
        .map(event -> "current status is " + event.getStatus() + "; only FAILED is retryable")
        .orElse("record was not found");
    metrics.manualRetry(
        "inbox",
        "rejected"
    );
    log.warn(
        "Inbox manual retry rejected eventId={} {}",
        eventId.value(),
        detail
    );
    throw new ManualRetryRejectedException(
        "inbox",
        eventId.value(),
        detail
    );
  }

  private static InboxEventView toView(
      InboxEventEntity event,
      boolean includePayload
  ) {
    return new InboxEventView(
        new EventId(event.getEventId()),
        event.getEventType(),
        event.getEventTimestamp(),
        event.getSource(),
        event.getCorrelationId(),
        event.getStatus(),
        event.getAttemptCount(),
        event.getReceivedAt(),
        event.getProcessingAt(),
        event.getProcessingBy(),
        event.getProcessedAt(),
        event.getFailedAt(),
        event.getAvailableAt(),
        event.getLastError(),
        event.getCreatedAt(),
        event.getUpdatedAt(),
        event.getContentType(),
        includePayload ? event.getPayload() : null
    );
  }

  private static InboxEventView toView(InboxSearchProjection event) {
    return new InboxEventView(
        new EventId(event.getEventId()),
        event.getEventType(),
        event.getEventTimestamp(),
        event.getSource(),
        event.getCorrelationId(),
        event.getStatus(),
        event.getAttemptCount(),
        event.getReceivedAt(),
        event.getProcessingAt(),
        event.getProcessingBy(),
        event.getProcessedAt(),
        event.getFailedAt(),
        event.getAvailableAt(),
        event.getLastError(),
        event.getCreatedAt(),
        event.getUpdatedAt(),
        event.getContentType(),
        null
    );
  }
}
