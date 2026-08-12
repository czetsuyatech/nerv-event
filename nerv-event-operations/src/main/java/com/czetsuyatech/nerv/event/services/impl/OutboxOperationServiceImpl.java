package com.czetsuyatech.nerv.event.services.impl;

import com.czetsuyatech.nerv.event.application.dto.OutboxEventView;
import com.czetsuyatech.nerv.event.application.dto.OutboxQuery;
import com.czetsuyatech.nerv.event.application.dto.OutboxSearchResult;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.exception.ManualRetryRejectedException;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import com.czetsuyatech.nerv.event.persistence.repository.OutboxOperationRepository;
import com.czetsuyatech.nerv.event.persistence.repository.OperationsMetrics;
import com.czetsuyatech.nerv.event.persistence.repository.OutboxSearchProjection;
import com.czetsuyatech.nerv.event.persistence.specification.OutboxSpecifications;
import com.czetsuyatech.nerv.event.services.OutboxOperationService;
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
@RequiredArgsConstructor
@Slf4j
public class OutboxOperationServiceImpl implements OutboxOperationService {

  @NonNull
  private final OutboxOperationRepository repository;

  @NonNull
  private final Clock clock;

  @NonNull
  private final OperationsMetrics metrics;

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
  public Optional<OutboxEventView> find(OutboxId outboxId) {
    Objects.requireNonNull(
        outboxId,
        "outboxId must not be null"
    );
    return repository.findById(outboxId.value())
        .map(
            entity -> toView(
                entity,
                true
            )
        );
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
  public OutboxSearchResult search(OutboxQuery query) {
    Objects.requireNonNull(
        query,
        "query must not be null"
    );
    Page<OutboxSearchProjection> page = repository.findBy(
        OutboxSpecifications.from(query),
        result -> result
            .as(OutboxSearchProjection.class)
            .page(
                PageRequest.of(
                    query.pageNumber(),
                    query.pageSize(),
                    Sort.by(
                        Sort.Order.desc("updatedAt"),
                        Sort.Order.desc("id")
                    )
                )
            )
    );
    return new OutboxSearchResult(
        page.getContent().stream().map(OutboxOperationServiceImpl::toView).toList(),
        page.getNumber(),
        page.getSize(),
        page.getTotalElements(),
        page.getTotalPages()
    );
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public OutboxEventView retryFailed(OutboxId outboxId) {
    Objects.requireNonNull(
        outboxId,
        "outboxId must not be null"
    );
    Instant now = clock.instant();
    try {
      int changed = repository.retryFailed(
          outboxId.value(),
          OutboxStatus.FAILED,
          OutboxStatus.PENDING,
          now,
          now
      );
      if (changed != 1) {
        reject(outboxId);
      }
      OutboxEventEntity event = repository.findById(outboxId.value())
          .orElseThrow(
              () -> new IllegalStateException("Outbox row disappeared after manual retry: " + outboxId.value())
          );
      OutboxEventView view = toView(
          event,
          true
      );
      metrics.manualRetry(
          "outbox",
          "success"
      );
      log.info(
          "Outbox manual retry requested outboxId={} eventId={} previousStatus={} attemptCount={}",
          view.outboxId().value(),
          view.eventId().value(),
          OutboxStatus.FAILED,
          view.attemptCount()
      );
      return view;
    } catch (ManualRetryRejectedException exception) {
      throw exception;
    } catch (RuntimeException exception) {
      metrics.manualRetry(
          "outbox",
          "failure"
      );
      log.error(
          "Unable to perform outbox manual retry outboxId={} errorType={}",
          outboxId.value(),
          exception.getClass().getSimpleName(),
          exception
      );
      throw exception;
    }
  }

  private void reject(OutboxId outboxId) {
    String detail = repository.findById(outboxId.value())
        .map(event -> "current status is " + event.getStatus() + "; only FAILED is retryable")
        .orElse("record was not found");
    metrics.manualRetry(
        "outbox",
        "rejected"
    );
    log.warn(
        "Outbox manual retry rejected outboxId={} {}",
        outboxId.value(),
        detail
    );
    throw new ManualRetryRejectedException(
        "outbox",
        outboxId.value(),
        detail
    );
  }

  private static OutboxEventView toView(
      OutboxEventEntity event,
      boolean includePayload
  ) {
    return new OutboxEventView(
        new OutboxId(event.getId()),
        new EventId(event.getEventId()),
        event.getEventType(),
        event.getEventTimestamp(),
        event.getSource(),
        event.getCorrelationId(),
        event.getDestination(),
        event.getStatus(),
        event.getAttemptCount(),
        event.getAvailableAt(),
        event.getLockedAt(),
        event.getLockedBy(),
        event.getLastError(),
        event.getCreatedAt(),
        event.getUpdatedAt(),
        event.getPublishedAt(),
        includePayload ? event.getPayload() : null
    );
  }

  private static OutboxEventView toView(OutboxSearchProjection event) {
    return new OutboxEventView(
        new OutboxId(event.getId()),
        new EventId(event.getEventId()),
        event.getEventType(),
        event.getEventTimestamp(),
        event.getSource(),
        event.getCorrelationId(),
        event.getDestination(),
        event.getStatus(),
        event.getAttemptCount(),
        event.getAvailableAt(),
        event.getLockedAt(),
        event.getLockedBy(),
        event.getLastError(),
        event.getCreatedAt(),
        event.getUpdatedAt(),
        event.getPublishedAt(),
        null
    );
  }
}
