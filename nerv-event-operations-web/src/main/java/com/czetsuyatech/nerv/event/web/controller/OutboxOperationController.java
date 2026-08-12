package com.czetsuyatech.nerv.event.web.controller;

import com.czetsuyatech.nerv.event.application.dto.PageResponse;
import com.czetsuyatech.nerv.event.application.mapper.WebQuerySupport;
import com.czetsuyatech.nerv.event.autoconfigure.NervEventOperationWebProperties;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.exception.OperationRecordNotFoundException;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.application.dto.OutboxEventView;
import com.czetsuyatech.nerv.event.services.OutboxOperationService;
import com.czetsuyatech.nerv.event.application.dto.OutboxQuery;
import com.czetsuyatech.nerv.event.application.dto.OutboxSearchResult;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP adapter over {@link OutboxOperationService}; it has no persistence or broker dependency.
 */
@RestController
@RequestMapping("${nerv.event.operations.web.base-path:/management/nerv-event}/outbox")
@RequiredArgsConstructor
@Slf4j
public class OutboxOperationController {

  private final OutboxOperationService operations;
  private final NervEventOperationWebProperties properties;

  @GetMapping
  public PageResponse<OutboxSummaryResponse> search(
      @RequestParam(required = false) String status,
      @RequestParam(required = false) String eventId,
      @RequestParam(required = false) String eventType,
      @RequestParam(required = false) String destination,
      @RequestParam(required = false) String source,
      @RequestParam(required = false) String correlationId,
      @RequestParam(required = false) String createdFrom,
      @RequestParam(required = false) String createdTo,
      @RequestParam(required = false) String updatedFrom,
      @RequestParam(required = false) String updatedTo,
      @RequestParam(defaultValue = "0") int page,
      @RequestParam(defaultValue = "20") int size
  ) {
    OutboxSearchResult result = operations.search(
        new OutboxQuery(
            status == null
                ? null
                : OutboxStatus.valueOf(status),
            eventId == null
                ? null
                : new EventId(eventId),
            eventType,
            destination,
            source,
            correlationId,
            WebQuerySupport.instant(
                createdFrom,
                "createdFrom"
            ),
            WebQuerySupport.instant(
                createdTo,
                "createdTo"
            ),
            WebQuerySupport.instant(
                updatedFrom,
                "updatedFrom"
            ),
            WebQuerySupport.instant(
                updatedTo,
                "updatedTo"
            ),
            page,
            WebQuerySupport.pageSize(size)
        )
    );
    return new PageResponse<>(
        result.events().stream().map(OutboxSummaryResponse::from).toList(),
        result.pageNumber(),
        result.pageSize(),
        result.totalElements(),
        result.totalPages()
    );
  }

  @GetMapping("/{outboxId}")
  public OutboxDetailResponse find(@PathVariable String outboxId) {
    return operations.find(new OutboxId(outboxId))
        .map(
            view -> OutboxDetailResponse.from(
                view,
                properties.getPayload().isEnabled()
            )
        )
        .orElseThrow(
            () -> new OperationRecordNotFoundException(
                "outbox",
                outboxId
            )
        );
  }

  @PostMapping("/{outboxId}/retry")
  public ResponseEntity<OutboxRetryResponse> retry(@PathVariable String outboxId) {
    OutboxId id = new OutboxId(outboxId);
    OutboxEventView existing = operations.find(id)
        .orElseThrow(
            () -> new OperationRecordNotFoundException(
                "outbox",
                outboxId
            )
        );
    OutboxEventView retried = operations.retryFailed(id);
    log.info(
        "Outbox manual retry accepted outboxId={} eventId={} previousStatus={} newStatus={}",
        retried.outboxId().value(),
        retried.eventId().value(),
        existing.status(),
        retried.status()
    );
    return ResponseEntity.status(HttpStatus.ACCEPTED)
        .body(
            new OutboxRetryResponse(
                retried.outboxId().value(),
                retried.eventId().value(),
                existing.status().name(),
                retried.status().name()
            )
        );
  }

  public record OutboxSummaryResponse(
      String outboxId,
      String eventId,
      String eventType,
      String source,
      String correlationId,
      String destination,
      String status,
      int attemptCount,
      Instant availableAt,
      Instant createdAt,
      Instant updatedAt,
      Instant publishedAt,
      String lastError
  )
  {
    static OutboxSummaryResponse from(OutboxEventView view) {
      return new OutboxSummaryResponse(
          view.outboxId().value(),
          view.eventId().value(),
          view.eventType(),
          view.source(),
          view.correlationId(),
          view.destination(),
          view.status().name(),
          view.attemptCount(),
          view.availableAt(),
          view.createdAt(),
          view.updatedAt(),
          view.publishedAt(),
          view.lastError()
      );
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record OutboxDetailResponse(
      String outboxId,
      String eventId,
      String eventType,
      Instant eventTimestamp,
      String source,
      String correlationId,
      String destination,
      String status,
      int attemptCount,
      Instant availableAt,
      Instant lockedAt,
      String lockedBy,
      String lastError,
      Instant createdAt,
      Instant updatedAt,
      Instant publishedAt,
      String contentType,
      String payload
  )
  {
    static OutboxDetailResponse from(
        OutboxEventView view,
        boolean includePayload
    ) {
      // The durable outbox model does not persist a content type; preserve that
      // absence as null.
      return new OutboxDetailResponse(
          view.outboxId().value(),
          view.eventId().value(),
          view.eventType(),
          view.eventTimestamp(),
          view.source(),
          view.correlationId(),
          view.destination(),
          view.status().name(),
          view.attemptCount(),
          view.availableAt(),
          view.lockedAt(),
          view.lockedBy(),
          view.lastError(),
          view.createdAt(),
          view.updatedAt(),
          view.publishedAt(),
          null,
          includePayload
              ? view.payload()
              : null
      );
    }
  }

  public record OutboxRetryResponse(
      String id,
      String eventId,
      String previousStatus,
      String status
  )
  {
  }
}
