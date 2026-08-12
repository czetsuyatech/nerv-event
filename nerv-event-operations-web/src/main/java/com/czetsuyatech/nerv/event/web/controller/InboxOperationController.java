package com.czetsuyatech.nerv.event.web.controller;

import com.czetsuyatech.nerv.event.application.dto.PageResponse;
import com.czetsuyatech.nerv.event.application.mapper.WebQuerySupport;
import com.czetsuyatech.nerv.event.autoconfigure.NervEventOperationWebProperties;
import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.exception.OperationRecordNotFoundException;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.application.dto.InboxEventView;
import com.czetsuyatech.nerv.event.services.InboxOperationService;
import com.czetsuyatech.nerv.event.application.dto.InboxQuery;
import com.czetsuyatech.nerv.event.application.dto.InboxSearchResult;
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
 * HTTP adapter over {@link InboxOperationService}; it has no persistence or broker dependency.
 */
@RestController
@RequestMapping("${nerv.event.operations.web.base-path:/management/nerv-event}/inbox")
@RequiredArgsConstructor
@Slf4j
public class InboxOperationController {

  private final InboxOperationService operations;
  private final NervEventOperationWebProperties properties;

  @GetMapping
  public PageResponse<InboxSummaryResponse> search(
      @RequestParam(required = false) String status,
      @RequestParam(required = false) String eventId,
      @RequestParam(required = false) String eventType,
      @RequestParam(required = false) String source,
      @RequestParam(required = false) String correlationId,
      @RequestParam(required = false) String receivedFrom,
      @RequestParam(required = false) String receivedTo,
      @RequestParam(required = false) String updatedFrom,
      @RequestParam(required = false) String updatedTo,
      @RequestParam(defaultValue = "0") int page,
      @RequestParam(defaultValue = "20") int size
  ) {

    log.info(
        "REST request to SEARCH events with parameters: status={}, eventId={}, eventType={}, source={}, correlationId={}, receivedFrom={}, receivedTo={}, updatedFrom={}, updatedTo={}, page={}, size={}",
        status,
        eventId,
        eventType,
        source,
        correlationId,
        receivedFrom,
        receivedTo,
        updatedFrom,
        updatedTo,
        page,
        size
    );

    InboxSearchResult result = operations.search(
        new InboxQuery(
            status == null
                ? null
                : InboxStatus.valueOf(status),
            eventId == null
                ? null
                : new EventId(eventId),
            eventType,
            source,
            correlationId,
            WebQuerySupport.instant(
                receivedFrom,
                "receivedFrom"
            ),
            WebQuerySupport.instant(
                receivedTo,
                "receivedTo"
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
        result.events()
            .stream()
            .map(InboxSummaryResponse::from)
            .toList(),
        result.pageNumber(),
        result.pageSize(),
        result.totalElements(),
        result.totalPages()
    );
  }

  @GetMapping("/{eventId}")
  public InboxDetailResponse find(@PathVariable String eventId) {
    return operations.find(new EventId(eventId))
        .map(
            view -> InboxDetailResponse.from(
                view,
                properties.getPayload().isEnabled()
            )
        )
        .orElseThrow(
            () -> new OperationRecordNotFoundException(
                "inbox",
                eventId
            )
        );
  }

  @PostMapping("/{eventId}/retry")
  public ResponseEntity<InboxRetryResponse> retry(@PathVariable String eventId) {
    EventId id = new EventId(eventId);
    InboxEventView existing = operations.find(id)
        .orElseThrow(
            () -> new OperationRecordNotFoundException(
                "inbox",
                eventId
            )
        );
    InboxEventView retried = operations.retryFailed(id);
    log.info(
        "Inbox manual retry accepted eventId={} previousStatus={} newStatus={}",
        retried.eventId().value(),
        existing.status(),
        retried.status()
    );
    return ResponseEntity.status(HttpStatus.ACCEPTED)
        .body(
            new InboxRetryResponse(
                retried.eventId().value(),
                existing.status().name(),
                retried.status().name()
            )
        );
  }

  public record InboxSummaryResponse(
      String eventId,
      String eventType,
      String source,
      String correlationId,
      String status,
      int attemptCount,
      Instant receivedAt,
      Instant availableAt,
      Instant processingAt,
      String processingBy,
      Instant processedAt,
      Instant failedAt,
      Instant updatedAt,
      String lastError
  )
  {

    static InboxSummaryResponse from(InboxEventView view) {
      return new InboxSummaryResponse(
          view.eventId().value(),
          view.eventType(),
          view.source(),
          view.correlationId(),
          view.status().name(),
          view.attemptCount(),
          view.receivedAt(),
          view.availableAt(),
          view.processingAt(),
          view.processingBy(),
          view.processedAt(),
          view.failedAt(),
          view.updatedAt(),
          view.lastError()
      );
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record InboxDetailResponse(
      String eventId,
      String eventType,
      Instant eventTimestamp,
      String source,
      String correlationId,
      String status,
      int attemptCount,
      Instant receivedAt,
      Instant availableAt,
      Instant processingAt,
      String processingBy,
      Instant processedAt,
      Instant failedAt,
      String lastError,
      Instant createdAt,
      Instant updatedAt,
      String contentType,
      String payload
  )
  {

    static InboxDetailResponse from(
        InboxEventView view,
        boolean includePayload
    ) {
      return new InboxDetailResponse(
          view.eventId().value(),
          view.eventType(),
          view.eventTimestamp(),
          view.source(),
          view.correlationId(),
          view.status().name(),
          view.attemptCount(),
          view.receivedAt(),
          view.availableAt(),
          view.processingAt(),
          view.processingBy(),
          view.processedAt(),
          view.failedAt(),
          view.lastError(),
          view.createdAt(),
          view.updatedAt(),
          includePayload
              ? view.contentType()
              : null,
          includePayload
              ? view.payload()
              : null
      );
    }
  }

  public record InboxRetryResponse(
      String eventId,
      String previousStatus,
      String status
  )
  {

  }
}
