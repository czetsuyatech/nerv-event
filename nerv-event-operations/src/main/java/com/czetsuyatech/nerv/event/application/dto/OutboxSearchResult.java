package com.czetsuyatech.nerv.event.application.dto;

import java.util.List;

/**
 * One stable, newest-updated-first page of outbox inspection results.
 */
public record OutboxSearchResult(
    List<OutboxEventView> events,
    int pageNumber,
    int pageSize,
    long totalElements,
    int totalPages
)
{
  public OutboxSearchResult {
    events = List.copyOf(events);
  }
}
