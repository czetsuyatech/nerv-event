package com.czetsuyatech.nerv.event.application.dto;

import java.util.List;

/**
 * One stable, newest-updated-first page of inbox inspection results.
 */
public record InboxSearchResult(
    List<InboxEventView> events,
    int pageNumber,
    int pageSize,
    long totalElements,
    int totalPages
)
{
  public InboxSearchResult {
    events = List.copyOf(events);
  }
}
