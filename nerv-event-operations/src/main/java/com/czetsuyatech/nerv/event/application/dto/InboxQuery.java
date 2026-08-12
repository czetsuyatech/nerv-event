package com.czetsuyatech.nerv.event.application.dto;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.model.EventId;
import java.time.Instant;

/**
 * Explicit, bounded filters for paginated inbox inspection.
 */
public record InboxQuery(
    InboxStatus status,
    EventId eventId,
    String eventType,
    String source,
    String correlationId,
    Instant receivedFrom,
    Instant receivedTo,
    Instant updatedFrom,
    Instant updatedTo,
    int pageNumber,
    int pageSize
)
{

  public static final int DEFAULT_PAGE_SIZE = 50;
  public static final int MAX_PAGE_SIZE = 200;
  public InboxQuery {
    if (pageNumber < 0)
      throw new IllegalArgumentException("pageNumber must not be negative");
    if (pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
      throw new IllegalArgumentException("pageSize must be between 1 and " + MAX_PAGE_SIZE);
    }
    validateRange(
        receivedFrom,
        receivedTo,
        "received"
    );
    validateRange(
        updatedFrom,
        updatedTo,
        "updated"
    );
  }

  public static InboxQuery firstPage() {
    return new InboxQuery(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        0,
        DEFAULT_PAGE_SIZE
    );
  }

  private static void validateRange(
      Instant from,
      Instant to,
      String name
  ) {
    if (from != null && to != null && from.isAfter(to)) {
      throw new IllegalArgumentException(name + "From must not be after " + name + "To");
    }
  }
}
