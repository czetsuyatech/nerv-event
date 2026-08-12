package com.czetsuyatech.nerv.event.application.dto;

import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.model.EventId;
import java.time.Instant;

/**
 * Explicit, bounded filters for paginated outbox inspection.
 */
public record OutboxQuery(
    OutboxStatus status,
    EventId eventId,
    String eventType,
    String destination,
    String source,
    String correlationId,
    Instant createdFrom,
    Instant createdTo,
    Instant updatedFrom,
    Instant updatedTo,
    int pageNumber,
    int pageSize
)
{

  public static final int DEFAULT_PAGE_SIZE = 50;
  public static final int MAX_PAGE_SIZE = 200;
  public OutboxQuery {
    validate(
        pageNumber,
        pageSize,
        createdFrom,
        createdTo,
        "created"
    );
    validateRange(
        updatedFrom,
        updatedTo,
        "updated"
    );
  }

  public static OutboxQuery firstPage() {
    return new OutboxQuery(
        null,
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

  private static void validate(
      int pageNumber,
      int pageSize,
      Instant from,
      Instant to,
      String name
  ) {
    if (pageNumber < 0)
      throw new IllegalArgumentException("pageNumber must not be negative");
    if (pageSize < 1 || pageSize > MAX_PAGE_SIZE) {
      throw new IllegalArgumentException("pageSize must be between 1 and " + MAX_PAGE_SIZE);
    }
    validateRange(
        from,
        to,
        name
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
