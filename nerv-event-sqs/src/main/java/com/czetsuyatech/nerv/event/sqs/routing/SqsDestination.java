package com.czetsuyatech.nerv.event.sqs.routing;

import com.czetsuyatech.nerv.event.sqs.client.SqsClientId;
import java.util.Objects;

/**
 * Resolved SQS-specific destination. Queue may be a queue name or URL.
 */
public record SqsDestination(
    SqsClientId clientId,
    String queue
)
{
  public SqsDestination {
    Objects.requireNonNull(
        clientId,
        "clientId must not be null"
    );
    Objects.requireNonNull(
        queue,
        "queue must not be null"
    );
    if (queue.isBlank()) {
      throw new IllegalArgumentException("queue must not be blank");
    }
  }
}
