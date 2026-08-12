package com.czetsuyatech.nerv.event.sqs.client;

import java.util.Objects;

/**
 * Stable identifier for one reusable SQS client/account configuration.
 */
public record SqsClientId(String value) {
  public SqsClientId {
    Objects.requireNonNull(
        value,
        "value must not be null"
    );
    if (value.isBlank()) {
      throw new IllegalArgumentException("value must not be blank");
    }
  }
}
