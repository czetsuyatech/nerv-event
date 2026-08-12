package com.czetsuyatech.nerv.event.core.broker;

import java.util.Objects;
import lombok.Builder;

/**
 * Broker acknowledgement information for a successfully published message.
 */
@Builder
public record BrokerPublishResult(String brokerReference) {

  public BrokerPublishResult {
    Objects.requireNonNull(
        brokerReference,
        "brokerReference must not be null"
    );
    if (brokerReference.isBlank()) {
      throw new IllegalArgumentException("brokerReference must not be blank");
    }
  }
}
