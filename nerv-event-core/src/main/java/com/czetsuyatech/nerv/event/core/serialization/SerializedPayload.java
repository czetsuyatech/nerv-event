package com.czetsuyatech.nerv.event.core.serialization;

import java.util.Objects;
import lombok.Builder;

/**
 * Immutable textual event content and its media type.
 */
@Builder
public record SerializedPayload(
    String value,
    String contentType
)
{

  public SerializedPayload {
    Objects.requireNonNull(
        value,
        "value must not be null"
    );
    Objects.requireNonNull(
        contentType,
        "contentType must not be null"
    );
    if (contentType.isBlank()) {
      throw new IllegalArgumentException("contentType must not be blank");
    }
  }
}
