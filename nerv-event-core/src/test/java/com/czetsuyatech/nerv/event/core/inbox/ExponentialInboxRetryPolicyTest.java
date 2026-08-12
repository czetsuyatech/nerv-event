package com.czetsuyatech.nerv.event.core.inbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class ExponentialInboxRetryPolicyTest {

  private static final Instant FAILED_AT = Instant.parse("2026-08-17T00:00:00Z");

  @Test
  void appliesCappedExponentialDelaysAndTotalAttemptLimit() {
    InboxRetryPolicy policy = new ExponentialInboxRetryPolicy(
        3,
        Duration.ofSeconds(1),
        2.0,
        Duration.ofSeconds(3)
    );

    assertThat(policy.canRetry(1)).isTrue();
    assertThat(policy.canRetry(2)).isTrue();
    assertThat(policy.canRetry(3)).isFalse();
    assertThat(
        policy.nextAttemptAt(
            1,
            FAILED_AT
        )
    ).isEqualTo(FAILED_AT.plusSeconds(1));
    assertThat(
        policy.nextAttemptAt(
            2,
            FAILED_AT
        )
    ).isEqualTo(FAILED_AT.plusSeconds(2));
    assertThat(
        policy.nextAttemptAt(
            3,
            FAILED_AT
        )
    ).isEqualTo(FAILED_AT.plusSeconds(3));
  }

  @Test
  void rejectsInvalidConfiguration() {
    assertThatThrownBy(
        () -> new ExponentialInboxRetryPolicy(
            0,
            Duration.ofSeconds(1),
            2.0,
            Duration.ofSeconds(3)
        )
    )
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
        () -> new ExponentialInboxRetryPolicy(
            1,
            Duration.ZERO,
            2.0,
            Duration.ofSeconds(3)
        )
    )
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
        () -> new ExponentialInboxRetryPolicy(
            1,
            Duration.ofSeconds(1),
            0.5,
            Duration.ofSeconds(3)
        )
    )
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
        () -> new ExponentialInboxRetryPolicy(
            1,
            Duration.ofSeconds(3),
            2.0,
            Duration.ofSeconds(1)
        )
    )
        .isInstanceOf(IllegalArgumentException.class);
  }
}
