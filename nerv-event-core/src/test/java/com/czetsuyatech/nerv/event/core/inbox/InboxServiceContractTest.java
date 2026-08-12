package com.czetsuyatech.nerv.event.core.inbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.czetsuyatech.nerv.event.model.EventId;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class InboxServiceContractTest {

  @Test
  void exposesPendingRetryClaimAndExplicitFailureTransitions() throws NoSuchMethodException {
    assertThat(
        InboxService.class.getMethod(
            "claimPendingRetries",
            int.class,
            Instant.class,
            String.class,
            Duration.class
        )
    ).isNotNull();

    assertThat(
        InboxService.class.getMethod(
            "markRetryPending",
            EventId.class,
            String.class,
            int.class,
            Instant.class,
            Instant.class,
            String.class
        )
    ).isNotNull();

    Method terminalMarkFailed = InboxService.class.getMethod(
        "markFailed",
        EventId.class,
        String.class,
        int.class,
        Instant.class,
        String.class
    );
    assertThat(terminalMarkFailed.getParameterCount()).isEqualTo(5);

    assertThatThrownBy(
        () -> InboxService.class.getMethod(
            "claimFailed",
            int.class,
            Instant.class,
            String.class,
            Duration.class
        )
    ).isInstanceOf(NoSuchMethodException.class);
  }
}
