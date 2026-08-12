package com.czetsuyatech.nerv.event.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class EventRetryableExceptionTest {

  @Test
  void isAnApplicationFacingNervEventException() {
    assertThat(EventRetryableException.class).isAssignableTo(NervEventException.class);
  }

  @Test
  void retainsMessageAndCauseAcrossAllSupportedConstructors() {
    RuntimeException cause = new RuntimeException("dependency unavailable");

    assertThat(new EventRetryableException("try later")).hasMessage("try later").hasNoCause();
    assertThat(new EventRetryableException(cause)).cause().isSameAs(cause);
    assertThat(new EventRetryableException("try later", cause)).hasMessage("try later").cause().isSameAs(cause);
  }
}
