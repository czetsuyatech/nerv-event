package com.czetsuyatech.nerv.event.spring.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class NervEventRetentionPropertiesTest {

  @Test
  void suppliesConservativeRetentionDefaults() {
    NervEventProperties.Retention properties = new NervEventProperties.Retention();

    assertThat(properties.isEnabled()).isFalse();
    assertThat(properties.getOutbox().isEnabled()).isTrue();
    assertThat(properties.getOutbox().getAge()).isEqualTo(Duration.ofDays(30));
    assertThat(properties.getInbox().isEnabled()).isTrue();
    assertThat(properties.getInbox().getAge()).isEqualTo(Duration.ofDays(30));
    assertThat(properties.getBatchSize()).isEqualTo(500);
    assertThat(properties.getPolling().getMinInterval()).isEqualTo(Duration.ofSeconds(30));
    assertThat(properties.getPolling().getMaxInterval()).isEqualTo(Duration.ofMinutes(30));
  }

  @Test
  void rejectsUnsafeRetentionConfiguration() {
    NervEventProperties properties = new NervEventProperties();
    properties.getRetention().setBatchSize(0);
    assertThatThrownBy(properties::validate).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("retention.batch-size");

    properties.getRetention().setBatchSize(1);
    properties.getRetention().getOutbox().setAge(Duration.ZERO);
    assertThatThrownBy(properties::validate).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("retention.outbox.age");
  }
}
