package com.czetsuyatech.nerv.event.persistence.repository;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;

/**
 * Bounded operational-recovery metric recorder.
 */
public final class OperationsMetrics {

  private final MeterRegistry registry;

  private OperationsMetrics(MeterRegistry registry) {
    this.registry = registry;
  }

  public static OperationsMetrics of(ObjectProvider<MeterRegistry> meterRegistry) {
    return new OperationsMetrics(meterRegistry.getIfAvailable());
  }

  public static OperationsMetrics of(MeterRegistry meterRegistry) {
    return new OperationsMetrics(meterRegistry);
  }

  public void manualRetry(
      String direction,
      String result
  ) {
    if (registry != null) {
      registry.counter(
          "nerv.event.operations.retry",
          "direction",
          direction,
          "result",
          result
      )
          .increment();
    }
  }
}
