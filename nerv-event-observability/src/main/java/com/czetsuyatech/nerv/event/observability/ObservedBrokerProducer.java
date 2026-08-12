package com.czetsuyatech.nerv.event.observability;

import com.czetsuyatech.nerv.event.core.broker.BrokerId;
import com.czetsuyatech.nerv.event.core.broker.BrokerMessage;
import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;
import com.czetsuyatech.nerv.event.core.broker.BrokerPublishResult;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Transparent broker producer decorator with only the bounded broker/result tags.
 */
public final class ObservedBrokerProducer implements BrokerProducer {

  private final BrokerProducer delegate;
  private final NervEventMetrics metrics;

  public ObservedBrokerProducer(
      BrokerProducer delegate,
      NervEventMetrics metrics
  )
  {
    this.delegate = delegate;
    this.metrics = metrics;
  }

  @Override
  public BrokerId brokerId() {
    return delegate.brokerId();
  }

  @Override
  public BrokerPublishResult publish(BrokerMessage message) throws Exception {
    long started = System.nanoTime();
    String result = "success";
    try {
      return delegate.publish(message);
    } catch (Exception ex) {
      result = isTimeout(ex) ? "timeout" : "failure";
      throw ex;
    } finally {
      metrics.brokerPublish(
          brokerId().value(),
          result,
          Duration.ofNanos(System.nanoTime() - started)
      );
    }
  }

  private static boolean isTimeout(Throwable ex) {
    for (Throwable t = ex; t != null; t = t.getCause()) {
      if (t instanceof TimeoutException) {
        return true;
      }
    }
    return false;
  }
}
