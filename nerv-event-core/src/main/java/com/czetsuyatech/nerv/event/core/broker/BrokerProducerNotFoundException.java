package com.czetsuyatech.nerv.event.core.broker;

import com.czetsuyatech.nerv.event.exception.NervEventException;

/**
 * Raised when no broker producer is registered for a configured broker.
 */
public final class BrokerProducerNotFoundException extends NervEventException {

  public BrokerProducerNotFoundException(BrokerId brokerId) {
    super("No BrokerProducer registered for broker '" + brokerId.value() + "'");
  }
}
