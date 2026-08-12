package com.czetsuyatech.nerv.event.spring.broker;

import com.czetsuyatech.nerv.event.core.broker.BrokerProducer;

/**
 * Optional, transparent decoration applied when the broker producer registry is assembled.
 */
public interface BrokerProducerDecorator {
  BrokerProducer decorate(BrokerProducer producer);
}
