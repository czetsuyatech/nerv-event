package com.czetsuyatech.nerv.event.core.broker;

/**
 * Sends messages through one broker integration.
 */
public interface BrokerProducer {
  BrokerId brokerId();

  BrokerPublishResult publish(BrokerMessage message) throws Exception;
}
