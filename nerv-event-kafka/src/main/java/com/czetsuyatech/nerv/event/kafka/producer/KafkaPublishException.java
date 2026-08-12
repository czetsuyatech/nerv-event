package com.czetsuyatech.nerv.event.kafka.producer;

import com.czetsuyatech.nerv.event.exception.NervEventException;

/**
 * Adds Kafka topic and event diagnostics to a publish failure.
 */
public final class KafkaPublishException extends NervEventException {

  public KafkaPublishException(
      String message,
      Throwable cause
  )
  {
    super(
        message,
        cause
    );
  }
}
