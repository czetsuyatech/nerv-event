package com.czetsuyatech.nerv.event.sqs.producer;

import com.czetsuyatech.nerv.event.exception.NervEventException;

/**
 * Adds routing context to an SQS send failure while preserving its cause.
 */
public class SqsPublishException extends NervEventException {
  public SqsPublishException(
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
