package com.czetsuyatech.nerv.event.sqs.consumer;

import com.czetsuyatech.nerv.event.exception.InvalidEventMessageException;

/**
 * Indicates missing or malformed generic event metadata on an SQS delivery.
 */
public class InvalidSqsEventMessageException extends InvalidEventMessageException {
  public InvalidSqsEventMessageException(String message) {
    super(message);
  }

  public InvalidSqsEventMessageException(
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
