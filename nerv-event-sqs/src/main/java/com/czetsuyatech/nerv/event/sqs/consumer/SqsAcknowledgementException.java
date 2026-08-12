package com.czetsuyatech.nerv.event.sqs.consumer;

import com.czetsuyatech.nerv.event.exception.NervEventException;

/**
 * Indicates SQS deletion failure after a durable Inbox outcome.
 */
public class SqsAcknowledgementException extends NervEventException {
  public SqsAcknowledgementException(Throwable cause) {
    super(
        "SQS acknowledgement failed after durable Inbox outcome",
        cause
    );
  }
}
