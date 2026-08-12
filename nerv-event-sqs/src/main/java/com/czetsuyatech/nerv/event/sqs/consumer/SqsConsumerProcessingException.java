package com.czetsuyatech.nerv.event.sqs.consumer;

/**
 * Identifies an unresolved durability stage; the SQS delivery must remain unacknowledged.
 */
public class SqsConsumerProcessingException extends RuntimeException {
  private final String stage;

  public SqsConsumerProcessingException(
      String stage,
      Throwable cause
  )
  {
    super(
        "SQS consumer processing failed at stage " + stage,
        cause
    );
    this.stage = stage;
  }

  public String stage() {
    return stage;
  }
}
