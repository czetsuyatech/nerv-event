package com.czetsuyatech.nerv.event.exception;

/**
 * Raised only after the operations contract reports that a durable record is absent.
 */
public final class OperationRecordNotFoundException extends RuntimeException {

  public OperationRecordNotFoundException(
      String direction,
      String identifier
  )
  {
    super("No " + direction + " record exists for " + identifier);
  }
}
