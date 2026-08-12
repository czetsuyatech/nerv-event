package com.czetsuyatech.nerv.event.exception;

/**
 * Raised when a manual retry cannot atomically transition a FAILED durable row.
 */
public final class ManualRetryRejectedException extends RuntimeException {

  public ManualRetryRejectedException(
      String direction,
      String identifier,
      String detail
  )
  {
    super("Manual " + direction + " retry rejected for " + identifier + ": " + detail);
  }
}
