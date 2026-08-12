package com.czetsuyatech.nerv.event.exception;

/**
 * <p>
 * Signals that an application event handler failed transiently and may be invoked again according to the configured
 * inbox retry policy.
 * </p>
 * <p>
 * This exception only classifies the failure. {@code InboxRetryPolicy} owns retry timing and attempt limits.
 * </p>
 */
public class EventRetryableException extends NervEventException {

  public EventRetryableException(String message) {
    super(message);
  }

  public EventRetryableException(String message, Throwable cause) {
    super(message, cause);
  }

  public EventRetryableException(Throwable cause) {
    super(cause);
  }
}
