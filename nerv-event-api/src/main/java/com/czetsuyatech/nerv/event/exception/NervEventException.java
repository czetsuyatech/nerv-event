package com.czetsuyatech.nerv.event.exception;

/**
 * Base runtime exception for application-facing Nerv Event failures.
 */
public class NervEventException extends RuntimeException {

  public NervEventException(String message) {
    super(message);
  }

  public NervEventException(String message, Throwable cause) {
    super(message, cause);
  }

  public NervEventException(Throwable cause) {
    super(cause);
  }
}
