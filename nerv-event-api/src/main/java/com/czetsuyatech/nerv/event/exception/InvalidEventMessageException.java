package com.czetsuyatech.nerv.event.exception;

/**
 * Indicates that generic event metadata or the event-message contract is invalid.
 */
public class InvalidEventMessageException extends NervEventException {

  public InvalidEventMessageException(String message) {
    super(message);
  }

  public InvalidEventMessageException(String message, Throwable cause) {
    super(message, cause);
  }
}
