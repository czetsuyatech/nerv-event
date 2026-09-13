package com.czetsuyatech.nerv.event.core.inbox;

/** Signals that handler execution completed but the durable PROCESSED transition did not. */
public final class InboxCompletionException extends RuntimeException {

  public InboxCompletionException(Throwable cause) {
    super("Inbox PROCESSED transition could not be committed", cause);
  }
}
