package com.czetsuyatech.nerv.event.core.consumer;

import com.czetsuyatech.nerv.event.exception.EventRetryableException;
import java.util.Objects;

/**
 * <p>
 * Treats failures as retryable only when an {@link EventRetryableException} appears in their cause chain. The traversal
 * is type-safe and guarded against malformed cyclic cause chains.
 * </p>
 */
public final class DefaultInboxFailureClassifier implements InboxFailureClassifier {

  @Override
  public boolean isRetryable(Throwable failure) {
    Throwable current = Objects.requireNonNull(
        failure,
        "failure must not be null"
    );
    while (current != null) {
      if (current instanceof EventRetryableException) {
        return true;
      }
      Throwable cause = current.getCause();
      if (cause == current) {
        return false;
      }
      current = cause;
    }
    return false;
  }
}
