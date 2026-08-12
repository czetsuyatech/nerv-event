package com.czetsuyatech.nerv.event.core.consumer;

/**
 * Classifies failures from application event handling without coupling to a broker.
 */
public interface InboxFailureClassifier {

  boolean isRetryable(Throwable failure);
}
