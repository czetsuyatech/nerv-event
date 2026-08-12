package com.czetsuyatech.nerv.event.spring.dispatcher;

import com.czetsuyatech.nerv.event.core.inbox.InboxRetryResult;
import java.time.Duration;

/**
 * Receives completed inbox retry scheduler cycles without coupling scheduling to a metrics system.
 */
public interface InboxRetryCycleListener {
  void onSuccess(
      InboxRetryResult result,
      Duration duration
  );

  void onFailure(
      Duration duration,
      Exception failure
  );
}
