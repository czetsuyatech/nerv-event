package com.czetsuyatech.nerv.event.spring.dispatcher;

import com.czetsuyatech.nerv.event.core.outbox.DispatchResult;
import java.time.Duration;

/**
 * Receives completed outbox scheduler cycles without coupling scheduling to a metrics system.
 */
public interface OutboxDispatchCycleListener {
  void onSuccess(
      DispatchResult result,
      Duration duration
  );

  void onFailure(
      Duration duration,
      Exception failure
  );
}
