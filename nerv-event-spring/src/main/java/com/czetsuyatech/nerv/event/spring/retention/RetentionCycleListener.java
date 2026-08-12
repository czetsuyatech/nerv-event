package com.czetsuyatech.nerv.event.spring.retention;

import java.time.Duration;

/**
 * Optional observer of retention scheduler cycles.
 */
public interface RetentionCycleListener {

  void onSuccess(
      RetentionResult result,
      Duration duration
  );

  void onFailure(
      Duration duration,
      Exception failure
  );
}
