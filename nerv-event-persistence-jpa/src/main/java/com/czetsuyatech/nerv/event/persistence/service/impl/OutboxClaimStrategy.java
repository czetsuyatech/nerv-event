package com.czetsuyatech.nerv.event.persistence.service.impl;

import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Specialized persistence operation for atomically leasing eligible outbox rows.
 */
public interface OutboxClaimStrategy {

  List<OutboxEventEntity> claim(
      Instant claimedAt,
      int batchSize,
      String owner,
      Duration leaseDuration
  );
}
