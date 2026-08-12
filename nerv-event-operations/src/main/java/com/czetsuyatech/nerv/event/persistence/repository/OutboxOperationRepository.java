package com.czetsuyatech.nerv.event.persistence.repository;

import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import java.time.Instant;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Persistence boundary for operational outbox lookup, search, and recovery.
 */
public interface OutboxOperationRepository
    extends
      JpaRepository<OutboxEventEntity, String>,
      JpaSpecificationExecutor<OutboxEventEntity> {

  @Modifying(clearAutomatically = true, flushAutomatically = true)
  @Query("""
      update OutboxEventEntity event
      set event.status = :pending,
          event.availableAt = :availableAt,
          event.updatedAt = :updatedAt,
          event.lockedAt = null,
          event.lockedBy = null,
          event.version = event.version + 1
      where event.id = :id and event.status = :failed
      """)
  int retryFailed(
      @Param("id") String id,
      @Param("failed") OutboxStatus failed,
      @Param("pending") OutboxStatus pending,
      @Param("availableAt") Instant availableAt,
      @Param("updatedAt") Instant updatedAt
  );
}
