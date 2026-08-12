package com.czetsuyatech.nerv.event.persistence.repository;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.InboxEventEntity;
import java.time.Instant;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Persistence boundary for operational inbox lookup, search, and recovery.
 */
public interface InboxOperationRepository
    extends
      JpaRepository<InboxEventEntity, String>,
      JpaSpecificationExecutor<InboxEventEntity> {

  @Modifying(clearAutomatically = true, flushAutomatically = true)
  @Query("""
      update InboxEventEntity event
      set event.status = :retryPending,
          event.availableAt = :availableAt,
          event.updatedAt = :updatedAt,
          event.processingAt = null,
          event.processingBy = null,
          event.version = event.version + 1
      where event.eventId = :eventId and event.status = :failed
      """)
  int retryFailed(
      @Param("eventId") String eventId,
      @Param("failed") InboxStatus failed,
      @Param("retryPending") InboxStatus retryPending,
      @Param("availableAt") Instant availableAt,
      @Param("updatedAt") Instant updatedAt
  );
}
