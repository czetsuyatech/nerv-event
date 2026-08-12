package com.czetsuyatech.nerv.event.persistence.persistence.repository;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.InboxEventEntity;
import jakarta.persistence.LockModeType;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Spring Data operations for owner-guarded inbox state transitions.
 */
public interface InboxEventRepository extends JpaRepository<InboxEventEntity, String> {

  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("select event from InboxEventEntity event where event.eventId = :eventId")
  Optional<InboxEventEntity> findByEventIdForUpdate(@Param("eventId") String eventId);

  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("""
      select event from InboxEventEntity event
      where event.status = :retryPending
        and event.availableAt is not null
        and event.availableAt <= :now
      order by event.availableAt asc
      """)
  List<InboxEventEntity> findRetryPendingForUpdate(
      @Param("retryPending") InboxStatus retryPending,
      @Param("now") Instant now,
      Pageable pageable
  );

  long countByStatus(InboxStatus status);

  @Modifying
  @Query(value = """
      delete from nerv_inbox_event
      where event_id in (
        select event_id from (
          select event_id
          from nerv_inbox_event
          where status = 'PROCESSED' and processed_at <= :cutoff
          order by processed_at asc, event_id asc
          limit :limit
        ) retention_candidates
      )
        and status = 'PROCESSED'
        and processed_at <= :cutoff
      """, nativeQuery = true)
  int deleteProcessedBefore(
      @Param("cutoff") Instant cutoff,
      @Param("limit") int limit
  );

  @Query("select min(event.failedAt) from InboxEventEntity event where event.status = :status")
  Instant oldestFailedAtByStatus(@Param("status") InboxStatus status);

  @Modifying
  @Query("""
      update InboxEventEntity event
      set event.status = :processed,
          event.attemptCount = event.attemptCount + 1,
          event.processedAt = :processedAt,
          event.updatedAt = :processedAt,
          event.processingAt = null,
          event.processingBy = null,
          event.availableAt = null,
          event.failedAt = null,
          event.lastError = null,
          event.version = event.version + 1
      where event.eventId = :eventId
        and event.status = :processing
        and event.processingBy = :owner
      """)
  int markProcessed(
      @Param("eventId") String eventId,
      @Param("owner") String owner,
      @Param("processed") InboxStatus processed,
      @Param("processing") InboxStatus processing,
      @Param("processedAt") Instant processedAt
  );

  @Modifying
  @Query("""
      update InboxEventEntity event
      set event.status = :retryPending,
          event.attemptCount = :attemptCount,
          event.failedAt = :failedAt,
          event.availableAt = :availableAt,
          event.lastError = :lastError,
          event.updatedAt = :failedAt,
          event.processingAt = null,
          event.processingBy = null,
          event.processedAt = null,
          event.version = event.version + 1
      where event.eventId = :eventId
        and event.status = :processing
        and event.processingBy = :owner
      """)
  int markRetryPending(
      @Param("eventId") String eventId,
      @Param("owner") String owner,
      @Param("retryPending") InboxStatus retryPending,
      @Param("processing") InboxStatus processing,
      @Param("attemptCount") int attemptCount,
      @Param("failedAt") Instant failedAt,
      @Param("availableAt") Instant availableAt,
      @Param("lastError") String lastError
  );

  @Modifying
  @Query("""
      update InboxEventEntity event
      set event.status = :failed,
          event.attemptCount = :attemptCount,
          event.failedAt = :failedAt,
          event.availableAt = null,
          event.lastError = :lastError,
          event.updatedAt = :failedAt,
          event.processingAt = null,
          event.processingBy = null,
          event.processedAt = null,
          event.version = event.version + 1
      where event.eventId = :eventId
        and event.status = :processing
        and event.processingBy = :owner
      """)
  int markFailed(
      @Param("eventId") String eventId,
      @Param("owner") String owner,
      @Param("failed") InboxStatus failed,
      @Param("processing") InboxStatus processing,
      @Param("attemptCount") int attemptCount,
      @Param("failedAt") Instant failedAt,
      @Param("lastError") String lastError
  );
}
