package com.czetsuyatech.nerv.event.persistence.persistence.repository;

import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import java.time.Instant;
import java.util.List;
import jakarta.persistence.LockModeType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Spring Data repository for ordinary outbox persistence and state changes.
 */
public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, String> {

  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("""
      select event from OutboxEventEntity event
      where (event.status = :pending and event.availableAt <= :claimedAt)
         or (event.status = :processing and event.lockedAt <= :expiredLeaseAt)
      order by event.createdAt asc
      """)
  List<OutboxEventEntity> findClaimableForUpdate(
      @Param("pending") OutboxStatus pending,
      @Param("processing") OutboxStatus processing,
      @Param("claimedAt") Instant claimedAt,
      @Param("expiredLeaseAt") Instant expiredLeaseAt,
      Pageable pageable
  );

  long countByStatus(OutboxStatus status);

  @Modifying
  @Query(value = """
      delete from nerv_outbox_event
      where id in (
        select id from (
          select id
          from nerv_outbox_event
          where status = 'PUBLISHED' and published_at <= :cutoff
          order by published_at asc, id asc
          limit :limit
        ) retention_candidates
      )
        and status = 'PUBLISHED'
        and published_at <= :cutoff
      """, nativeQuery = true)
  int deletePublishedBefore(
      @Param("cutoff") Instant cutoff,
      @Param("limit") int limit
  );

  @Query("select min(event.createdAt) from OutboxEventEntity event where event.status = :status")
  Instant oldestCreatedAtByStatus(@Param("status") OutboxStatus status);

  @Modifying
  @Query("""
      update OutboxEventEntity event
      set event.status = :published,
          event.publishedAt = :publishedAt,
          event.updatedAt = :updatedAt,
          event.lockedAt = null,
          event.lockedBy = null,
          event.lastError = null,
          event.version = event.version + 1
      where event.id = :id
        and event.status = :processing
        and event.lockedBy = :owner
      """)
  int markPublished(
      @Param("id") String id,
      @Param("owner") String owner,
      @Param("published") OutboxStatus published,
      @Param("processing") OutboxStatus processing,
      @Param("publishedAt") Instant publishedAt,
      @Param("updatedAt") Instant updatedAt
  );

  @Modifying
  @Query("""
      update OutboxEventEntity event
      set event.status = :pending,
          event.attemptCount = :attemptCount,
          event.availableAt = :availableAt,
          event.updatedAt = :updatedAt,
          event.lockedAt = null,
          event.lockedBy = null,
          event.lastError = :lastError,
          event.version = event.version + 1
      where event.id = :id
        and event.status = :processing
        and event.lockedBy = :owner
      """)
  int reschedule(
      @Param("id") String id,
      @Param("owner") String owner,
      @Param("pending") OutboxStatus pending,
      @Param("processing") OutboxStatus processing,
      @Param("attemptCount") int attemptCount,
      @Param("availableAt") Instant availableAt,
      @Param("updatedAt") Instant updatedAt,
      @Param("lastError") String lastError
  );

  @Modifying
  @Query("""
      update OutboxEventEntity event
      set event.status = :failed,
          event.attemptCount = :attemptCount,
          event.updatedAt = :updatedAt,
          event.lockedAt = null,
          event.lockedBy = null,
          event.lastError = :lastError,
          event.version = event.version + 1
      where event.id = :id
        and event.status = :processing
        and event.lockedBy = :owner
      """)
  int markFailed(
      @Param("id") String id,
      @Param("owner") String owner,
      @Param("failed") OutboxStatus failed,
      @Param("processing") OutboxStatus processing,
      @Param("attemptCount") int attemptCount,
      @Param("updatedAt") Instant updatedAt,
      @Param("lastError") String lastError
  );
}
