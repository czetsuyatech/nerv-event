package com.czetsuyatech.nerv.event.persistence.persistence.repository;

import com.czetsuyatech.nerv.event.persistence.persistence.entity.TraceContextEntity;
import java.util.Collection;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/**
 * Spring Data persistence operations for trace-context sidecar rows.
 */
public interface TraceContextEntityRepository extends JpaRepository<TraceContextEntity, String> {

  @Query("""
      select context.eventId
      from TraceContextEntity context
      where not exists (
        select 1 from OutboxEventEntity outbox where outbox.eventId = context.eventId
      )
        and not exists (
        select 1 from InboxEventEntity inbox where inbox.eventId = context.eventId
      )
      order by context.eventId asc
      """)
  List<String> findUnreferencedEventIds(Pageable pageable);

  @Modifying
  @Query("""
      delete from TraceContextEntity context
      where context.eventId in :eventIds
        and not exists (
          select 1 from OutboxEventEntity outbox where outbox.eventId = context.eventId
        )
        and not exists (
          select 1 from InboxEventEntity inbox where inbox.eventId = context.eventId
        )
      """)
  int deleteUnreferencedByEventIds(@Param("eventIds") Collection<String> eventIds);
}
