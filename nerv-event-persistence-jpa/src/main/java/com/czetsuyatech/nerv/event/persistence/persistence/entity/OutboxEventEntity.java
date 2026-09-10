package com.czetsuyatech.nerv.event.persistence.persistence.entity;

import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * JPA representation of a durable outbox delivery.
 */
@Entity
@Getter
@Setter
@NoArgsConstructor
@Table(
    name = "nerv_outbox_event",
    indexes = {
        @Index(name = "idx_nerv_outbox_status_available", columnList = "status,available_at"),
        @Index(name = "idx_nerv_outbox_status_locked", columnList = "status,locked_at"),
        @Index(name = "idx_nerv_outbox_status_published", columnList = "status,published_at"),
        @Index(name = "idx_nerv_outbox_event_id", columnList = "event_id"),
        @Index(name = "idx_nerv_outbox_created_at", columnList = "created_at"),
        @Index(name = "idx_nerv_outbox_status_updated", columnList = "status,updated_at,id")
    })
public class OutboxEventEntity {

  @Id
  @Column(name = "id", nullable = false, updatable = false, length = 128)
  private String id;

  @Column(name = "event_id", nullable = false, length = 256)
  private String eventId;

  @Column(name = "event_type", nullable = false, length = 256)
  private String eventType;

  @Column(name = "source", nullable = false, length = 512)
  private String source;

  @Column(name = "correlation_id", length = 256)
  private String correlationId;

  @Column(name = "event_timestamp", nullable = false)
  private Instant eventTimestamp;

  @Column(name = "destination", nullable = false, length = 512)
  private String destination;

  /**
   * PostgreSQL TEXT keeps serialized payloads readable and avoids Large Object/OID storage.
   */
  @Column(name = "payload", nullable = false, columnDefinition = "text")
  private String payload;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false, length = 32)
  private OutboxStatus status;

  @Column(name = "attempt_count", nullable = false)
  private int attemptCount;

  @Column(name = "available_at", nullable = false)
  private Instant availableAt;

  @Column(name = "locked_at")
  private Instant lockedAt;

  @Column(name = "locked_by", length = 128)
  private String lockedBy;

  @Column(name = "claim_version", nullable = false)
  private long claimVersion;

  @Column(name = "last_error", length = 2048)
  private String lastError;

  @Column(name = "created_at", nullable = false, updatable = false)
  private Instant createdAt;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;

  @Column(name = "published_at")
  private Instant publishedAt;

  @Version
  @Setter(AccessLevel.NONE)
  @Column(name = "version", nullable = false)
  private long version;
}
