package com.czetsuyatech.nerv.event.persistence.persistence.entity;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import jakarta.persistence.Version;
import java.time.Instant;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.domain.Persistable;

/**
 * JPA representation of one durable inbound event, unique by logical event id.
 */
@Entity
@Getter
@Setter
@NoArgsConstructor
@Table(
    name = "nerv_inbox_event",
    indexes = {
        @Index(name = "idx_nerv_inbox_status_available", columnList = "status,available_at"),
        @Index(name = "idx_nerv_inbox_status_processing", columnList = "status,processing_at"),
        @Index(name = "idx_nerv_inbox_status_processed", columnList = "status,processed_at"),
        @Index(name = "idx_nerv_inbox_received_at", columnList = "received_at"),
        @Index(name = "idx_nerv_inbox_event_type", columnList = "event_type"),
        @Index(name = "idx_nerv_inbox_status_updated", columnList = "status,updated_at,event_id")
    })
public class InboxEventEntity implements Persistable<String> {

  @Transient
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  private boolean newEntity = true;

  @Id
  @Column(name = "event_id", nullable = false, updatable = false, length = 256)
  private String eventId;

  @Column(name = "event_type", nullable = false, length = 256)
  private String eventType;

  @Column(name = "event_timestamp", nullable = false)
  private Instant eventTimestamp;

  @Column(name = "source", nullable = false, length = 512)
  private String source;

  @Column(name = "correlation_id", length = 256)
  private String correlationId;

  /**
   * PostgreSQL TEXT keeps serialized payloads readable and avoids Large Object/OID storage.
   */
  @Column(name = "payload", nullable = false, columnDefinition = "text")
  private String payload;

  @Column(name = "content_type", nullable = false, length = 128)
  private String contentType;

  @Enumerated(EnumType.STRING)
  @Column(name = "status", nullable = false, length = 32)
  private InboxStatus status;

  @Column(name = "attempt_count", nullable = false)
  private int attemptCount;

  @Column(name = "received_at", nullable = false, updatable = false)
  private Instant receivedAt;

  @Column(name = "available_at")
  private Instant availableAt;

  @Column(name = "processing_at")
  private Instant processingAt;

  @Column(name = "processing_by", length = 128)
  private String processingBy;

  @Column(name = "processed_at")
  private Instant processedAt;

  @Column(name = "failed_at")
  private Instant failedAt;

  @Column(name = "last_error", length = 2048)
  private String lastError;

  @Column(name = "created_at", nullable = false, updatable = false)
  private Instant createdAt;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;

  @Version
  @Setter(AccessLevel.NONE)
  @Column(name = "version", nullable = false)
  private long version;

  @Override
  public String getId() {
    return eventId;
  }

  @Override
  public boolean isNew() {
    return newEntity;
  }

  @PostLoad
  @PostPersist
  void markNotNew() {
    newEntity = false;
  }
}
