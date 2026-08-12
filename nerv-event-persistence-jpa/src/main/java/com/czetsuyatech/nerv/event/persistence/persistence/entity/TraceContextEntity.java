package com.czetsuyatech.nerv.event.persistence.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.domain.Persistable;

/**
 * Sidecar infrastructure metadata for a logical event; it deliberately has no payload fields.
 */
@Entity
@Getter
@Setter
@NoArgsConstructor
@Table(name = "nerv_event_trace_context")
public class TraceContextEntity implements Persistable<String> {

  @Transient
  private boolean newEntity = true;

  @Id
  @Column(name = "event_id", nullable = false, updatable = false, length = 256)
  private String eventId;

  @Column(name = "context_json", nullable = false, columnDefinition = "text")
  private String contextJson;

  @Column(name = "created_at", nullable = false, updatable = false)
  private Instant createdAt;

  public TraceContextEntity(
      String eventId,
      String contextJson,
      Instant createdAt
  )
  {
    this.eventId = eventId;
    this.contextJson = contextJson;
    this.createdAt = createdAt;
  }

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
