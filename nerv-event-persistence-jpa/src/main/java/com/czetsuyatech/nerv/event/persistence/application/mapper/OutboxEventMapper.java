package com.czetsuyatech.nerv.event.persistence.application.mapper;

import com.czetsuyatech.nerv.event.core.outbox.OutboxEvent;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.model.Destination;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.model.EventMessage;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.OutboxEventEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants.ComponentModel;
import org.mapstruct.ReportingPolicy;

/**
 * Structural MapStruct mappings between the core outbox envelope and its JPA representation.
 */
@Mapper(componentModel = ComponentModel.SPRING, unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface OutboxEventMapper {

  @Mapping(target = "id", source = "id")
  @Mapping(target = "eventId", source = "event.id")
  @Mapping(target = "eventType", source = "event.type")
  @Mapping(target = "source", source = "event.source")
  @Mapping(target = "correlationId", source = "event.correlationId")
  @Mapping(target = "eventTimestamp", source = "event.timestamp")
  @Mapping(target = "destination", source = "destination")
  @Mapping(target = "payload", ignore = true)
  @Mapping(target = "availableAt", source = "nextAttemptAt")
  @Mapping(target = "lockedAt", ignore = true)
  @Mapping(target = "lockedBy", ignore = true)
  @Mapping(target = "lastError", ignore = true)
  @Mapping(target = "createdAt", ignore = true)
  @Mapping(target = "updatedAt", ignore = true)
  @Mapping(target = "publishedAt", ignore = true)
  @Mapping(target = "version", ignore = true)
  OutboxEventEntity toJpa(OutboxEvent event);

  @Mapping(target = "id", source = "entity.id")
  @Mapping(target = "event", source = "event")
  @Mapping(target = "destination", source = "entity.destination")
  @Mapping(target = "attemptCount", source = "entity.attemptCount")
  @Mapping(target = "nextAttemptAt", source = "entity.availableAt")
  @Mapping(target = "status", source = "entity.status")
  OutboxEvent toCore(
      OutboxEventEntity entity,
      EventMessage<?> event
  );

  @Mapping(target = "id", source = "entity.eventId")
  @Mapping(target = "type", source = "entity.eventType")
  @Mapping(target = "timestamp", source = "entity.eventTimestamp")
  @Mapping(target = "source", source = "entity.source")
  @Mapping(target = "correlationId", source = "entity.correlationId")
  @Mapping(target = "payload", source = "payload")
  EventMessage<Object> toEventMessage(
      OutboxEventEntity entity,
      Object payload
  );

  default String toString(OutboxId id) {
    return id == null ? null : id.value();
  }

  default OutboxId toOutboxId(String id) {
    return id == null ? null : new OutboxId(id);
  }

  default String toString(EventId id) {
    return id == null ? null : id.value();
  }

  default EventId toEventId(String id) {
    return id == null ? null : new EventId(id);
  }

  default String toString(Destination destination) {
    return destination == null ? null : destination.name();
  }

  default Destination toDestination(String destination) {
    return destination == null ? null : new Destination(destination);
  }
}
