package com.czetsuyatech.nerv.event.persistence.application.mapper;

import com.czetsuyatech.nerv.event.core.inbox.InboxEvent;
import com.czetsuyatech.nerv.event.core.serialization.SerializedPayload;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.InboxEventEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants.ComponentModel;
import org.mapstruct.ReportingPolicy;

/**
 * Structural MapStruct mappings for the broker-neutral inbox model.
 */
@Mapper(componentModel = ComponentModel.SPRING, unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface InboxEventMapper {

  @Mapping(target = "eventId", source = "eventId")
  @Mapping(target = "eventTimestamp", source = "timestamp")
  @Mapping(target = "payload", source = "payload.value")
  @Mapping(target = "contentType", source = "payload.contentType")
  @Mapping(target = "createdAt", source = "receivedAt")
  @Mapping(target = "updatedAt", source = "receivedAt")
  @Mapping(target = "version", ignore = true)
  InboxEventEntity toJpa(InboxEvent event);

  @Mapping(target = "timestamp", source = "eventTimestamp")
  @Mapping(target = "payload", expression = "java(toSerializedPayload(entity))")
  InboxEvent toCore(InboxEventEntity entity);

  default String toString(EventId eventId) {
    return eventId == null ? null : eventId.value();
  }

  default EventId toEventId(String eventId) {
    return eventId == null ? null : new EventId(eventId);
  }

  default SerializedPayload toSerializedPayload(InboxEventEntity entity) {
    return new SerializedPayload(
        entity.getPayload(),
        entity.getContentType()
    );
  }
}
