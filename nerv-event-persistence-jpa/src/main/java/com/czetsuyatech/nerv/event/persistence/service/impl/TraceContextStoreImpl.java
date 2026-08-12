package com.czetsuyatech.nerv.event.persistence.service.impl;

import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.observability.tracing.TraceContextStore;
import com.czetsuyatech.nerv.event.persistence.persistence.entity.TraceContextEntity;
import com.czetsuyatech.nerv.event.persistence.persistence.repository.TraceContextEntityRepository;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

/**
 * Transactional EventId sidecar. Different duplicate contexts retain the original context.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class TraceContextStoreImpl implements TraceContextStore {

  private static final TypeReference<Map<String, String>> MAP = new TypeReference<>() {
  };

  private final TraceContextEntityRepository entityRepository;
  private final ObjectMapper objectMapper;
  private final Clock clock;

  @Override
  @Transactional(propagation = Propagation.MANDATORY)
  public void save(
      EventId eventId,
      Map<String, String> fields
  ) {
    String json = write(fields);
    TraceContextEntity existing = entityRepository.findById(eventId.value()).orElse(null);
    if (existing == null) {
      entityRepository.save(
          new TraceContextEntity(
              eventId.value(),
              json,
              clock.instant()
          )
      );
      return;
    }
    if (!existing.getContextJson().equals(json)) {
      log.warn(
          "Retaining original trace context for duplicate eventId={}",
          eventId.value()
      );
    }
  }

  @Override
  @Transactional(readOnly = true, propagation = Propagation.SUPPORTS)
  public Optional<Map<String, String>> find(EventId eventId) {
    return entityRepository.findById(eventId.value()).map(row -> read(row.getContextJson()));
  }

  private String write(Map<String, String> fields) {
    try {
      return objectMapper.writeValueAsString(Map.copyOf(fields));
    } catch (Exception e) {
      throw new IllegalStateException(
          "Cannot serialize trace propagation fields",
          e
      );
    }
  }

  private Map<String, String> read(String json) {
    try {
      return Map.copyOf(
          objectMapper.readValue(
              json,
              MAP
          )
      );
    } catch (Exception e) {
      log.debug(
          "Ignoring unreadable trace context sidecar",
          e
      );
      return Map.of();
    }
  }
}
