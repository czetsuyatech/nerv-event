package com.czetsuyatech.nerv.event.observability.tracing;

import com.czetsuyatech.nerv.event.model.EventId;
import java.util.Map;
import java.util.Optional;

/**
 * Persistence-side storage for configured tracing carrier fields, keyed by logical event identity.
 */
public interface TraceContextStore {

  void save(
      EventId eventId,
      Map<String, String> propagationFields
  );

  Optional<Map<String, String>> find(EventId eventId);
}
