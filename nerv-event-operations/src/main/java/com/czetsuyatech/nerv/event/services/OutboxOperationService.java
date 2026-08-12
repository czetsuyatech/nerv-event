package com.czetsuyatech.nerv.event.services;

import com.czetsuyatech.nerv.event.application.dto.OutboxEventView;
import com.czetsuyatech.nerv.event.application.dto.OutboxQuery;
import com.czetsuyatech.nerv.event.application.dto.OutboxSearchResult;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import java.util.Optional;

/**
 * <p>
 * Operator-facing inspection and recovery API for durable outbox delivery rows.
 * </p>
 *
 * <p>
 * Manual retry only permits {@code FAILED -> PENDING}. It preserves the existing durable row, outbox id, event id,
 * payload, metadata, and attempt count; it never replays or clones an event.
 * </p>
 */
public interface OutboxOperationService {

  Optional<OutboxEventView> find(OutboxId outboxId);

  OutboxSearchResult search(OutboxQuery query);

  OutboxEventView retryFailed(OutboxId outboxId);
}
