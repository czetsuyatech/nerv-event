package com.czetsuyatech.nerv.event.services;

import com.czetsuyatech.nerv.event.application.dto.InboxEventView;
import com.czetsuyatech.nerv.event.application.dto.InboxQuery;
import com.czetsuyatech.nerv.event.application.dto.InboxSearchResult;
import com.czetsuyatech.nerv.event.model.EventId;
import java.util.Optional;

/**
 * <p>
 * Operator-facing inspection and recovery API for durable inbox rows.
 * </p>
 *
 * <p>
 * Manual retry only permits {@code FAILED -> RETRY_PENDING}. The normal {@code InboxRetryDispatcher} claims and
 * executes the row later; this API never invokes an event handler directly and never creates a second inbox record.
 * </p>
 */
public interface InboxOperationService {

  Optional<InboxEventView> find(EventId eventId);

  InboxSearchResult search(InboxQuery query);

  InboxEventView retryFailed(EventId eventId);
}
