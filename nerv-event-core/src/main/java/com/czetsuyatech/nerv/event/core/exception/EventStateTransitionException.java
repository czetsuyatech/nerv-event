package com.czetsuyatech.nerv.event.core.exception;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.exception.NervEventException;
import com.czetsuyatech.nerv.event.model.EventId;

/**
 * Indicates that a guarded Inbox or Outbox state transition was rejected.
 */
public final class EventStateTransitionException extends NervEventException {

  public EventStateTransitionException(
      EventId eventId,
      InboxStatus expectedStatus,
      InboxStatus requestedStatus,
      String owner
  )
  {
    super(
        "Inbox state transition rejected eventId=" + eventId.value()
            + " expectedStatus=" + expectedStatus
            + " requestedStatus=" + requestedStatus
            + " owner=" + owner
    );
  }

  public EventStateTransitionException(
      OutboxId outboxId,
      OutboxStatus expectedStatus,
      OutboxStatus requestedStatus,
      String owner
  )
  {
    super(
        "Outbox state transition rejected outboxId=" + outboxId.value()
            + " expectedStatus=" + expectedStatus
            + " requestedStatus=" + requestedStatus
            + " owner=" + owner
    );
  }
}
