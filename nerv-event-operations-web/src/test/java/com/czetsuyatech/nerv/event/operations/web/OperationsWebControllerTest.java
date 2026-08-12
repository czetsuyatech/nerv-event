package com.czetsuyatech.nerv.event.operations.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.czetsuyatech.nerv.event.core.inbox.InboxStatus;
import com.czetsuyatech.nerv.event.core.outbox.OutboxId;
import com.czetsuyatech.nerv.event.core.outbox.OutboxStatus;
import com.czetsuyatech.nerv.event.model.EventId;
import com.czetsuyatech.nerv.event.application.dto.InboxEventView;
import com.czetsuyatech.nerv.event.services.InboxOperationService;
import com.czetsuyatech.nerv.event.application.dto.InboxSearchResult;
import com.czetsuyatech.nerv.event.exception.ManualRetryRejectedException;
import com.czetsuyatech.nerv.event.application.dto.OutboxEventView;
import com.czetsuyatech.nerv.event.services.OutboxOperationService;
import com.czetsuyatech.nerv.event.application.dto.OutboxSearchResult;
import com.czetsuyatech.nerv.event.web.controller.InboxOperationController;
import com.czetsuyatech.nerv.event.web.controller.OutboxOperationController;
import com.czetsuyatech.nerv.event.web.advice.OperationWebExceptionHandler;
import com.czetsuyatech.nerv.event.autoconfigure.NervEventOperationWebProperties;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class OperationsWebControllerTest {

  private final OutboxOperationService outbox = org.mockito.Mockito.mock(OutboxOperationService.class);
  private final InboxOperationService inbox = org.mockito.Mockito.mock(InboxOperationService.class);
  private MockMvc mvc;
  private NervEventOperationWebProperties properties;

  @BeforeEach
  void setUp() {
    properties = new NervEventOperationWebProperties();
    mvc = MockMvcBuilders.standaloneSetup(
        new OutboxOperationController(
            outbox,
            properties
        ),
        new InboxOperationController(
            inbox,
            properties
        )
    )
        .setControllerAdvice(new OperationWebExceptionHandler())
        .addPlaceholderValue(
            "nerv.event.operations.web.base-path",
            "/management/nerv-event"
        )
        .build();
  }

  @Test
  void failedOutboxRetryIsAcceptedWithoutChangingItsIdentifiers() throws Exception {
    OutboxId id = new OutboxId("outbox-1");
    when(outbox.find(id)).thenReturn(Optional.of(outboxView(OutboxStatus.FAILED)));
    when(outbox.retryFailed(id)).thenReturn(outboxView(OutboxStatus.PENDING));

    mvc.perform(post("/management/nerv-event/outbox/outbox-1/retry"))
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.id").value("outbox-1"))
        .andExpect(jsonPath("$.eventId").value("event-1"))
        .andExpect(jsonPath("$.previousStatus").value("FAILED"))
        .andExpect(jsonPath("$.status").value("PENDING"))
        .andExpect(jsonPath("$.payload").doesNotExist());
    verify(outbox).retryFailed(id);
  }

  @Test
  void rejectedOutboxRetryAndUnknownOutboxMapToConflictAndNotFound() throws Exception {
    OutboxId published = new OutboxId("published");
    when(outbox.find(published)).thenReturn(Optional.of(outboxView(OutboxStatus.PUBLISHED)));
    when(outbox.retryFailed(published)).thenThrow(
        new ManualRetryRejectedException(
            "outbox",
            "published",
            "current status is PUBLISHED"
        )
    );
    when(outbox.find(new OutboxId("unknown"))).thenReturn(Optional.empty());

    mvc.perform(post("/management/nerv-event/outbox/published/retry"))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.code").value("NERV_EVENT_RETRY_CONFLICT"));
    mvc.perform(post("/management/nerv-event/outbox/unknown/retry"))
        .andExpect(status().isNotFound())
        .andExpect(jsonPath("$.code").value("NERV_EVENT_NOT_FOUND"));
  }

  @Test
  void failedInboxRetryIsAcceptedAndPayloadPolicyAppliesOnlyToDetail() throws Exception {
    EventId id = new EventId("event-1");
    when(inbox.find(id)).thenReturn(Optional.of(inboxView(InboxStatus.FAILED)));
    when(inbox.retryFailed(id)).thenReturn(inboxView(InboxStatus.RETRY_PENDING));
    when(inbox.search(any())).thenReturn(
        new InboxSearchResult(
            List.of(inboxView(InboxStatus.FAILED)),
            0,
            20,
            1,
            1
        )
    );

    mvc.perform(post("/management/nerv-event/inbox/event-1/retry"))
        .andExpect(status().isAccepted())
        .andExpect(jsonPath("$.eventId").value("event-1"))
        .andExpect(jsonPath("$.previousStatus").value("FAILED"))
        .andExpect(jsonPath("$.status").value("RETRY_PENDING"))
        .andExpect(jsonPath("$.payload").doesNotExist());
    mvc.perform(get("/management/nerv-event/inbox/event-1"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.payload").doesNotExist());
    mvc.perform(get("/management/nerv-event/inbox"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.items[0].payload").doesNotExist());

    properties.getPayload().setEnabled(true);
    mvc.perform(get("/management/nerv-event/inbox/event-1"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.payload").value("{\"sensitive\":true}"))
        .andExpect(jsonPath("$.contentType").value("application/json"));
  }

  @Test
  void searchUsesBoundedOperationsQueriesAndRejectsInvalidInput() throws Exception {
    when(outbox.search(any())).thenReturn(
        new OutboxSearchResult(
            List.of(outboxView(OutboxStatus.FAILED)),
            0,
            20,
            1,
            1
        )
    );

    mvc.perform(
        get("/management/nerv-event/outbox")
            .param(
                "status",
                "FAILED"
            )
            .param(
                "eventId",
                "event-1"
            )
            .param(
                "destination",
                "orders"
            )
    )
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.items[0].eventId").value("event-1"))
        .andExpect(jsonPath("$.items[0].payload").doesNotExist())
        .andExpect(jsonPath("$.size").value(20));
    mvc.perform(
        get("/management/nerv-event/outbox").param(
            "size",
            "101"
        )
    )
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("NERV_EVENT_INVALID_QUERY"));
    mvc.perform(
        get("/management/nerv-event/outbox").param(
            "updatedFrom",
            "not-a-date"
        )
    )
        .andExpect(status().isBadRequest());
  }

  private static OutboxEventView outboxView(OutboxStatus status) {
    Instant now = Instant.parse("2026-08-19T00:00:00Z");
    return new OutboxEventView(
        new OutboxId("outbox-1"),
        new EventId("event-1"),
        "OrderCreated",
        now,
        "orders",
        "correlation-1",
        "orders",
        status,
        2,
        now,
        null,
        null,
        "broker unavailable",
        now,
        now,
        null,
        "{\"sensitive\":true}"
    );
  }

  private static InboxEventView inboxView(InboxStatus status) {
    Instant now = Instant.parse("2026-08-19T00:00:00Z");
    return new InboxEventView(
        new EventId("event-1"),
        "OrderCreated",
        now,
        "orders",
        "correlation-1",
        status,
        2,
        now,
        null,
        null,
        null,
        null,
        now,
        "handler unavailable",
        now,
        now,
        "application/json",
        "{\"sensitive\":true}"
    );
  }
}
