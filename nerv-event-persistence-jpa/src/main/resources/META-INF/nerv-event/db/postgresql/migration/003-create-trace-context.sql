-- EventId-keyed tracing sidecar. It intentionally has no foreign keys to Outbox or Inbox.
create table nerv_event_trace_context (
  event_id varchar(256) not null,
  context_json text not null,
  created_at timestamp with time zone not null,
  constraint pk_nerv_event_trace_context primary key (event_id)
);
