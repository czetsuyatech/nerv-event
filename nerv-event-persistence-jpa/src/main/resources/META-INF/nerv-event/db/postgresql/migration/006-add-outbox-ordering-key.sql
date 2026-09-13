-- Preserve an optional logical ordering identity across Outbox retries.
alter table nerv_outbox_event
  add column ordering_key varchar(512);

create index idx_nerv_outbox_ordering_sequence
  on nerv_outbox_event (ordering_key, created_at, id)
  where ordering_key is not null and status in ('PENDING', 'PROCESSING');
