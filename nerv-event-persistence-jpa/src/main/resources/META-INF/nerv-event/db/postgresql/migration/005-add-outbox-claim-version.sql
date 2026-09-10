-- Fence stale outbox workers after lease ownership changes.
alter table nerv_outbox_event
  add column claim_version bigint not null default 0;

alter table nerv_outbox_event
  add constraint chk_nerv_outbox_claim_version_nonnegative check (claim_version >= 0);
