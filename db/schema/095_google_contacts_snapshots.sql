-- 095 — a copy of rick@'s Google contacts, taken before the sync writes.
-- Owner 2026-11-15: "keep a copy of my current contacts before the edit gets
-- pushed". A live sync run refuses to write to Google unless a snapshot from
-- the last 20 hours exists (it takes one if not). `data` holds every contact
-- with exactly the fields the sync can change (names, phones, emails,
-- organizations, addresses, label memberships) plus the label list — enough
-- to put back anything the sync touched. The newest 30 are kept.
CREATE TABLE IF NOT EXISTS google_contacts_snapshots (
  id            bigserial PRIMARY KEY,
  account       text        NOT NULL,
  taken_at      timestamptz NOT NULL DEFAULT now(),
  reason        text,
  contact_count integer     NOT NULL,
  group_count   integer     NOT NULL,
  data          jsonb       NOT NULL
);
CREATE INDEX IF NOT EXISTS google_contacts_snapshots_account_taken
  ON google_contacts_snapshots (account, taken_at DESC);
