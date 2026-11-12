-- 093: a real "last edited" on material estimates (owner 2026-11-12, estimates list).
--
-- synced_at LOOKS like one — estimateUpdate stamps it on every Save — but every
-- Airtable-era estimate carries the ETL batch time (2026-08-12 04:50) there, so
-- the list would say "Edited Aug 12" on 16 estimates nobody touched that day.
-- updated_at is stamped ONLY by estimateUpdate. Backfilled from synced_at for
-- app-born rows (airtable_id IS NULL), where synced_at IS the last Save;
-- legacy rows stay NULL = "unknown", and the list shows just the created date.

ALTER TABLE material_estimates ADD COLUMN IF NOT EXISTS updated_at timestamptz;

UPDATE material_estimates SET updated_at = synced_at
 WHERE airtable_id IS NULL AND updated_at IS NULL;
