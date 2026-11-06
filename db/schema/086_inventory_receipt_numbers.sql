-- 086 — Materials Receipt numbers for inventory pushes.
--
-- An inventory push used to write the WHOLE item list into the job expense's
-- description ("Inventory materials — 12 WHITE/BLACK THHN ×30.8lbs, 10 BLUE
-- THHN …" — 70 items in one cell). The owner wants the expense to just point
-- at the Materials Receipt PDF, which already carries every line. That needs a
-- number both can print: "Inventory materials — Receipt #1001 (68 items)".
--
-- ⚠ KEYED ON push_id, the push's existing idempotency key. A retried push
-- (double tap, timeout that landed) carries the same push_id, so it gets the
-- SAME receipt number back rather than a second one — the upsert below is the
-- whole mechanism. Numbers can have gaps (a push minted but refused by a
-- later guard); they never repeat.
CREATE TABLE IF NOT EXISTS inventory_receipts (
  push_id     text PRIMARY KEY,
  receipt_no  bigint GENERATED ALWAYS AS IDENTITY (START WITH 1001) UNIQUE,
  job_name    text,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- Mint-or-read, used by inventory.js:
--   INSERT INTO inventory_receipts (push_id, job_name) VALUES ($1, $2)
--   ON CONFLICT (push_id) DO UPDATE SET push_id = EXCLUDED.push_id
--   RETURNING receipt_no;
