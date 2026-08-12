-- 038_expense_pushes.sql
-- Slice 2 of the inventory Airtable-write cutover (docs/PLAN-inventory-airtable-cutover.md).
-- The push HISTORY — what was charged, to which job, from which transactions.
--
-- 34 headers and 415 lines today. Small, but it is the audit trail for every
-- dollar the inventory app has ever put on a job, which is why it moves rather
-- than being dropped.

CREATE TABLE IF NOT EXISTS expense_pushes (
  id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  -- NULL on every row written after this slice. Kept for the 34 historical rows.
  airtable_id        text UNIQUE,
  -- The client-generated idempotency key for one push of one job. The SAME key
  -- is stamped on the expenses in the main base and on inventory_transactions,
  -- and it is what guard #1 matches on retry — so it is the anchor that ties all
  -- three together. UNIQUE here means a retried push cannot mint a second
  -- history row for a charge that only happened once.
  push_id            text,
  title              text,
  pushed_at          timestamptz,
  pushed_by          text,
  job_name           text,
  -- Record id of the job in the MAIN base (text, not a link — Drop-Jobs-mirror).
  job_airtable_id    text,
  materials_total    numeric(14,4),
  tax_total          numeric(14,4),
  -- Stored rather than generated: the historical rows carry Airtable's own
  -- figure, and a generated column would silently overwrite it with a
  -- recomputation. Money keeps the number that was actually charged.
  total_pushed       numeric(14,4),
  tx_count           integer,
  item_count         integer,
  taxable            boolean NOT NULL DEFAULT false,
  -- Comma-separated main-base Expense record ids created by this push.
  expense_record_ids text,
  description        text,
  synced_at          timestamptz
);

-- Partial, because the 34 historical rows predate the key and carry NULL.
CREATE UNIQUE INDEX IF NOT EXISTS exp_push_pushid_idx
    ON expense_pushes (push_id) WHERE push_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS exp_push_date_idx ON expense_pushes (pushed_at DESC);
CREATE INDEX IF NOT EXISTS exp_push_job_idx
    ON expense_pushes (job_airtable_id) WHERE job_airtable_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS expense_push_lines (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id      text UNIQUE,
  -- Both forms of the parent handle. The text one is how the loader resolves a
  -- historical row; the uuid is the real FK and the only one native rows have.
  push_airtable_id text,
  expense_push_id  uuid REFERENCES expense_pushes(id) ON DELETE CASCADE,
  line_title       text,
  item_name        text,
  quantity         numeric(14,4),
  unit_cost        numeric(14,4),
  line_total       numeric(14,4),
  -- Wire items only: pounds are what stock moves in, feet are what people say.
  wire_ft          numeric(14,4),
  synced_at        timestamptz
);

CREATE INDEX IF NOT EXISTS exp_push_line_parent_idx ON expense_push_lines (expense_push_id);

COMMENT ON TABLE expense_pushes IS
  'One row per job pushed to the main base as expenses. The audit trail behind every dollar the inventory app has charged to a job. push_id ties it to the main-base Expenses and to inventory_transactions.push_id.';
COMMENT ON COLUMN expense_pushes.airtable_id IS
  'Airtable rec id for the 34 rows that predate the slice-2 cutover (2026-08-12). NULL on every row born in Postgres — id (uuid) is the handle the app uses.';
