-- Neon slice — soft-delete for clock punches.
--
-- Applied BARE via the Neon MCP; this file is the annotated source of truth.
--
-- A punch can be flat-out wrong: clocked in by mistake, clocked in twice on two
-- devices, started against a job the person never went to. Editing the times can't
-- fix "this shift should not exist".
--
-- ── SOFT, NOT HARD ───────────────────────────────────────────────────────────
-- Deleting is the one clock action with no audit trail of its own — an edit keeps
-- original_started_at, but a hard DELETE leaves nothing at all, which makes it the
-- obvious way to make an inconvenient shift disappear. So it is soft, matching how
-- job photos and prints already work in this app (a bin, not an incinerator).
--
-- Every read filters on `deleted_at IS NULL`. ⚠ A new query over clock_punches that
-- forgets that filter will resurrect deleted shifts — and if it is a payroll or
-- reconciliation query, it will do so as HOURS.
ALTER TABLE clock_punches ADD COLUMN IF NOT EXISTS deleted_at timestamptz;
ALTER TABLE clock_punches ADD COLUMN IF NOT EXISTS deleted_by uuid REFERENCES employees(id);

COMMENT ON COLUMN clock_punches.deleted_at IS
  'Soft delete. NULL = live. Every read of this table must filter deleted_at IS '
  'NULL; forgetting to do so resurrects the shift as payable hours. If the punch '
  'had been promoted, its time_entries row is hard-deleted at the same moment, in '
  'the same statement — payroll has no concept of a soft-deleted entry.';

-- Live punches are the overwhelming majority and every hot query filters on this,
-- so the index is partial and stays small.
CREATE INDEX IF NOT EXISTS clock_punches_live_idx
  ON clock_punches (employee_id, work_date DESC) WHERE deleted_at IS NULL;
