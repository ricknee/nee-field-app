-- LOAD CALCS — All Charts tab 7 calculations saved on a job.
-- APPLIED to the default branch of Neon project damp-silence-99074350 on 2026-09-21.
--
-- Same convention as the other files here: applied BARE via the Neon MCP
-- (which mangles inline SQL comments), with the reasoning kept here.
--
-- ── WHY A JSONB BLOB AND NOT ROWS PER LOAD ─────────────────────────────────
-- A load calculation is a DOCUMENT the Load Calc tab edits as a whole — service,
-- floor area, 30 load lines, the generator inputs — and it is only ever read
-- back whole, by the same screen. Nothing queries one load across jobs. Rows per
-- load would be a second schema to keep in step with the calculator's line shape
-- (which gained an EV type and a per-phase unit in its first day) for no reader.
-- The calculation itself is NEVER stored: it is recomputed from `data` every
-- time, so a fix to a rule reaches every saved calc on next open.
--
-- ── KEYED ON jobs.id (uuid), NOT NULL ──────────────────────────────────────
-- Unlike panel_schedules (007), which predates native jobs and so carries the
-- Airtable rec id: every job now exists in Neon at the moment it is created
-- (identity cutover, JOB_CREATE_SOURCE=native), so a real FK is safe. The
-- handler resolves whatever id the client holds — rec… or uuid — through
-- `airtable_id = $1 OR id::text = $1`.

CREATE TABLE load_calcs (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id      uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  name        text NOT NULL,
  data        jsonb NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  -- A NAME, not a link — same reasoning as panel_schedules.updated_by.
  updated_by  text
);

CREATE INDEX load_calcs_job ON load_calcs (job_id);
