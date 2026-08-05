-- ── Crew Schedule → Neon (migration Step 4a) ───────────────────────────────
-- Added 2026-08-05. Owner's chosen first Step-4 slice.
--
-- WHY THIS WENT FIRST. It was missing from docs/ROADMAP.md entirely — a top-bar
-- tab and five backend handlers with no place in the running order — and it turned
-- out to be the smallest domain in the app: one table, 7 fields, 64 rows, no money,
-- no formulas, no rollups, and no Make involvement. Both of its links (Job and
-- Employee) were ALREADY in Neon, so the foreign keys resolved on day one. That
-- makes it the cheapest possible place to prove the mirror → read-flip → write-flip
-- pattern before using it on Fleet (4b) and eventually Expenses (4d).
--
-- It also removes a genuinely silly read: the Airtable path pages THREE whole
-- tables on every calendar load — Schedule Entries, Jobs and Employees — purely to
-- resolve names for the grid. Here it is one query with two joins.
CREATE TABLE IF NOT EXISTS schedule_entries (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  -- Nullable BY DESIGN. Entries created after the write flip have no Airtable twin
  -- until the mirror succeeds, and none at all once the Airtable table is retired.
  airtable_id  text UNIQUE,
  title        text,
  entry_type   text NOT NULL DEFAULT 'Job',
  job_id       uuid REFERENCES jobs(id),
  start_date   date,
  end_date     date,
  notes        text,
  -- 'airtable' for rows the ETL backfilled, 'app' for rows born here. There is no
  -- third writer — no Make scenario, no puller — so unlike time_entries this needs
  -- no CHECK forcing an upstream key.
  source       text NOT NULL DEFAULT 'app',
  created_at   timestamptz NOT NULL DEFAULT now(),
  synced_at    timestamptz
);

-- Crew is a real many-to-many, not an array column. Arrays cannot carry a foreign
-- key, and Step 4c (generators + their service history) needs proper relations —
-- this is the slice where that pattern gets proven cheaply.
--
-- ON DELETE CASCADE matters: deleting an entry must not strand crew rows, and the
-- delete handler deliberately does NOT clean them up itself.
CREATE TABLE IF NOT EXISTS schedule_entry_crew (
  schedule_entry_id uuid NOT NULL REFERENCES schedule_entries(id) ON DELETE CASCADE,
  employee_id       uuid NOT NULL REFERENCES employees(id),
  PRIMARY KEY (schedule_entry_id, employee_id)
);

CREATE INDEX IF NOT EXISTS schedule_entries_dates_idx ON schedule_entries (start_date, end_date);

-- ── Notes on behaviour that differs from the Airtable path ─────────────────
-- 1. `id` is the Neon uuid, not the Airtable rec id. Safe here in a way it was not
--    for jobs: nothing else in the base references a schedule entry, and the app is
--    its only writer. Handlers still resolve a `rec…` id because the Airtable read
--    fallback returns those.
-- 2. Crew comes back ordered by NAME rather than Airtable's insertion order. The
--    grid renders them as an unordered set of pills, so this is deterministic
--    rather than arbitrary.
-- 3. Deletes are NOT tombstoned, unlike time entries. A schedule entry is a plan,
--    not a financial record — losing one costs a re-drag, not someone's pay.
--
-- Backfill + acceptance checks: db/etl/schedule-entries.mjs
-- First load 2026-08-05: 64 entries, 147 crew links, 59 with a job — all matching.
-- Read flip verified against production by diffing both paths across four filter
-- scenarios (none / wide window / narrow window / by job): all identical.
