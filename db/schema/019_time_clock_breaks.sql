-- Neon slice — breaks/lunch on the time clock, and city tax on a punch.
--
-- Applied BARE via the Neon MCP (which mangles inline comments); this file is the
-- annotated source of truth. Same convention as 001/002/018.
--
-- Owner asked for both on first sight of the clock, 2026-08-08: "i want a Break
-- button which pauses time for lunch etc. and then i need to keep track of city
-- taxes." Breaks were explicitly out of scope in PLAN-time-clock.md §9, so this is a
-- genuine addition rather than a correction.
--
-- ── WHY BREAKS ARE STORED AS A RUNNING TOTAL, NOT AS INTERVAL ROWS ────────────
-- The obvious shape is a `punch_breaks` table with one row per break. It was not
-- worth it here: nothing in payroll, GP or labor cost asks "when was lunch", only
-- "how long was the shift". A running total plus the CURRENT break's start answers
-- every question the app actually has, in the row that is already being read, with
-- no join and no second write path to keep atomic with punch-out.
--
-- The cost is that individual breaks aren't itemised after the fact — you can see a
-- shift had 47 minutes of break, not that it was 30 + 17. If a dispute ever needs
-- that, add the interval table THEN; break_seconds stays as the authoritative total
-- either way, so it is an additive change rather than a migration.

-- ── 1. The open shift learns about breaks ────────────────────────────────────

-- Running total of break time already TAKEN and closed on this shift.
-- NOT NULL DEFAULT 0 so the arithmetic never has to COALESCE, and so a shift opened
-- before this migration behaves as "no breaks" rather than as NULL.
ALTER TABLE open_punches
  ADD COLUMN IF NOT EXISTS break_seconds numeric(12,1) NOT NULL DEFAULT 0;

-- The break currently IN PROGRESS. NULL means working; non-NULL means on break
-- since this instant. This single nullable column is the entire "am I on break"
-- state — there is no separate boolean to disagree with it, which is deliberate:
-- two columns encoding one fact is how you get a shift that is both on and off
-- break at once.
ALTER TABLE open_punches
  ADD COLUMN IF NOT EXISTS break_started_at timestamptz;

-- ── 2. The completed punch records what was deducted ─────────────────────────
--
-- ⚠ duration_seconds on clock_punches stays the NET worked time — elapsed minus
-- breaks — because that is what gets promoted into time_entries and paid. This
-- column exists so the deduction is visible and auditable rather than implied by a
-- gap between started_at/ended_at and duration_seconds. Do not "simplify" by
-- recomputing duration from the timestamps; it would silently pay people for lunch.
ALTER TABLE clock_punches
  ADD COLUMN IF NOT EXISTS break_seconds numeric(12,1) NOT NULL DEFAULT 0;

-- ── 3. City tax ──────────────────────────────────────────────────────────────
--
-- No schema change needed: open_punches.city_taxes and clock_punches.city_taxes
-- both already exist from 018, and promotion already carries city_taxes into
-- time_entries. What was missing was the UI ever SETTING it — punches were being
-- recorded with a NULL city, which promotion would have turned into the
-- 'A No Tax' fallback. That is a client-side fix, not a database one.
--
-- ⚠ The values are free text carrying QuickBooks Time's own spellings — Massilon,
-- New Philadephia, N Canton — and the app's PR_CITY_TAXES list must match them
-- VERBATIM or a value silently falls back to "A No Tax". Do not "correct" the
-- spellings on either side. See 001_time_entries.sql:56-60.
--
-- The default is derived from the job's address_city (jobs.address_city, already
-- surfaced to the client as `customerCity` by both mapJob paths) through an alias
-- table in index.html that maps real city names onto QB's spellings. It is only a
-- DEFAULT — the punch-out sheet always lets it be changed, because the crew knows
-- where they actually worked and the job address doesn't always.
