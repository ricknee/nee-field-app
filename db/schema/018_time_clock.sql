-- Neon slice — the in-app time clock (docs/PLAN-time-clock.md).
--
-- Same convention as 001/002/017: this file is the annotated source of truth, and the
-- SQL is applied BARE via the Neon MCP migration path (which mangles inline comments).
-- Keep the two in sync by hand.
--
-- ── THE SHAPE, AND WHY IT IS NOT WHAT THE PLAN FIRST DREW ──────────────────────
--
-- PLAN-time-clock.md §4 drew this as: `open_punches` + on punch-out insert straight
-- into `time_entries` with source = 'Clock'. That is the right END state and this
-- migration builds toward it. It is NOT safe as the starting state, because of the
-- owner's decision on 2026-08-08:
--
--     "build the app and replace QB Time later on. not going to use time tracking
--      in the app until its complete and ready for use"
--
-- So QuickBooks Time keeps running, and keeps being the book of record, while this is
-- built and shaken out. Two systems will hold hours for the whole build. A punch that
-- lands directly in `time_entries` is therefore a punch that lands in PAYROLL:
-- `handlePayrollHoursRollup` (airtable.js:1462) sums `time_entries` with no source
-- filter at all, and so do 7 other reader sites plus the labor-cost views in 004/006.
-- A test punch would be double-paid against a QB timesheet for the same hours.
--
-- The obvious fix — add `source <> 'Clock'` to every reader — is the exact shape of
-- bug this repo has already shipped once (see the memory of grepping `startsWith("rec")`
-- and missing live call sites). It fails OPEN: forget one site, and money is wrong.
--
-- So instead the clock gets its OWN ledger, and promotion into `time_entries` is a
-- separate, switched step:
--
--     punch  ->  clock_punches (always)  ->  time_entries (only when TIME_CLOCK=on)
--
-- Nothing existing reads `clock_punches`, so while the switch is off the clock cannot
-- touch payroll BY CONSTRUCTION rather than by remembering to filter. Zero edits to
-- the 8 existing readers. At cutover the switch flips and `promoteClockPunches`
-- backfills anything punched before it.
--
-- This is the same shadow-then-flip shape as the login migration (LOGIN_SOURCE), which
-- is the pattern this codebase has already proven twice.

-- ── 1. The open shift ─────────────────────────────────────────────────────────
--
-- `time_entries` has no concept of being on the clock: it stores work_date and
-- duration_seconds and nothing else. This table is that missing state.
--
-- employee_id is the PRIMARY KEY, not just a column with an index. That single choice
-- is what makes double-punching IMPOSSIBLE rather than merely discouraged — two taps
-- on a flaky connection, or a replayed offline punch, hit a uniqueness violation
-- instead of quietly opening a second shift that never gets closed. There is no
-- application-level "check if already clocked in" race to lose.
CREATE TABLE IF NOT EXISTS open_punches (
  employee_id    uuid PRIMARY KEY REFERENCES employees(id) ON DELETE CASCADE,

  -- The moment the person said "I started". On an offline punch this is the CLIENT's
  -- timestamp, deliberately: see the replay note in section 3.
  started_at     timestamptz NOT NULL,

  -- Snapshot pair, same rule as time_entries: keep BOTH the FK and the static text,
  -- and never derive the text from the link. job_name carries jobs.po_locked as it
  -- stood at punch time, because that is what an import today would have recorded.
  job_id         uuid REFERENCES jobs(id),
  job_name       text,

  class          text,
  city_taxes     text,
  notes          text,

  -- One-shot geolocation AT the punch, when the browser offers it. There is no
  -- background geofencing here and there cannot be — iOS Safari has no background
  -- geolocation and a PWA gets no reliable wake-up, so QB Time's auto-punch-on-arrival
  -- is NOT reproducible on this stack. Do not promise it. Nullable: permission denied
  -- is the normal case, not an error.
  start_lat      numeric(9,6),
  start_lon      numeric(9,6),

  -- Idempotency key minted by the CLIENT before the request leaves the phone. The
  -- offline queue may replay the same punch many times; this is what makes the second
  -- through Nth attempt a no-op instead of a duplicate shift.
  client_punch_id text UNIQUE,

  created_at     timestamptz DEFAULT now()
);

-- ── 2. The clock's own ledger ─────────────────────────────────────────────────
--
-- A completed punch. This row is DURABLE and is written whether or not the clock is
-- switched on — it is the clock's record of what actually happened, at full precision,
-- independent of what payroll later rounds it to.
--
-- Keeping raw started_at/ended_at here (rather than only a duration) is what lets a
-- dispute be answered later: `time_entries.hours` is rounded to the quarter hour by an
-- immovable rule (see section 4), so the entry alone can never say when someone
-- actually worked.
CREATE TABLE IF NOT EXISTS clock_punches (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  -- NOT NULL, unlike time_entries.employee_id which is nullable and historically
  -- unlinked on ~38% of rows. A punch comes from a signed-in session, so there is no
  -- such thing as a clock row that does not know whose it is.
  employee_id      uuid NOT NULL REFERENCES employees(id),
  employee_name    text,

  started_at       timestamptz NOT NULL,
  ended_at         timestamptz NOT NULL,

  -- ⚠ THE OVERNIGHT RULE, decided explicitly here because it silently moves hours
  -- between PAY WEEKS if left implicit (time_entries.week_start_date is GENERATED
  -- from work_date, so this choice propagates straight into payroll).
  --
  --     A shift belongs to the local date it STARTED.
  --
  -- 22:00 Tuesday -> 02:00 Wednesday is a Tuesday shift, all four hours of it. That
  -- matches how crews describe their own work ("Tuesday night"), and it keeps one
  -- continuous shift inside one pay week instead of splitting it across two.
  --
  -- The zone is named, not inherited. Postgres would otherwise cast timestamptz->date
  -- in whatever TimeZone the connection happens to carry (UTC on Neon's pooler), which
  -- would file every evening punch after 20:00 EDT under the NEXT day. This is the
  -- first place in the whole schema that converts a timestamp to a date; every other
  -- date came pre-made from QuickBooks. Ohio: the city-tax list is Massilon, New
  -- Philadephia, N Canton.
  work_date        date NOT NULL,

  -- Full precision. The quarter-hour rounding lives in time_entries.hours and is
  -- applied at PROMOTION, never here.
  duration_seconds numeric(12,1) NOT NULL,

  job_id           uuid REFERENCES jobs(id),
  job_name         text,
  class            text,
  city_taxes       text,
  notes            text,

  start_lat        numeric(9,6),
  start_lon        numeric(9,6),
  end_lat          numeric(9,6),
  end_lon          numeric(9,6),

  -- Same client-minted replay key as open_punches, carried across so a punch-out that
  -- is retried after the connection returns collapses onto the row it already wrote.
  client_punch_id  text UNIQUE,

  -- ── The promotion link ──
  -- NULL means "this punch has not been counted as payroll hours". While TIME_CLOCK is
  -- off that is every row, which is precisely why payroll cannot be polluted during the
  -- build. ON DELETE SET NULL so deleting a bad time entry does not delete the evidence
  -- of the punch behind it — it just un-promotes it, and it can be promoted again.
  time_entry_id    uuid REFERENCES time_entries(id) ON DELETE SET NULL,
  promoted_at      timestamptz,

  created_at       timestamptz DEFAULT now(),

  -- Time does not run backwards. Equality is permitted: a mis-tap that clocks in and
  -- straight back out is a real thing a person does, and it should record as a zero,
  -- not fail and leave them stuck on the clock.
  CONSTRAINT clock_punch_ordered CHECK (ended_at >= started_at)
);

CREATE INDEX IF NOT EXISTS clock_punches_employee_date_idx
  ON clock_punches (employee_id, work_date DESC);

-- Finds everything awaiting promotion at cutover. Partial, because after cutover the
-- overwhelming majority of rows are promoted and would be dead weight in a full index.
CREATE INDEX IF NOT EXISTS clock_punches_unpromoted_idx
  ON clock_punches (work_date) WHERE time_entry_id IS NULL;

-- ── 3. time_entries learns when the work happened ─────────────────────────────
--
-- Nullable and staying nullable: a QB-imported or hand-typed row genuinely does not
-- know its own start and end, and inventing values for 14.5k historical rows would be
-- fiction. Only promoted clock rows carry them.
ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS started_at timestamptz;
ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS ended_at   timestamptz;

-- ── 4. te_has_a_key must admit the new provenance ─────────────────────────────
--
-- Without this, promotion fails on EVERY row and the failure only appears at cutover.
-- The constraint's purpose is unchanged: a row must declare an origin, so nothing lands
-- that no writer can address and no reconciler can explain. 'Clock' is that declaration
-- for a punched row.
--
-- 'Clock' rather than reusing 'Manual' is deliberate. 'Manual' means "a human typed
-- hours into the payroll screen" — a different provenance with different trust, and the
-- first time anything is audited the two will need separating. It is also what makes
-- the cutover reversible: while QB Time is still running, `DELETE FROM time_entries
-- WHERE source = 'Clock'` is a complete, exact undo of everything the clock ever
-- contributed to payroll. Reusing 'Manual' would have destroyed that.
ALTER TABLE time_entries DROP CONSTRAINT IF EXISTS te_has_a_key;
ALTER TABLE time_entries ADD CONSTRAINT te_has_a_key
  CHECK (airtable_id IS NOT NULL
      OR qb_timesheet_id IS NOT NULL
      OR source = 'Manual'
      OR source = 'Clock');
