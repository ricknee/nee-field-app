-- Neon slice — overhead jobs on the time clock.
--
-- Applied BARE via the Neon MCP; this file is the annotated source of truth.
--
-- ── THE PROBLEM THIS REMOVES ─────────────────────────────────────────────────
-- The clock's job picker chooses what to show from STATUS (Awarded / Ready to
-- Invoice / Completed). That is right for real customer jobs and accidental for
-- the two that aren't:
--
--   Shop Work (SHS 115)   Ready to Invoice   -> visible to the crew, but only
--                                               because of a status that isn't
--                                               true. ~500 h/yr flows through it.
--   Office Work (ADO 248) Not Awarded        -> hidden behind the "show every
--                                               job" tick, which happens to be
--                                               the desired behaviour.
--
-- So today, marking Shop Work Completed — a perfectly reasonable tidy-up —
-- silently removes it from every employee's picker, and their shop time starts
-- landing on whatever they pick instead. Nobody would notice until a GP report
-- looked wrong weeks later.
--
-- This column makes the intent explicit, so job status can go back to describing
-- the job rather than quietly driving the clock.
--
--   NULL      normal. Status decides, exactly as now. This is every real job.
--   'all'     always offered, to everyone, whatever the status says.
--   'admin'   always offered, but only to admins. For overhead nobody else books.
--   'hidden'  never offered. For a job that should not collect time at all.
--
-- ⚠ SAFE FROM THE HOURLY SYNC ONLY BECAUSE IT IS NOT IN _jobs-sync.js's FIELDS.
-- Same rule as jobs.city_tax (020) and the pre-existing billable_rate_id. Add it
-- to FIELDS and the hourly refresh will overwrite every setting with Airtable's
-- NULL, silently, an hour later.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS clock_visibility text;

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_clock_visibility_chk;
ALTER TABLE jobs ADD CONSTRAINT jobs_clock_visibility_chk
  CHECK (clock_visibility IS NULL OR clock_visibility IN ('all', 'admin', 'hidden'));

COMMENT ON COLUMN jobs.clock_visibility IS
  'Overrides status when deciding whether a job appears in the time clock''s job '
  'picker. NULL = status decides (normal). all = always, everyone. admin = always, '
  'admins only. hidden = never. App-owned: keep it OUT of _jobs-sync FIELDS.';
