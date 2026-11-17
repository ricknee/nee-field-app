-- 096 — Stage on an awarded job (owner 2026-11-17).
--
-- "Awarded" says the job is ours; it does not say whether a crew is on it.
-- Southwood Pallet is awarded but nobody is on site before February — material
-- is being ordered. The Overview needs to tell those apart.
--
-- A SUB-STATUS, deliberately NOT new values of jobs.status: Workload, the
-- supplier-invoice charge picker, the time clock and more all key off
-- status = 'Awarded' exactly, and a new status would quietly drop jobs out of
-- every one of them. Stage sits beside status and changes none of that.
--
--   preconstruction  awarded, ordering / submittals, no crew on site
--   onsite           crew is working it
--   punchlist        nearly done, a few trips left
--   onhold           waiting on customer / GC / weather
--   NULL             nobody has said. The Overview then uses the Planned Start
--                    (jobs.start_date): a future start reads as Upcoming.
--
-- Additive and nullable, so safe to apply ahead of the code.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS stage text;

DO $$ BEGIN
  ALTER TABLE jobs ADD CONSTRAINT jobs_stage_check
    CHECK (stage IS NULL OR stage IN ('preconstruction', 'onsite', 'punchlist', 'onhold'));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;
