-- 098 — Stage "complete" (owner 2026-11-19).
--
-- "when i set a job status to completed change this status to something like
-- completed ... do it automaticly". updateJobStatus sets stage = 'complete'
-- when status becomes Completed, and clears it again (back to NULL) if the
-- job is moved off Completed, so a re-opened job never shows a stale
-- "Complete" pill. It can also be picked by hand in Project Info → Edit.
--
-- Widens the CHECK only; no row changes. Must land BEFORE the code that can
-- write the new key.

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_stage_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_stage_check
  CHECK (stage IS NULL OR stage IN ('preconstruction', 'onsite', 'rough', 'finish', 'punchlist', 'onhold', 'complete'));
