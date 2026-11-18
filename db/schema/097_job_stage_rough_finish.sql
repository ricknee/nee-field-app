-- 097 — Residential stages: Rough & Service, Finish (owner 2026-11-18).
--
-- "this is a residential job. and the stages we have are Rough & Service -
-- Finish". There is no residential flag on jobs (job_type is Contract / T&M /
-- Service Calls), so the two are added to the ONE stage list and the picker
-- offers them on every job; pick what fits.
--
--   rough    Rough & Service — crew on site doing rough-in and the service
--   finish   Finish — crew on site doing trim-out / finish
--
-- Both count as "On site now" on the Overview. Widens the CHECK only; no row
-- changes. Must land BEFORE the code that can write the new keys.

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_stage_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_stage_check
  CHECK (stage IS NULL OR stage IN ('preconstruction', 'onsite', 'rough', 'finish', 'punchlist', 'onhold'));
