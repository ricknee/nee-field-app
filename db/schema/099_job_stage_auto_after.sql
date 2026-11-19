-- 099 — First hours move a job to On site (owner 2026-11-19).
--
-- "when the 1st hour is clocked or recorded can u change the status to on
-- site ... all jobs just go to onsite. then i change to rough if i wanna."
--
-- The hourly qb-time-pull (_job-stage-auto.js) sets stage = 'onsite' on an
-- AWARDED job whose stage is Pre-con or not set, once it has hours RECORDED
-- (time_entries.created_at) after `stage_auto_after`. It never touches rough /
-- finish / punchlist / onhold / complete.
--
-- stage_auto_after is "only hours recorded after this moment can move the
-- stage". It is stamped when the stage changes (by hand or by the rule) and
-- when the status changes (so awarding a job starts the clock — estimating
-- site-visit hours don't count). That is what lets a job be put BACK to
-- Pre-con after early prep hours (Ryan Brewer: 3.5 h in August) and stay there.
--
-- Backfilled to now() so no hours already on file move anything: the rule
-- starts with the next hours recorded.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS stage_auto_after timestamptz;
UPDATE jobs SET stage_auto_after = now() WHERE stage_auto_after IS NULL;
