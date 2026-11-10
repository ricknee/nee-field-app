-- 088 — "Due date met" on a job's completion date (owner 2026-11-09).
--
-- Trail Cabinet reached its completion date — the customer can operate — but a
-- few items are still open that have nothing to do with the date, so the job is
-- not Completed and the Project Info card kept saying "N days OVERDUE".
--
-- completion_met_on is the day someone said the date was met. NULL = not said.
-- It changes nothing but that display: the date itself, the job status, the
-- schedule's 🏁 pill and Workload all read completion_date exactly as before.
-- Additive and nullable, so safe to apply ahead of the code.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS completion_met_on date;
