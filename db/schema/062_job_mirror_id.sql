-- 062_job_mirror_id.sql — the ghost-job fix. Found in production 2026-08-24,
-- roughly three hours after JOB_CREATE_SOURCE=native went live, on the first
-- native job ever created (Test 10 / MIT 301).
--
-- ── THE BUG ────────────────────────────────────────────────────────────────
-- 061 and the `createJobNative` comment both say a native job is INVISIBLE to
-- the hourly `_jobs-sync.js`, because that sync is
-- `INSERT … ON CONFLICT (airtable_id)` and a native job has `airtable_id NULL`.
-- Both are true, and both are about the WRONG ROW.
--
-- A native create also writes a best-effort **mirror** into Airtable, and that
-- mirror is a real Airtable record with its own rec id. On the next hourly pass
-- the sync reads it, finds no Neon row carrying that rec id — because we
-- deliberately never stamp it back — and so takes the INSERT branch. The mirror
-- lands as a SECOND Neon job:
--
--   846245ef… airtable_id NULL             Estimating  po_locked set     ← real
--   f10b709f… airtable_id recPvbB0WaNllOhNm New Lead    po_locked NULL    ← ghost
--
-- Both carry po_number 301. `handleGetJobs` is `${JOB_SELECT} ORDER BY j.name`
-- with no filter, so the crew sees the job twice, and the ghost — frozen at the
-- mirror's creation-time values forever, since later edits have no Airtable
-- address to PATCH — is the one with no PO string and no pCloud folders.
--
-- It would have happened to EVERY native job, once an hour, forever.
--
-- ⚠ THE NEAR MISS. The ghost's `po_locked` is NULL only because `Job PO -
-- Locked` is not in the mirror payload. Fill it and `backfillJobLinks` sees two
-- jobs on one Job PO, its `= 1` guard calls the match ambiguous, and QuickBooks
-- hours stop attaching to the job — the duplicate-PO failure mode, arrived at
-- from a direction nobody was watching.
--
-- ── THE FIX ────────────────────────────────────────────────────────────────
-- Record the mirror's rec id HERE, in its own column, and have `syncJobs` skip
-- any Airtable record whose id is in it.
--
-- ⚠⚠ THIS COLUMN IS NOT A SECOND `airtable_id` AND MUST NEVER BE READ AS ONE.
-- Nothing resolves a job through it, nothing emits it, no view joins on it. It
-- exists so the sync can recognise our own mirror and leave it alone. Putting
-- the value in `airtable_id` instead would "work" for exactly one hour and then
-- do precisely what 061 exists to prevent: make the job a conflict target, so
-- Airtable's frozen copy overwrites all 38 columns every hour.
--
-- Why a column and not a marker field on the Airtable record: the base is being
-- archived, and a skip list we own cannot be edited, renamed or filtered out
-- from the Airtable side. The cost is a window — mirror POSTed, UPDATE lost —
-- which `createJobNative` logs loudly with the rec id rather than swallowing.

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS airtable_mirror_id text;

COMMENT ON COLUMN jobs.airtable_mirror_id IS
  'Rec id of the fail-soft Airtable MIRROR of a Neon-native job. Written once at create, never read back as a handle. Its ONLY consumer is _jobs-sync.js, which skips any Airtable record whose id appears here — without that the mirror re-imports as a duplicate job every hour. ⚠⚠ Never copy this into airtable_id: that is the hourly-overwrite trap 061 exists to prevent.';

-- Partial-unique: one mirror per job, and no two jobs claiming one mirror. A
-- violation here means a mirror was POSTed twice, which is a bug worth failing on
-- rather than absorbing.
CREATE UNIQUE INDEX IF NOT EXISTS jobs_airtable_mirror_id_key
  ON jobs (airtable_mirror_id) WHERE airtable_mirror_id IS NOT NULL;

-- ── THE ONE ROW THAT ALREADY EXISTS ────────────────────────────────────────
-- Test 10's mirror, adopted so the sync stops re-creating its ghost, and then
-- the ghost itself. Verified first: the ghost had no time entries, expenses,
-- estimates, invoices, photos, panels or schedule entries pointing at it — it
-- had existed for two hours and nothing had ever been filed against it.
--
-- UPDATE jobs SET airtable_mirror_id = 'recPvbB0WaNllOhNm'
--  WHERE id = '846245ef-294f-423b-a2b1-4b4a919607f8';
-- DELETE FROM jobs WHERE id = 'f10b709f-78e8-4731-a700-b6e8973ab886';
--
-- Left as comments deliberately: both ran once against production on 2026-08-24
-- and re-running them on a fresh database would target ids that do not exist.
