-- ── PO numbers allocated by Neon, and jobs born in Neon ────────────────────
-- Applied BARE to the production branch via the Neon MCP 2026-08-12; this file
-- is the annotated source of truth. Audit item 05.
-- Design and decode: docs/PLAN-job-creation-neon.md.
--
-- ⚠⚠ PO NUMBERS ARE JOB IDENTITY. They render into `Job PO` (a formula), which
-- names the **pCloud job folder**, fills `Job PO - Locked`, and travels to
-- Trello and QuickBooks Time. A duplicate is not a display bug — it is two jobs
-- claiming the same identity across four systems, and pCloud folders are
-- painful to rename once files are in them.
--
-- ── WHY: a job took an hour to exist ───────────────────────────────────────
-- `handleCreateJob` POSTed to Airtable and returned. The job reached Neon only
-- when `_jobs-sync.js` next ran, so **a new job showed an empty Time Entries tab
-- for its first hour**. Job creation was the last write living entirely in
-- Airtable.
--
-- ── WHAT A PO ACTUALLY IS (owner, 2026-08-12) ──────────────────────────────
-- `Bethel School (MIB 433)` = **MI** contractor code + **B** first letter of the
-- job name + **433** the autonumber. `Job PO` is an AIRTABLE FORMULA — its own
-- description says *"Combines Job Name, Contractor Code, and Job PO Number"* —
-- so the prefix is assembled from a stored Contractor Code, not derived here.
--
-- **The app supplies ONLY the number.** All prefix logic stays in Airtable and
-- is untouched by this change. If PO formatting ever needs to move, that is its
-- own slice and it needs the Contractor Code field to come with it.
--
-- ── ⚠⚠ THE YEARLY RESTART — WANTED, AND SILENTLY BROKEN IN AIRTABLE ────────
-- Owner: *"i would like to restart that autonumber each new year. so the numbers
-- dont get too high."*
--
-- **That is what this implementation does**, and it is worth being explicit
-- about why the old one would not have. The Airtable automation's counter
-- lookup filters on a **hardcoded literal year**:
--
--     findRecords(tbl8s6L1i6wotlEsn) WHERE fldtG0ZdTJBm1HBtI = "2026"
--                                                              ^^^^^^
--
-- So on 1 January 2027 it would have carried on reading the **2026** row and
-- issuing 287, 288, 289… The pre-seeded 2027 row (last_used 99) would have sat
-- unused, and nothing would have failed loudly — somebody would simply have had
-- to remember to edit the automation every January, forever.
--
-- The app derives the year with `new Date().getFullYear()`, so the restart is
-- automatic: 2027's first job gets **100**, 2028's gets 100, and a year with no
-- pre-seeded row creates one at 100 via the upsert. Nothing to remember.
--
-- ── THE COUNTER, AND THE RULE THAT MUST SURVIVE ────────────────────────────
-- Airtable keeps a counter table (`tbl8s6L1i6wotlEsn`), one row per year, and
-- automation `wfltJAiEaavVLA0wB` reads `next`, stamps the job, and writes it
-- back. Seeded here verbatim: 2025 → 99, 2026 → **285**, 2027 → 99.
--
-- ⚠⚠ **DO NOT DERIVE THE NEXT NUMBER FROM THE JOBS TABLE.** 112 jobs run from
-- 102 to 436 and **22 sit ABOVE the counter**, because Dollar General jobs carry
-- the general contractor's own numbering (`Adena DG (31614) (PEA 435)`).
-- `max(po_number) + 1` would jump to 437 and abandon ~150 unused numbers.
-- The counter is the authority; the jobs table is not. Verified at seed time:
-- last used 285, and **no job holds 286**.
--
-- Allocation is ONE statement, so two people creating a job in the same instant
-- cannot both read the same value — the flaw Airtable's read-then-write has
-- always had and got away with because one person creates jobs at a time:
--
--   INSERT INTO job_po_counters (year, last_used) VALUES ($1, 100)
--   ON CONFLICT (year) DO UPDATE SET last_used = job_po_counters.last_used + 1
--   RETURNING last_used
--
-- A brand-new year starts at 100, matching the 2025 and 2027 rows. PREPARE
-- checked: `{integer}`.
--
-- ── THE CUTOVER IS THE ORDERING, NOT AN UNDEPLOY ───────────────────────────
-- The automation triggers on *"status = New Lead AND Job PO Number is EMPTY"*.
-- The app allocates the number and includes it **in the same POST that creates
-- the record**, so the condition is false on arrival and the automation stands
-- down by itself.
--
-- That is why there is no undeploy gap: no window where a job is created with
-- nobody assigning a PO, and none where both assign and the job ends up with
-- two different numbers. Undeploy `wfltJAiEaavVLA0wB` later, at leisure, once
-- the app has been seen to do it.
--
-- ⚠ AIRTABLE IS STILL CREATED FIRST, and the plan's original "writes Neon
-- first" wording was wrong. `jobs.airtable_id` is NOT NULL and every job id the
-- client holds IS the Airtable rec id (`mapJobFromNeon` returns
-- `id: r.airtable_id`), so a Neon-first job would have no id the app could use.
-- Order is: allocate PO → POST Airtable → INSERT Neon.
--
-- Both Neon steps fail SOFT. A failed allocation omits the field, and Airtable's
-- automation assigns exactly as it does today. A failed insert leaves the job in
-- Airtable for the hourly sync to adopt — costing the old one-hour lag rather
-- than the job. **Neither may block someone creating a job.**
--
-- ── SHIPS INERT ────────────────────────────────────────────────────────────
--   JOB_CREATE_SOURCE  unset | airtable  -> Airtable assigns, as today
--   JOB_CREATE_SOURCE  = neon            -> the app assigns and writes Neon
--
-- ⚠ While it is off, the app must NOT send `Job PO Number` at all. Sending it
-- empty would satisfy nothing and still stand the automation down, leaving a job
-- with no number — worse than the status quo. There is a test for exactly that.
--
-- ── VERIFY BEFORE TRUSTING ─────────────────────────────────────────────────
--   SELECT po_number, count(*) FROM jobs WHERE po_number IS NOT NULL
--    GROUP BY po_number HAVING count(*) > 1;      -- must be empty
--   SELECT year, last_used FROM job_po_counters;  -- must advance by exactly 1
--
-- `jobs.po_number` was backfilled from the `po` display string for all 112 jobs
-- so that check is meaningful from day one: 112 numbered, 0 duplicates.
--
-- ── ROLLBACK ───────────────────────────────────────────────────────────────
-- `netlify env:unset JOB_CREATE_SOURCE` + rebuild. Airtable resumes assigning
-- immediately; its automation was never undeployed. Leave the tables — they are
-- additive and nothing else reads them.
CREATE TABLE IF NOT EXISTS job_po_counters (
  year      int PRIMARY KEY,
  last_used int NOT NULL,
  synced_at timestamptz DEFAULT now()
);

-- Verbatim from Airtable's counter table, 2026-08-12.
INSERT INTO job_po_counters (year, last_used)
VALUES (2025, 99), (2026, 285), (2027, 99)
ON CONFLICT (year) DO NOTHING;

-- The numeric PO. Neon carried only the rendered `po` string ("Bethel School
-- (MIB 433)"); the number itself lives in Airtable's `Job PO Number`.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS po_number int;

-- Backfill from the display string, so the duplicate check above has something
-- to check. 112 of 112 matched.
UPDATE jobs SET po_number = NULLIF(regexp_replace(po, '^.*\s([0-9]+)\)\s*$', '\1'), po)::int
 WHERE po ~ '\([A-Z]+\s[0-9]+\)\s*$'
   AND po_number IS DISTINCT FROM NULLIF(regexp_replace(po, '^.*\s([0-9]+)\)\s*$', '\1'), po)::int;
