-- Neon slice — per-job city tax for the time clock.
--
-- Applied BARE via the Neon MCP (which mangles inline comments); this file is the
-- annotated source of truth. Same convention as 001/002/018/019.
--
-- ── WHY THIS EXISTS: THE ADDRESS CANNOT ANSWER THE QUESTION ──────────────────
--
-- The clock briefly defaulted the city tax from jobs.address_city. That was removed
-- on 2026-08-08 because it is confidently wrong in the direction that costs money.
-- The owner's own example is the clearest statement of the problem:
--
--   "some jobs are no city tax, for instance Aaron Mclauglin is not in an actual
--    city limit. although it is columbiana.. we dont have to pay columbiana tax"
--
-- A mailing address of "Columbiana, OH" says nothing about whether the site sits
-- INSIDE the municipal corporation. 19 live jobs mail as Alliance; many of those are
-- in Lexington Township, which levies nothing. No lookup fixes this reliably either:
-- postal city is not a tax boundary, geocoders return places rather than tax
-- districts, and Ohio JEDDs/JEDZs tax township land at a city rate by agreement.
--
-- So the jurisdiction becomes a per-job fact that a human sets ONCE, on the job,
-- instead of a guess repeated on every punch. Owner: "per job city tax because they
-- always vary."
--
-- ── ONE COLUMN, NOT TWO ──────────────────────────────────────────────────────
--
-- An earlier draft had city_tax_work AND city_tax_travel per job. The owner settled
-- it: "travel is always no city tax." That is a company-wide rule, not a per-job
-- one, so it lives in code (the clock forces 'A No Tax' for the Travel class) and
-- not in a column that could be set inconsistently on 112 jobs.

-- App-owned column on a table that is otherwise an Airtable mirror.
--
-- ⚠ SAFE FROM THE HOURLY SYNC, BUT ONLY BECAUSE IT IS NOT IN `FIELDS`.
-- _jobs-sync.js upserts with `DO UPDATE SET` over exactly the columns listed in its
-- FIELDS array; anything absent is untouched. `billable_rate_id` is the existing
-- precedent for app-owned state living on this table. If you ever add city_tax to
-- FIELDS, the hourly sync will start overwriting it with Airtable's NULL and every
-- setting made here will silently vanish an hour later.
--
-- NULL means "never set" and is distinct from 'A No Tax' meaning "set, and there is
-- no tax". That distinction is the point: it lets the UI show which jobs still need
-- a human decision instead of pretending an unanswered job is answered.
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS city_tax text;

COMMENT ON COLUMN jobs.city_tax IS
  'City tax district for work performed on this job. Set per job by an admin; the '
  'site address cannot determine it (postal city is not the taxing jurisdiction). '
  'NULL = not yet decided; ''A No Tax'' = decided, no tax. Travel time always uses '
  '''A No Tax'' regardless of this value. App-owned: keep it OUT of _jobs-sync FIELDS.';
