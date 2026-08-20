# TODO

## ✅ DONE 2026-08-03 — `FIND(name, ARRAYJOIN(...))` substring sweep

Seven sites fixed, three deliberately left. Regression tests added for the three that could be
reached through a handler (`jobInspections`, `jobEstimates`, `generator`) — each fails against the
old code.

**Fixed** — newline-delimited so `FIND` matches per linked element, plus an in-memory record-id
check wherever an id was available:

| Site | What leaked |
|---|---|
| `handleGenerator` | wrong job's generator **and** its whole service history |
| `handleGenerator` (service) | wrong generator's services via prefix-colliding asset id |
| `handleJobInspections` | wrong job's inspections |
| `handleJobEstimates` | wrong job's estimates — money, so it misstated expected revenue and GP |
| `handleEstimateTemplates` | another contractor's templates ("Case Farms" vs "Case Farms North") |
| generator resolve-by-job (commissioning) | attached a service record to the wrong generator |
| duplicate-service check | **functional**: a substring hit made the dup check fire on another generator and silently skip creating a record |
| existing-warranty count | decided whether warranties got created |

`handleGenerator` was also interpolating the job name **unescaped** — a name containing a double
quote broke the formula outright. Now escaped.

**Deliberately left:**
- `airtable.js:2846` (`Job Name (Text)`) — already safe. `{Job Name (Text)}` holds
  `"Job Name (PO suffix)"`, so an exact match never fires; the substring `FIND` is a deliberate
  loose prefilter and correctness comes from the in-memory record-id check right below it.
- `airtable.js:3339` (Inspection Agency Name), `:3433` (power-company contacts) — these back
  **name typeaheads**, where substring matching is plausibly the intended behaviour. Making them
  exact could break a picker. Confirm the callers pass a full selected name before changing.

## Previously recorded (kept for context)

The cross-job filter bug fixed in `03f552a` (handleTimeEntries / handleExpenses) has the same
shape in four remaining places. Each matches a job by NAME via substring, so one job whose name
contains another's leaks across — and duplicate exact names collide outright.

**When it fires:** only when one job's name is *contained inside* another's — `FIND` is a
substring test. The known real case is **"Jenny Ln 1" inside "Jenny Ln 10/11/12"**: numbered jobs
at the same address or on the same street. Duplicate exact names collide outright.

> Similar-*looking* names are NOT the trigger. "Craig Davidson" and "Davidson's Addition" are
> fine — neither contains the other. (An earlier revision of this file cited those as examples;
> that was wrong.) The naming convention is not the problem — the substring match is.

The damage is inspections or estimates appearing **on the wrong job** — quiet, plausible-looking
wrong data rather than an error anyone would notice.

Line numbers verified 2026-08-02 (they drift; grep `FIND(` + `ARRAYJOIN` to confirm):

- `netlify/functions/airtable.js:2078` — generator lookup (`F.gen.job`)
- `netlify/functions/airtable.js:2194` — Job Inspections (`FIND(jobName, ARRAYJOIN({Job}))`)
- `netlify/functions/airtable.js:2224` — Job Estimates (`FIND(jobName, ARRAYJOIN({Job}))`)
- `netlify/functions/airtable.js:2480` — Contractor (`FIND(safeContractor, ARRAYJOIN({Contractor}))`);
  lower duplicate-name risk but the same shape

**Fix pattern** — keep the `FIND` as a loose prefilter, then verify the linked record ID in
memory. Already done correctly in `handleGetJobInvoices`; the newline-delimited variant at
airtable.js:2824 / :2851 is the reference to copy. Add a test per site.

> Worth noting the photo system sidesteps this entirely by keying on the Airtable **record id**
> (`jobs/<recId>/…`) rather than the name — record ids cannot collide. Same idea applies here.

## Receipts on expenses — partly shipped

- ✅ **Materials-PDF archiving** shipped 2026-08-01 (`54201d7` + `5b5ccf5`): the inventory app's
  generated PDF now uploads to R2 as well as downloading, admin/office only.
- ⬜ **Attach-a-receipt slices 1-3** (~8-11 h) — not started. Must accept BOTH phone photos
  (compress) and ScanSnap PDFs (upload untouched, no thumbnail). Full plan, decisions and
  gotchas in `docs/PLAN-expense-receipts.md`.

## ✅ CLOSED 2026-08-20: `handleUpdatePowerCo` — already fixed, was never open

Fixed in `70e4315` ("swept every job writer"), which landed the same day this note was written.
The handler writes Neon FIRST at `airtable.js:5485`, with a comment naming the sweep that caught
it. Kept below for the failure mode, which is the one worth remembering.

*Original note follows.*

**The same bug as `ff21d46`, unverified, ~30 min to confirm and fix.**

`handleUpdateJobStatus` wrote Airtable only while `handleJobs` reads **Neon first**, so an
awarded job reverted to its old status on refresh. Found in the field 2026-08-12 — the **fourth**
instance of "flip a read without its write" (ROADMAP §8 records three in one day). Fixed there,
and in `handleStartServiceCall` + `handleCompleteServiceCall`, which had the identical shape.

**`handleUpdatePowerCo` has that shape too and was not swept.** It PATCHes six fields that are
all in `JOB_SELECT` and therefore all served from Neon:

`fld3fZ9isIQmcFDna` power company · `fldhKlMCFsnmHo5PH` power contact · AIC number ·
temp work order · perm work order · meter number

> **Why nobody has reported it:** these are set once during power-company setup and rarely
> re-read in the same session, and the hourly `_jobs-sync.js` papers over it. The symptom would
> be "I typed the meter number, it saved, and next time I looked it was blank" — hours later,
> long after anyone would connect it to the save.

**To close it:** confirm by setting a power-co field, hard-refreshing, and seeing whether it
survives. Then add a `neonWrite` before the PATCH, Neon-first and failing closed, matching the
three already fixed. **Sweep the other job writers in the same pass** rather than waiting for
each to surface on its own — this class has now bitten four times, and every instance was found
by a person hitting it rather than by a test.

## Smaller, unscheduled

- **R2 lifecycle rule** to expire the photo recycle bin (`_deleted/`) at 30 days — ~15 min. Must
  **exclude `expenses/`**: receipts are financial records and are deliberately never auto-purged.
- **Retire the JotForm photo path** (~2026-08-08, after a week's soak): pause form
  `260246511955053` and Make scenario `4522457`. Pause, don't delete. No code change.
- **Archive Push History reprints** — `reprintPushHistoryPdf` rebuilds the PDF without a `pushId`
  or job id, so it downloads but doesn't archive. Deliberate; easy to add.
- **Offline upload queue** for photos — basements and steel buildings. Worth building around
  observed field behaviour rather than guesses.
