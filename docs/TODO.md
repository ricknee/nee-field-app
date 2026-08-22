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

## ✅ FIXED 2026-08-22 — pCloud job folders landed in the wrong place

Make scenario `4509211` reported **21 successful operations** and created almost nothing where
anyone would look for it. Found when WatersEdge 13 went New Lead → Estimating and no folders
appeared.

**The mechanism.** Modules 8 (year), 9 (contractor) and 10 (job) each carry an
`onerror → builtin:Resume` whose mapper is **entirely blank**. That is correct for 8 and 9 — the
year and contractor folders usually already exist, pCloud answers `[2004] File or folder already
exists`, and the run should carry on. But every one of the 18 downstream folders built its path
from **`{{10.name}}`**, the job folder's returned name. So the moment module 10 resumed blank, the
path lost a segment and the whole tree was created **directly inside the contractor folder**:

```
NEE Jobs/2026/KDC Properties/Full Prints/…      ← wrong
NEE Jobs/2026/KDC Properties/WatersEdge 13 (KDW 279)/Full Prints/…   ← intended
```

Make cannot see this. Every `createFolder` succeeded; only the parent was wrong.

**The fix.** Nothing in the path may come from a module's *output* when the webhook payload
already carries the value. All paths now use `{{43.jobPO}}` and `{{43.contractor}}` — the values
the app sends — plus literals for the fixed folder names (`Full Prints`,
`Quote - Contract - Expenses`). The blank Resumes stay, because tolerating "already exists" is
still right; they simply can no longer poison anything.

⚠ **The general rule, worth carrying to every other scenario:** a `Resume` that returns blanks is
only safe if nothing downstream reads its output. Check what consumes a module before giving it an
error handler.

⚠ **Stray folders may exist** at the contractor level from earlier runs of any job that hit this.
They are harmless but confusing; delete by hand.

## Still open — the folder ids never come back

`4509211` has **no callback module at all**: it is `webhook → 21× createFolder` and nothing else.
So `jobs.pcloud_job_folder_id` and its four siblings (`db/schema/046`) are permanently NULL, and
`handleJobAutomationResult` — which already accepts all five — never hears from it.

Nothing in the app reads those columns today, so this is a gap rather than a fault. To close it:

1. `_job-webhooks.js` must add a **scope token** to the pCloud payload (`signScope(["jobAutomation",
   recordId])`), exactly as the Awarded and Completed payloads already do. Without it the callback
   cannot authenticate — the endpoint is unauthenticated by design and the token is its only guard.
2. Add an `http:ActionSendData` module at the end of the scenario POSTing `recordId`, `token` and
   the five folder ids to `/.netlify/functions/airtable` with `action: "jobAutomationResult"`.

## Smaller, unscheduled

- **R2 lifecycle rule** to expire the photo recycle bin (`_deleted/`) at 30 days — ~15 min.
  ⚠⚠ **Scope it to the `_deleted/` prefix, not to the bucket.** One bucket holds every domain
  (`jobs/`, `expenses/`, `lifts/`, `fleet/`, `estimates/`, `payroll/`), so a bucket-wide
  "expire after N days" rule would delete **financial records**, not old photos. The two that
  must never be auto-purged:
  - `expenses/` — receipts, wanted years later at audit time.
  - `payroll/` — the run PDFs, added 2026-08-21 (`db/schema/052`). This is the artifact people
    were paid from. Nothing in the app ever deletes one, and nothing ever moves one into
    `_deleted/`, so a prefix-scoped rule cannot reach them — a bucket-wide one would.

  Related, and worth confirming while you are in the Cloudflare console: **public access and the
  `r2.dev` subdomain should be OFF.** Every read the app performs is a signed, expiring URL
  (`presignGet` / `presignGetDownload`), so the bucket never needs to be public — and it now
  holds payroll.
- **Retire the JotForm photo path** (~2026-08-08, after a week's soak): pause form
  `260246511955053` and Make scenario `4522457`. Pause, don't delete. No code change.
- **Archive Push History reprints** — `reprintPushHistoryPdf` rebuilds the PDF without a `pushId`
  or job id, so it downloads but doesn't archive. Deliberate; easy to add.
- **Offline upload queue** for photos — basements and steel buildings. Worth building around
  observed field behaviour rather than guesses.
