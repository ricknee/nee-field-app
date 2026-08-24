# Audit item 10 — the identity cutover

*Written 2026-08-21, after items 02/03/04 landed. This is the last structural piece
between the app and archiving the Airtable base.*

## What it is, in one sentence

**Stop minting `rec…` ids in Airtable and let Neon mint uuids** — while every id-consuming
surface keeps accepting both, forever.

It is *not* "delete the mirror writes". Deleting the mirrors is the last step of each slice,
and on its own it would do nothing but break Make. The work is moving where an id is born.

## Why this is the blocker, and the only one

Measured 2026-08-21 by sweeping every Airtable call in both functions:

| | Count | Meaning |
|---|---|---|
| Write paths that reach Neon | **55 of 55** | Nothing lives only in Airtable. Losing the base loses no data. |
| Already Neon-first + fail-soft mirror | 21 | Time entries, fleet, lifts, scheduling, generators, inspections, mileage. Unaffected by anything below. |
| Airtable-**first** writes | 34 | Not because Neon can't hold the record — because **the rec id is minted there**. |
| Handlers with no Neon path at all | **1** | `handleUnlinkedMaterialAllocations`. See "The one genuine straggler". |

So if Airtable went away today: every read still works, and **nothing can be created**. That is
one problem wearing 34 hats.

## The checklist is already in the database

`airtable_id NOT NULL` is a formal statement that a table cannot hold a row Airtable has never
seen. Dropping that constraint is what "goes native" means, so the tables carrying it are the
work, and the tables without it are already free.

**Still constrained — the actual scope:**

| Table | Slice | Notes |
|---|---|---|
| `expense_vendors`, `labor_billable_rates`, `power_companies`, `companies` | 1 — leaves | Reference data. Nothing keys on them but jobs/expenses, by rec id, from rows that already exist. |
| `payroll_runs`, `payroll_bonuses` | 2 | Files already moved to R2 on the **Neon uuid** (`db/schema/052`). Half done by accident. |
| `job_estimates`, `invoices`, ~~`job_labor_allocations`~~ | 3 | Money. `sent_estimate_pdfs` is already nullable and points at estimates. ⚠ `job_labor_allocations` was wrong: nothing in either function reads or writes it, so its NOT NULL is vestigial — see the slice 3 section. |
| `expenses` | 4 | **The hard one.** R2 receipt keys are built from the expense rec id, and `inventory.js` writes this table from the other app. |
| `employees` | 5 | The login `id` is this rec id and it is persisted in every browser's `localStorage`. |
| `jobs` | 6 — last | Everything above links to it. |

**Already nullable, i.e. already able to go native:** `time_entries`, `generators`,
`generator_service`, `schedule_entries`, `warranties`, `job_inspections`, `inspection_agencies`,
`inspection_contacts`, `contacts`, `power_contacts`, `fleet_*`, `scissor_lifts`,
`labor_billing_allocations`, `material_billing_allocations`, `sent_estimate_pdfs`,
`estimate_templates`, and the whole inventory cluster.

**Ignore:** `pipe_usage` / `wire_weigh_ins` (JotForm path retired), `employee_weekly_time`
(frozen), `time_entries_deleted` (archive), `labor_cost_rates` (Neon already owns it; the
constraint is vestigial), `_*_baseline` (scratch tables).

## Where each of the 34 writes goes

The table above is the scope; this is the same scope named handler by handler, so nothing can be
assumed covered. Measured 2026-08-21.

**13 are already Neon-first** — they write Neon, then Airtable, and are mirrors in everything but
name. They are not identity problems and carry no risk; each becomes a one-line
`mirrorToAirtable(...)` wrapper in whichever slice owns its table:

`handleUpdateEmployee` · `handleSetEmployeePin` · `handleSetEmployeeActive` ·
`handleUpdateJobStatus` · `handleJobAutomationResult` · `handleUpdatePowerCo` ·
`handleStartServiceCall` · `handleCompleteServiceCall` · `handleCalculateMileage` ·
`handleCreateInspectionContact` · `handleUpdateJobBillableRate` · `handleAddWarranty` ·
`handleUpdateJobNotes`

**21 are Airtable-first** — these are the work — plus `createJobRecord` (`_jobs.js`) and
`handlePushExpenses` (`inventory.js`):

| Slice | Handlers | Note |
|---|---|---|
| **1** reference leaves | `handleCreateVendor` · `handleCreateCompany` · `handleCreatePowerCompany` · `handleCreateContact` · `handleCreatePowerContact` | The last two write tables that are **already nullable** — they are Airtable-first out of habit, not constraint, and are the cheapest thing here. |
| **2** payroll | `handlePayrollRunCreate` | The files already went to R2 on the Neon uuid (`db/schema/052`). |
| **3** estimates + invoices | `handleSaveEstimate` · `handleCreateJobEstimate` · `handleUpdateEstimate` · `handleUpdateEstimateStatus` · `handleDeleteJobEstimate` · `handleSaveInvoice` · `handleSetInvoiceStatus` | All five estimate paths move together — `createEstimateFromTemplate` is the reason: a SECOND create path shipped a live 404 during the inventory cutover because only the open handler was checked. **Grep the POST to the table, not your open handlers.** |
| **4** expenses | `handleAddGeneralExpense` · `handleAddLiftExpense` · `handleUpdateExpense` · `handleApproveExpense` · `handleDeleteExpense` · **`inventory.js handlePushExpenses`** | Plus R2 receipt keys and `handleUnlinkedMaterialAllocations`. Own session. |
| **5** employees | `handleCreateEmployee` | The login id. Stale sessions must survive it. |
| **6** jobs | `createJobRecord` (`_jobs.js`) · `handleUpdateJobInfo` · `handleUpdateJobInspection` | Last. `job_inspections` is already nullable; the job link is what holds it here. |

## The pattern is already proven here

Nothing below is novel. It is the inventory app's item cutover, repeated:

- **The dual handle.** `db/schema/041` — inventory items carry *rec id else uuid*, and the
  comment says explicitly **do NOT tidy it away**. Every Neon read in the field app already
  returns `COALESCE(airtable_id, id::text)`, so the **read contract is already dual today**.
  Only creates are still Airtable-first.
- **Leaf-first ordering**, from the same cutover: a domain goes native only after everything
  that keys on its ids can accept a uuid.
- **A domain leaves the mirror the day it goes native** — not before (Make breaks), not later
  (two writers, two ids).

## Order of work

### ✅ Slice 0 — RUN 2026-08-21. Findings below.

**1. Make — PASSED, with one live exception.**
All 18 active scenarios checked via `usedModules` in the scenario listing (no blueprints needed —
the listing carries the module chain). **Not one has an Airtable trigger.** Every trigger is a
webhook, a Gmail watch, or a pCloud folder watch. So the earlier worry is dead: *retiring a mirror
write cannot stop a Make scenario, because no scenario is watching an Airtable table.*

Six scenarios still call Airtable inside their body:

| Scenario | State | Verdict |
|---|---|---|
| `4723276` Upload to pCloud — Estimates / Invoices / Generator | **LIVE.** Called straight from `index.html`. `CustomWebHook → ActionGetRecord@airtable ×2 → router → 3× pCloud upload`. 11 executions, 2 errors. | 🔴 **Must be converted to a payload before slice 3 and slice 6.** It re-reads the job and the estimate/invoice by rec id to build the pCloud path. When those go native the reads return nothing and **PDFs silently stop being filed** — Make's failure emails are the only monitoring. |
| `4729925` / `4739070` / `4739000` / `4735255` / `4739137` — the five Google syncs | Dormant. Webhook-triggered by Airtable automations that were **undeployed 2026-08-20**, so they now receive nothing (1 execution, lifetime). | Not a cutover blocker. They belong to item 07 (Google contacts) and will be rebuilt or retired there. |

Independent bonus: the four job scenarios report **zero** Airtable modules, confirming the
2026-08-20 conversion from the outside.

**2. R2 — PASSED, clean.**
Zero `rec` assumptions anywhere in `_r2.js`; grep found none. Every key-building call site already
passes either the client handle (`jobId`, `expenseId`) or a Neon uuid (`payrollPrefix(run.id)`,
`liftPrefix(target.id)`), so keys follow the handle automatically and **no work is needed here.**

> ⚠ One rule falls out of that: **never back-fill an `airtable_id` onto a row that went native.**
> Its handle would change from uuid to rec id and every R2 object already written under the old
> prefix would orphan — photos, receipts and prints, silently, with no error anywhere.

**3. ETL delete passes — PASSED.**
Exactly one destructive set-difference exists in the whole codebase (`_billing-sync.js:157/161`)
and it already carries `airtable_id IS NOT NULL`, added 2026-08-11 after it deleted every native
allocation within the hour. Every other sync is upsert-only. **Re-check before each slice** — a new
delete pass without the guard reintroduces the same silent loss.

**4. The clause sweep — this is what changed the plan.**
Swept every `WHERE …airtable_id = $n` that is a *lookup* (excluding write-backs that stamp the
mirror id, and clauses already dual): **77 lines across 62 functions.** They are not extra work —
a bare lookup only breaks when *that table's* ids go native — so they are a per-slice checklist.
But the distribution is nothing like the original estimate:

| Ids being resolved | Lines | Functions | Slice |
|---|---|---|---|
| **jobs** | **46** | **37** | 6 |
| **employees** | **19** | **18** | 5 |
| companies | 5 | 4 | 1 |
| payroll_runs | 3 | 1 | 2 |
| job_estimates | 3 | 3 | 3 |
| expenses · power_companies · labor_billable_rates · time_entries | 6 | 6 | 3–4 |

Slices 5 and 6 are roughly **three times** the size originally costed. Slices 1–4 are unchanged —
slice 4's difficulty was never its SQL.

**Recommendation that came out of this: add a generated `handle` column per table as it goes
native**, rather than hand-editing 46 job clauses into `OR id::text = $1`:

```sql
ALTER TABLE jobs ADD COLUMN handle text
  GENERATED ALWAYS AS (COALESCE(airtable_id, id::text)) STORED;
CREATE UNIQUE INDEX jobs_handle ON jobs (handle);
```

Every `WHERE airtable_id = $1` becomes `WHERE handle = $1`. Still 46 edits, but each is mechanical,
greppable and uniform, and a missed one is findable by grepping for the old column instead of
reading 37 functions. **This is the `db/schema/043` pattern** (`item_handle` / `location_handle` in
`v_stock_levels`), already proven in the inventory app.

### ✅ Slice 1 — SHIPPED 2026-08-21 (`41bd94c`, `db/schema/053`)

All five creates reversed: companies, expense vendors, power companies, contacts, power contacts.
The row is real the moment Neon has it; Airtable is best-effort.

**⚠⚠ It corrected the slice-0 recommendation, which was wrong.** A generated `handle` column of
`COALESCE(airtable_id, id::text)` holds ONE value — so the moment a best-effort mirror succeeds and
stamps `airtable_id`, the handle **flips** from uuid to rec id and every client holding the uuid
can no longer find the row. That is exactly the *saves fine, then cannot be found again* bug the
inventory cutover hit three times. **Use `WHERE airtable_id = $1 OR id::text = $1`** — it accepts
either, permanently, and it is already proven in `inventory.js`. A generated handle is safe only on
a table whose rows are native *forever*, which none of these are yet.

**⚠⚠ Companies are not a leaf, and the mirror has to stay.** `createJobRecord` posts
`Contractor: ["rec…"]`, an Airtable **linked-record** field; a uuid there 422s the whole job create.
So these tables keep minting rec ids until jobs go native in slice 6. *Leaf-first by Neon foreign
key is not the same as leaf-first by Airtable link field* — that is the lesson of this slice, and it
should be re-asked at every remaining one. The only case where a company has no rec id is "Airtable
was down when it was created", and in that state job creation is already impossible, so no new
failure mode was introduced.

**Two live bugs found on the way:**
- `handleListContractors` selected `WHERE airtable_id IS NOT NULL` — a native company would have
  been invisible to the picker that created it, the same bug `createPowerCompany` shipped on
  2026-08-12, lying in wait.
- The duplicate guards on companies and vendors read **Airtable**, which after the reversal both
  defeated the point (an outage threw there) and could not see a native row. Now Neon-first, backed
  by new unique indexes so a race cannot produce two rows with one name.

**New contract: these creates fail CLOSED without a database.** Every read of these tables is
Neon-first, so an Airtable-only row is invisible forever — nothing back-fills them. Three tests were
inverted to assert exactly that, and the Airtable field-id mappings they used to check are now
covered by source assertions, since the mirror is unreachable offline.

### ✅ Slice 2 — FLIPPED 2026-08-24 (`5328a0b`, `db/schema/056`)

**The gate below was met by the 2026-08-09 → 08-22 payroll run**, created 09:28 that morning —
the first time the R2 write inside `handlePayrollRunCreate` ever ran in production.

| check | result |
|---|---|
| `pdf_key` / `json_key` set | ✅ `payroll/825eab70…/20260824092819.pdf` (+ `.json`) |
| `r2Error: null`, `pdfArchived: true` | ✅ implied — see below |
| PDF opens from Payroll Archive | ✅ owner confirmed via the **Reprint** button |

Two things made those checks mean what they claim, and both are worth reusing in slices 4-6:

- ⚠ **The key shape is the fingerprint.** `20260824092819.pdf` is timestamp-named; the 28 runs
  backfilled on 08-21 are `att….pdf`, because `copyPayrollFilesToR2` keys on the Airtable
  attachment id. Seeing a timestamp key is how you know the in-handler write ran and not the
  copier.
- ⚠ **The keys ARE the evidence of the write, not a record of it.** They are stamped only after
  *both* `putBufferToR2` calls resolve, and explicitly cleared on any failure. Non-null keys
  therefore prove the PUTs succeeded — no R2 listing needed, which matters because the R2
  credentials are write-only Netlify secrets and cannot be listed locally.
- ⚠⚠ **The "PDF opens" check was nearly worthless.** `payrollRunsList` falls back to Airtable
  **wholesale** unless *every* run has a `pdf_key` — one null sends the entire grid back to
  attachment urls. Had any run been missing a key, clicking Reprint would have opened an Airtable
  attachment and proved nothing. All 29 rows had both keys, so the click really did exercise
  `presignGetDownload` against R2. **Check the fallback condition before trusting a UI check.**

**What shipped:** the create is Neon-first; Airtable is a `mirrorToAirtable(...)` whose rec id is
stamped back onto the run and each bonus. Two contracts inverted with it:

- **R2 became a hard precondition**, checked before anything is written. A native run has no
  Airtable record, so R2 holds the only copy of the artifact people are paid from. Unconfigured →
  503, nothing written. Failed PUT → the Neon row is deleted and the request 500s. The client
  already answers a throw with `showArchiveErrorModal()`, which offers a retry *and* still hands
  over the locally-generated PDF, so refusing costs nothing. R2 is "optional as a group" everywhere
  else in this codebase; on this path it is required.
- **The supersede flag is written to Neon unconditionally**, not only alongside the Airtable PATCH.
  A native predecessor has nothing to patch, and the chain then exists only in Neon.

#### ⚠⚠ "One handler, not a sweep" was WRONG — and the same undercount is likely in slices 4-6

054's header and this document both estimated slice 2 at *~20 minutes, one handler*. The flip
touched **four** handlers, and the three extra sites were all **silent wrong numbers, not errors**:

1. **`payrollBonusesRollup`** joined `r.airtable_id = b.payroll_run_airtable_id`. A native run has
   a NULL `airtable_id`, `NULL = NULL` is not true, and the LEFT JOIN then drops the row at
   `r.pay_period_end >= $1` — so every bonus on a native run vanishes from the year-to-date total.
   **This is the `v_invoices` bug from slice 3 in a plain handler.** The slice-0 note said "a VIEW
   can join on rec ids too"; the real rule is broader — *anything* can, so grep the handlers for
   `airtable_id =` as well as `pg_get_viewdef`.
2. **`payrollEmployeeBonusHistory`** — the same join, plus a bare `payroll_run_airtable_id` handed
   back as the run handle and a bare `b.airtable_id` as the sort tiebreaker.
3. **`findMatchingPayrollRun`** returned a bare `airtable_id` — the sharpest of the three. NULL for
   a native run, so the client reads `found.runId || null`, concludes there is no prior run for the
   period, and **skips the supersede confirm dialog entirely**. Two non-superseded runs on one
   period, which `computePayrollDateRanges` resolves by `generated_at` and gets wrong — moving
   every payroll tile by a fortnight. The period 2026-07-26 → 08-08 already carries six runs, five
   superseded, so this is the normal case, not an edge one.

**Method that caught them:** grep every SQL site touching the table, not just the handler being
flipped — `grep -n "payroll_runs\|payroll_bonuses" netlify/functions/*.js`. The flip handler itself
was the *least* interesting of the four.

**Method that made the swap safe:** each rewritten read was proved equivalent against live data
*before* shipping, with an `EXCEPT` both ways — 31 bonuses, 4 rollup rows, $12,900, zero diff. So
the change is provably inert today and correct once native runs exist. Every new statement was
verified as **`PREPARE name AS <sql>` with no type list**, per the correction slice 3 paid for.

**Schema 056** adds `pr_bonuses_run_uuid_idx` on `payroll_bonuses(payroll_run_id)`. The existing
index (052) was on `payroll_run_airtable_id` — the column both reads had just stopped joining on,
which would have left every payroll screen on a sequential scan. ⚠ **When a join column changes,
the index does not follow it.**

**Known cosmetic leak:** if the two R2 PUTs succeed but the `pdf_key` UPDATE fails, the rollback
deletes the run and leaves two objects under a uuid prefix nothing references. Harmless, invisible,
and not worth a transaction.

**Still true:** nothing re-reads Airtable to insert payroll rows — there is no ETL for these tables
(verified across `_*.js`, `qb-time-pull.js` and `db/etl/`) — so the slice-0 "failed stamp duplicates
the row" trap does not apply here. A failed stamp just leaves the run Neon-only, which every read
already handles.

<details>
<summary>The original gate, kept for the record</summary>

Everything around the create shipped; the create itself did not. This is not caution — it is one
piece of evidence:

**`handlePayrollRunCreate` stays Airtable-first because the R2 write inside it has never run in
production.** The 28 runs backfilled on 2026-08-21 went through `copyPayrollFilesToR2`, a
*different* path that downloads an Airtable attachment and PUTs it. The write in the create handler
(base64 from the browser → Buffer → presigned PUT) has never executed. A native run has **no
Airtable record and therefore no PDF attachment**, so R2 becomes the only copy — reversing now
would make the first real exercise of an untested write also the sole copy of a payroll PDF. Nor
can it be smoke-tested on demand: a payroll run is a fortnightly event, not a button you press
twice.

**The gate** (also written into `054`'s header and the handler). At the next real payroll run:

- the response carries `pdfArchived: true` and `r2Error: null`;
- `SELECT pdf_key FROM payroll_runs ORDER BY generated_at DESC LIMIT 1` is set;
- the PDF opens from the Payroll Archive tab.

Then the flip is **~20 minutes** — Neon INSERT first, the Airtable POST and its attachments become
a best-effort mirror, exactly as slice 1 did. One handler, not a sweep.

**What did ship, and is inert until then:**

- `NOT NULL` off `payroll_runs.airtable_id` and `payroll_bonuses.airtable_id`, so a native run is
  possible at all;
- all three run lookups accept **either** id form — supersedes resolution, the bonus-to-run link,
  and the supersede flag;
- `payrollRunsList` stops emitting a bare `airtable_id` as the run id, for both the run and its
  superseded-by pointer.

No native row can appear until the create is reversed, so none of the above can change behaviour
today. That is the point of splitting it: the risky half is one handler, and it waits on evidence
rather than on a calendar.

⚠ That last claim — "three run lookups accept either id form" — was the undercount. It covered the
three *write*-side lookups and missed the three *read*-side ones listed above. Splitting the slice
was still right; the estimate of what remained was not.

</details>

### ✅ Slice 2.5 — SHIPPED 2026-08-22 (`eb38e2e` + a Make blueprint edit)

Make scenario `4723276` — the pCloud PDF upload the app calls directly — no longer touches
Airtable. `usedPackages` is now `gateway, builtin, pcloud`, and operations per upload dropped
**5 → 3**.

**⚠ pCloud did NOT move and never will.** Make keeps the grandfathered connection; their
app-registration page is still down and no token can be issued. Only the two `ActionGetRecord`
modules were removed. This is the same replumb as `4509211` (job folders): *Make stays, its data
source changes.*

The two modules supplied exactly two strings for the folder path:

```
/Northeastern Electric Jobs/NEE Jobs/‹year›/‹contractorName›/‹jobNumber›/…
```

`jobNumber` was **already in the payload** (`job.po`), so module #2 was pure redundancy.
`contractorName` is now sent too, resolved from `state.jobs` when a caller omits it.

**⚠⚠ A CLAIM MADE IN `eb38e2e`'s COMMIT MESSAGE IS NOT PROVEN.** That message states the folder
path has been filing without its contractor level. The evidence is strong but circumstantial:

- module #7 read the **Companies** table (`tblSMTewjVSCVRb0J`) using the **job** id;
- `Contractor Name (Text)` exists on **Jobs** and **not** on Companies — verified against the live
  base schema;
- a job rec id queried against Companies returns **zero records** — verified live;
- yet the module's own cached sample is a **Jobs** record showing `KDC Properties`, and a cached
  pCloud sample from May shows a correct path *with* the contractor folder.

So the table on module #7 appears to have been changed from Jobs to Companies at some point after
May 2026, and executions have succeeded since. **Make's execution API returns only `SUCCESS` with
no per-module detail**, so the actual written path could not be read back. The definitive check is
the pCloud folder listing: if `NEE Jobs/2026/` contains job folders directly rather than contractor
folders, the claim holds. Ask before repeating it as fact.

The fix is correct either way — after this change the path comes from the payload, which is known
good.

### ✅ Slice 3 — SHIPPED 2026-08-22 (`db/schema/055`)

Estimates, sent-estimate PDFs and invoices are born in Neon. Seven handlers reversed:
`handleCreateJobEstimate` · `handleUpdateEstimate` · `handleUpdateEstimateStatus` ·
`handleDeleteJobEstimate` · `handleSaveEstimate` · `handleSaveInvoice` · `handleSetInvoiceStatus`.
All of them now fail **closed** without a database and mirror to Airtable best-effort.

**⚠⚠ THE FINDING THAT MATTERED: `v_invoices` JOINED ON REC IDS.** Its labor and material CTEs
resolved `invoice_airtable_id = i.airtable_id`. A native invoice has no rec id, so both would have
missed and `invoice_total_calc` — the figure the invoice screen and the printed PDF both use —
would have come out **zero for every T&M invoice**. Not an error, not a warning: a $0 invoice.
Same failure shape as the NULL `bill_rate` in `db/schema/036`. The view now resolves
`COALESCE(uuid, resolved-from-rec-id)` in every CTE, uuid **first** — a rec-id-first resolution
would drop exactly the newest work, because an allocation attached to a native invoice never gets
a rec id and the hourly sync will never fill one in.

`labor_billing_allocations` had no `invoice_id` at all (its material twin has had one since 033).
Added and backfilled: **1,221 of 1,221 attached rows resolved, zero orphans.**

**⚠ Airtable computed three of these columns, and one of them is GP.**

| Column | Formula | Why it matters |
|---|---|---|
| `estimated_labor_cost` | hours × **32.50** | `v_job_rollups` sums it into `est_labor_cost_rollup` — estimated GP. Left null, a job reads as more profitable than it is. |
| `calculated_estimated_total` | labor + material | Shown on every estimate. |
| `invoice_number` | `{Job} & "-" & RIGHT("000"&{Invoice Sequence},3)` | See below. |

Both estimate formulas were diffed against **all 89 estimates before any code was written — zero
mismatches** — and the derivation lives in SQL, not JS, so the create path and the partial-update
path cannot drift. An update that changes only material cost still recomputes the total from the
*stored* hours.

**32.50 is the prevailing-wage constant.** `docs/PLAN-prevailing-wage.md` is the project that
changes it, and this is now the only place the app decides an estimate's labor cost.

**`invoice_number` has always been `<job name>-001`, on every invoice ever written.** Its
`Invoice Sequence` counts the records in the invoice's own Job *link* field, which is always 1, so
Bethel School has two invoices both numbered `Bethel School-001`. Reproduced as-is: a cutover is
the wrong moment to change what a customer-facing document says. `invoice_display_no` is the real
number. Recorded in `docs/TODO.md`.

**⚠⚠ A MIRROR THAT HALF-SUCCEEDS CAN DUPLICATE AN INVOICE.** `syncInvoiceToNeon` is an
`INSERT … ON CONFLICT (airtable_id)`. If the Airtable POST succeeds and the stamp that records its
rec id then fails, the row is still native, nothing conflicts, and the carry-back writes a
**second invoice for the same work** — one native, one mirrored, both billable. The carry-back is
gated on `recId || stamped`, and `stamped` is set only by a stamp that actually returned.

**⚠ The stamp is safe on these tables, and the reasoning does not transfer.** Slice 0's rule —
never back-fill `airtable_id` onto a native row — is about **R2 keys**. Estimate PDFs in R2 are
keyed on the *Neon uuid* already (`estimates/<uuid>/…`, see `copyAirtablePhotosToR2`), and invoices
have no R2 objects at all: invoice PDFs go to pCloud from the browser, and scenario `4723276` has
taken its folder path from the **payload** since slice 2.5. Nothing downstream re-reads these rec
ids, so the handle may change. On a table with files it may not.

**Two live bugs found on the way, both silent:**
- `index.html` filtered the estimate back-links it sends with `startsWith("rec")`, so every
  estimate created after this slice would have been dropped from the link. The symptom is not an
  error: the snapshot saves, and the estimate it came from **loses its scope text** on the next
  load.
- `attachAllocationsToInvoice` treated "unattached" as `invoice_airtable_id IS NULL`. Left alone,
  every allocation sitting on a *native* invoice would have been re-attached — and re-billed —
  every time any invoice on that job was saved. It now requires **both** handles to be empty.

**⚠ `job_labor_allocations` IS NOT IN THIS SLICE, and the plan's scope table above is wrong about
it.** It is the weekly allocation table from `db/schema/004`; nothing in either function reads or
writes it, its only writer is `db/etl/time-entries-full.mjs`, and its newest row is 2026-08-09. It
has no create path to reverse, so its `NOT NULL` is vestigial exactly like `labor_cost_rates`.
Dropping it would buy nothing and would imply a native row is expected there.

**Verified before shipping, not after:**
- `v_invoices` old vs new, all 56 invoices: **zero diffs on every component**, total
  `$1,267,086.19` both ways.
- The estimate back-link change, all 89 estimates: **identical**, 23 with a snapshot before and
  after.
- **18 new parameterised statements PREPAREd against the live schema** — the offline suite cannot
  catch broken SQL, which is the lesson from the employees flip.
- 182 tests pass (3 new; 2 existing ones inverted, see below).
- Zero duplicates in `invoice_display_no` / `display_number` before adding the unique indexes that
  now back the MAX()+1 mint.

**Two tests were inverted rather than deleted.** `deleteJobEstimate` asserted that a uuid is
*refused* "because estimates are still Airtable-identity" — that guard would have made every
estimate created since this slice undeletable. The allocation attach test was updated to the
two-handle signature, plus a new case that a rec id **alone** is refused.


**🔴 FIXED 2026-08-23 — slice 3 shipped broken, and the VERIFICATION is why.**

The first click in production failed: `estimate.create: inconsistent types deduced for parameter
$5`. `$5` (labor hours) fed the `numeric` column *and* `COALESCE($5, 0)`, where the bare `0` is an
**integer** literal — and a parameter used twice must resolve to one type. Fixed with
`COALESCE($5::numeric, 0::numeric)`.

⚠⚠ **`PREPARE name(text, numeric, …) AS …` CANNOT CATCH THIS, AND THAT IS WHAT WAS RUN.**
Declaring the parameter types *resolves* the ambiguity before Postgres has to deduce it, so the
statement prepares cleanly and then fails on the first real call. The driver sends parameters
**untyped**.

> **Verify with `PREPARE name AS …` — no type list.** That is what the driver does. Re-run against
> all 17 of this slice's statements in that form: the estimate create was the only one affected.

Nothing was written — every reversed handler fails closed, so the estimate simply did not exist.
That half worked exactly as designed.

### Smoke test, 2026-08-23 — estimates PASS, invoices UNTESTED

Run by the owner on Classical Construction (Fuel Tank), then deleted.

✅ **Proven end to end**, and the `$5` bug above is what it caught first:

| | Result |
|---|---|
| `createJobEstimate` | Neon insert → Airtable mirror → rec-id stamp, all landed |
| The two formulas | `$325` labor / `$575` total — **and Airtable independently computed the same figures from its own formulas**, which is stronger evidence than the 89-row diff |
| `updateEstimate` | material 250 → 500 moved the total to `$825` and GP to 18% — the partial recompute reads stored hours, as intended |
| `updateEstimateStatus` | Sent reached both stores and moved Expected Revenue |
| `deleteJobEstimate` | gone from Neon **and** the Airtable mirror, with the old `startsWith("rec")` guard removed |

⬜ **Still dark, and not to be assumed working:**

- ~~**The whole invoice half**~~ — ✅ **MATERIAL HALF SMOKED 2026-08-24** on Test 2, invoice 1671,
  off the back of the slice-4c pushes. The full native chain ran: four Neon-native push expenses →
  `unlinkedMaterialAllocations` → `createMaterialAllocation` → the attach. **Four allocations
  created, every one `airtable_id NULL`, resolved by expense UUID**, totalling $39.74 —
  `invoice_material_amount` and `invoice_total_calc` both $39.74 against a $39.74 snapshot. That is
  the Bethel $34,937.50 pair holding on an end-to-end native chain, on a T&M job **with a prior
  invoice**, which is the exact configuration that hid the original bug.
  ⚠ `snapshot_total` round-tripped correctly through the Airtable mirror — worth checking every
  time, because it drives `Total Contract Billed` → `Previous Contract Billing` →
  `Contract Remaining`, the cap on what the next contract invoice may bill.
  🔴 **THE LABOR HALF IS STILL DARK, and its $0 here proves nothing** — Test 2 has **zero time
  entries**, so the labor line is correct by absence, not by test. The Bethel bug *was* the labor
  pair specifically. It still needs a T&M job with real hours.
- 📝 **Finding, benign, do not "fix" it by reading the column.** Invoice 1671's stored
  `invoice_total` is **0.00** while `invoice_total_calc` is the correct $39.74. `handleSaveInvoice`
  carries Airtable's computed columns back via `syncInvoiceToNeon` after mirroring, and Airtable's
  rollup **cannot see a Neon-native allocation**, so it will read 0.00 on every native-allocation
  invoice from now on. Harmless today: nothing reads it — every read in `airtable.js` uses
  `invoice_total_calc` (grep confirms three hits on the bare column, all writes or comments).
  ⚠ But note the inconsistency: the native INSERT leaves `invoice_total` NULL *on purpose*, with a
  comment saying a second decaying opinion of a total in a money column is how a wrong number gets
  quoted later — and then the carry-back writes 0.00 over it. ⬜ Worth dropping `invoice_total`
  from `syncInvoiceToNeon`'s column list so it stays NULL. Not done: it changes a money write path
  and nothing observable, so it is the owner's call.
- **`handleSaveEstimate`** (the sent-PDF snapshot). Reversed, never run. The next real estimate
  that goes out exercises it.
- **The native / uuid branch of every dual-handle lookup.** Everything tested carried a rec id
  because the mirror succeeded. That branch runs only when Airtable is unreachable at create time,
  so it cannot be staged in production — it gets its first real test during an Airtable outage,
  which is precisely when nobody wants a surprise. Worth a deliberate exercise on a sandbox base
  before slice 4 makes the same bet on expenses.

⚠ **Zero native rows exist**, so `git revert` is still a clean rollback of the whole slice.

### ✅ Slice 4c — SHIPPED 2026-08-24 (`db/schema/059`)

**The inventory app now makes zero authoritative Airtable writes.** `handlePushExpenses` was the
last one, and it existed for exactly one reason: Airtable was the identity authority for expenses.
Slice 4b ended that for the field app; this ends it for the push. Materials and sales-tax expenses
are born in Neon; the main base gets a best-effort mirror that nothing reads.

**What moved**

- **Guard #1 reads Neon.** It was a `filterByFormula` on `{Push ID}` — and *that* is the part of
  this slice that could have cost money. A native expense may never reach Airtable at all, so
  asking Airtable "has this push id already produced expenses?" would answer **no** about a push
  that had already charged, and the retry would charge the customer again. Proved equivalent
  before the swap: all 21 Airtable expenses carrying a `{Push ID}` diffed against Neon's `push_id`
  with `EXCEPT` **both ways, on (rec id, push id) pairs, not counts** — zero rows either direction.
  It fails **closed**: an unavailable guard is indistinguishable from "nothing has been pushed".
- **The two creates became one statement.** Materials and its tax row insert together
  (`createPushExpensesNative`). Two calls are two transactions, and a materials row that landed
  while its tax row failed is a job undercharged by 7.5% — with guard #1 then short-circuiting
  every retry, because the push id already "produced Expenses". One statement makes the half-state
  impossible.
- **Step E is gone, and its 502 inverted.** Step E healed an expense that reached Airtable but not
  Neon. That direction no longer exists. The 502 now reports the opposite: a group whose Neon
  insert refused, which means it was **not** charged and its transactions are still pending.
- **`syncExpenseToNeon` is no longer imported by `inventory.js`** and must not come back — see the
  ETL note below for what it does to a native row.

**⚠⚠ THE TRAP THIS SLICE ADDED THAT NO PRIOR SLICE HAD: a NULL `job_id` is not an error, it is a
discount.** `v_expenses` derives `billable_material_amount_calc` as
`manual_material_cost × (1 + COALESCE(j.markup_pct, 0))` **through `job_id`**. An expense whose job
does not resolve therefore inserts happily and prices the material **at cost** — the same shape as
the bug that billed a new job's first hour at cost (`a04b11f`), and invisible in exactly the same
way. The INSERT is a `CROSS JOIN` against a `j` CTE, so an unresolvable job writes **zero rows**
and the handler refuses the group. Verified live: 100.00 at a 10% markup → `110.00` billable
through `job_id`, and a ghost job id → 0 rows.

**⚠⚠ AND THE SLICE BROKE AN EXISTING ETL — `db/etl/expenses-backfill.mjs`, now hard-stopped.**
The 4b note said *"if an expense ETL is ever added it MUST skip rows it can't match by rec id"*.
One already existed. Its definition of "missing" is *Airtable rec id not present in
`expenses.airtable_id`*, and **every mirror of a native expense matches that, permanently, by
design** — because the rec id is deliberately never stamped back. Running it would
`syncExpenseToNeon` each mirror, `ON CONFLICT (airtable_id)` cannot fire on a NULL, and it would
**insert a second copy of spend already recorded**, both counting in GP.
⚠ **It cannot be fixed by filtering.** Nothing Airtable-side identifies a record as the mirror of
a Neon row; that back-pointer is the exact thing the R2 receipt-key rule forbids. So it refuses to
run, before it even reads `.env`.
> **The general rule, worth applying to slices 5 and 6:** the question is not "will I add an ETL?"
> — it is **"what already reads this table out of Airtable?"** Grep `db/etl/` for the table name
> in every remaining slice.

**Verification done before shipping**

- `PREPARE … AS <sql>` **untyped** for both the one-row and two-row shapes (a type list resolves
  the ambiguity the driver actually hits — it is not a check).
- Then **executed for real** and rolled back by hand, because `NOT NULL`/`CHECK` are runtime
  constraints that never fire on prepare. Confirmed: `airtable_id NULL`, job resolved, vendor
  resolved to "NEE Inventory", the four Airtable formula columns NULL, every `*_calc` correct.
- `expenses.airtable_id` NOT NULL was **checked, not assumed** (058 dropped it) — and so were
  `expense_pushes`, `expense_push_lines`, `inventory_transactions`, all already nullable. This is
  the check 057 skipped, which is why the first real expense after 4b failed.
- 12 push tests (three inverted for the new direction, four new), 279 across all four suites.

✅ **PROD-SMOKED 2026-08-24 on job "Test 2 (MIT 298)"** (Taxable, 10% markup — the job shape that
exercises materials, tax and markup in one push). Logged `Use 300`, `Return −300`, `Use 10` of
1/2" EMT PIPE and pushed. Every check passed:

- **The markup came through `job_id`** — $7.60 → **$8.36** and $0.57 → **$0.63**, both at 10%, in
  the field app's Expenses tab. This is the one failure mode of this slice that would have looked
  fine (a NULL `job_id` shows the cost with no error anywhere), so it was checked first.
- **Both rows born native** — `airtable_id NULL`, `job_id` resolved, `push_id` stamped, and the
  four Airtable formula columns NULL while the UI still showed the right money (schema 057's
  `*_calc` doing its job).
- **The pair is atomic** — identical `synced_at` to the millisecond (`11:52:47.841`), which is the
  single-statement insert holding.
- **Mirror created one second later** (`11:52:48`, `recPVkzemcaXz7G3D` + `recTveg4nbocTm2ZE`) —
  correct order, and the rec ids were **never stamped back**.
- **Netting works** — the three transactions collapsed to one push line of 10, all marked
  `expense_created` under the same push id.
- **Push history holds uuids**, not rec ids, in `expense_record_ids`.

⚠ Note the push id was *not* the risky path here: the mirror succeeded, so this run did not
exercise the "Airtable unreachable at create time" branch. That branch is covered by tests only —
same residual gap slice 3 recorded.

✅ **Second prod push, 2026-08-24 12:05 — the materials PDF now attaches to its expense.**
100 × 1/2" EMT COUPLING at $0.26 → $26.00 + $1.95 tax, billable $28.60 / $2.15, both native, both
inserted in the same millisecond, mirror one second later, rec ids never stamped back. **The
receipt appeared on the expense**, which is the leg that had no automated coverage — the presign
and the browser PUT can only be proved by a real push.

> ⚠ **Debugging note that cost a minute of false alarm:** a `left(description, 55)` in a *diagnostic
> query* clipped `×100` to `×10` and looked like a quantity bug. The stored value was 56 characters.
> When a verification query truncates, it is a **verification** artefact — widen the column before
> believing the data is wrong.

### Slices 4–6

Each slice is the same five steps, and they ship together in one commit:

1. `ALTER TABLE … ALTER COLUMN airtable_id DROP NOT NULL`.
2. The create handler writes **Neon first**, `RETURNING id`, and returns that id to the client.
3. The Airtable write becomes a `mirrorToAirtable(...)` — fail-soft, never blocking.
4. Any read that filters on `airtable_id` moves to the dual handle.
5. Smoke: create one, find it again, open its files, invoice it if it can be invoiced.

> ⚠ **Step 4 is where this bites.** The inventory cutover found three bugs of exactly one
> shape — *saves fine, then cannot be found again* — and all three were a `WHERE airtable_id = $1`
> that should have been the dual handle. **Grep the CLAUSE, not the table name.**

**Slice 4 (expenses) carries two extra jobs** and should be planned as its own session:

- R2 receipt keys move to the dual handle. Old receipts stay at `expenses/rec…/`.
- `inventory.js`'s `handlePushExpenses` is the **only remaining direct Airtable write in that
  app** and it exists solely because Airtable is the expense identity authority. It ends here.
- `handleUnlinkedMaterialAllocations` must move in the same commit — see below.

### ✅ Slice 5 — SHIPPED 2026-08-24 (`4bfabec`, `db/schema/060`)

**The stale-session risk turned out to be the easy half.** The plan's warning was right about the
exposure — the login id is persisted client-side *and* baked into a 30-day HMAC token, so every
phone in the field is holding a `rec…` — but `COALESCE(airtable_id, id::text)` on the emit side
disposes of it entirely: an employee who *has* a rec id emits that rec id, byte for byte. Verified
12/12 existing employees unchanged. Only a native hire ever yields a uuid.
> ⚠ **Never "simplify" the emit to a bare `id`.** That is the version of this slice that logs the
> entire crew out at once, and it looks tidier.

**The sweep was the work, and it was bigger than the estimated 19 clauses:**

- **15 handlers validated `String(employeeId).startsWith("rec")`** and would have 400'd a native
  hire on their own PIN screen, hours, rate history and the People screen — a flat "invalid
  employeeId" with nothing to suggest the id was fine and the guard was stale. Replaced with
  `isEmployeeHandle`, deliberately a **superset** so no rec id that works today can start failing.
  ⚠⚠ **This is the `b79b9a0` trap inverted.** That regression was handlers which *never* validated
  and silently forwarded an id; the note from it was "grepping `startsWith('rec')` is NOT
  sufficient". Also true in reverse — a guard that *does* validate hard-fails the new id form — and
  a grep for the guard finds only this half. **Both halves have to be swept.**
- 🔴 **`_revocation.js` keyed its map on a bare `airtable_id`.** NULL for a native hire, so their
  entry landed under the string `"null"` while their session carried a uuid: the lookup missed,
  `isSessionRevoked` answered "not revoked", and **deactivating that person would not have ended
  their session at all** — full access for the rest of a 30-day token while the admin watched the
  toggle flip. Security, not id tidiness.
- 🔴 **`updateEmployee` was `INSERT … ON CONFLICT (airtable_id)`.** Handed a uuid it conflicts with
  nothing and **inserts a second employee** whose `airtable_id` is that uuid string — same name,
  same PIN, `neonLoginCandidate` then sees two matches and refuses as `ambiguous`, locking **both**
  people out, having told the admin the edit saved. Now a plain `UPDATE` on the dual handle.
- **The crew picker filtered `airtable_id IS NOT NULL`** — which after this slice describes every
  native hire, making them silently unschedulable. Proved live: **10 active with the fix, 9 with
  the old filter.**
- **The duplicate-PIN check scanned Airtable**, where a native hire does not appear, so a second
  person could be given the same PIN. Reads Neon now and **fails closed** — guessing "free" is the
  answer that creates the collision.
- **Both `Submitted By` writes** put `authUser.id` into an Airtable **linked-record** field with
  `typecast: true`, which *creates* a record for an unknown value: a uuid there adds a junk person
  to Employees and every expense that hire files attributes to it. Gated on the rec-id form.
- **Three Airtable PATCHes** addressed `Employees/<id>` directly — a uuid 404s **after** the
  authoritative Neon write has landed, telling the admin it failed when it had not.
- **`inventory.js`'s own login stamp**, which a `FROM employees` grep misses because it is an
  `UPDATE`. ⚠ **Grep the clause, not the table name.**

**Verified:** `PREPARE … AS` untyped for the create and the update, then a real native employee
inserted, resolved by the actual login query, dual-resolved, and deleted. 286 tests across four
suites (4 new, 1 inverted).

⬜ **Not smoked on production.** The test is: add a person on the People screen, then log in as
them. Watch for the crew picker and their PIN screen, which are the two that would fail silently.

## The one genuine straggler

`handleUnlinkedMaterialAllocations` (airtable.js:8318) is the only handler in the field app with
no Neon path at all. It reads material allocations by job **name** out of Airtable.

Its twin, `handleUnlinkedLaborAllocations`, was fixed on 2026-08-11 after a real loss: it returned
rec ids while `timeEntries` had gone Neon-first and returned uuids, the two sets never intersected,
every T&M re-invoice silently proposed materials only, and **Bethel School's $34,937.50 of labor
was typed in by hand.**

The material one has not bitten **because every material allocation still has an Airtable twin** —
302 of 302, verified 2026-08-21, since allocations key off *expenses* and expenses still get rec
ids. **Slice 4 is exactly what breaks that.** So it is not a task before item 10; it is a line item
inside slice 4, and shipping slice 4 without it repeats a known $35k bug.

## What deliberately does not move

- **Existing R2 object keys.** `jobs/rec…/`, `expenses/rec…/` stay as they are. Those records keep
  their rec ids forever; the handle resolves.
- **The `rec…` ids already in the data.** Nothing is renumbered. `airtable_id` becomes history,
  not garbage.
- **Google contact ids** (`contacts.google_person_id_1/2`, `db/schema/049`) — a separate project,
  owner-gated on a Google Cloud project. 230 of 240 contacts already exist in Google across two
  destinations; syncing without those ids creates ~230 duplicates twice over.

## Then, and only then

Archive the base. Order: revoke nothing until the app has run a full pay period without an
Airtable read appearing in the logs, then set the PAT read-only for a week, then archive.

## Rough sizing

Re-costed 2026-08-21 **after** slice 0, which found slices 5 and 6 to be about three times the
size first estimated.

| Slice | Was | Now | Why it moved |
|---|---|---|---|
| 0 — verify | ~1 h | ✅ **done** | Make passed, R2 passed, delete passes passed. |
| 1 — reference leaves | ~1 h | ✅ **done 2026-08-21** (`41bd94c`) | Ran long: the SQL was 5 clauses, but two live bugs surfaced (see below) and three tests had to be inverted. |
| 2 — payroll runs + bonuses | ~45 min | ✅ **done 2026-08-24** (`160d944` + `5328a0b`, schema 054 + 056) | Prep 08-21, flip 08-24 once a real payroll run exercised the R2 write. ⚠ The "~20 min, one handler" estimate was wrong — it was four handlers, three of them silent-wrong-number reads. See the slice 2 section. |
| **2.5 — convert Make `4723276` to a payload** | — | ✅ **done 2026-08-22** (`eb38e2e` + Make edit) | Was gating slice 3. See below. |
| 3 — estimates, invoices, allocations | ~1.5 h | ✅ **done 2026-08-22** (`db/schema/055`) | Ran long. The 3 clauses were the easy part; `v_invoices` joined on rec ids and would have printed every native T&M invoice at $0, and three Airtable formula columns had to be reproduced. |
| 4 — **expenses** (own session) | ~2–3 h | ✅ **done 2026-08-24** — 4a `e071bd1`, 4b `a04b11f` + schema 057/058, 4c schema 059 | Ran long, and again not because of the SQL: 4b shipped without the `DROP NOT NULL` and the first real expense failed on it; 4c found an **existing** ETL that would have duplicated every native expense. ⬜ Prod-smoked for 4a/4b; the push (4c) still needs one real push. |
| 5 — employees | ~1 h | ✅ **done 2026-08-24** (`4bfabec`, schema 060) | The 19 clauses were ~30, and the stale-session risk the estimate was built around evaporated on one `COALESCE`. What ran long instead: 15 rec-id GUARDS, an ON CONFLICT upsert that would duplicate a person, and a revocation map that would not revoke them. |
| 6 — jobs | ~1.5 h | **~3–4 h** | **46 clauses across 37 functions** — the single biggest piece of the whole cutover. |

**~12–15 h.** The risk still concentrates in slice 4; the *labour* concentrates in slice 6.
