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

### ⚠ Slice 2 — PREPPED 2026-08-21 (`160d944`, `db/schema/054`), **NOT FLIPPED**

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

⬜ **Not smoke-tested.** The money path needs a person: create an estimate, save its PDF, reopen
the job and check the scope text came back, invoice it, confirm the labor and material lines are
non-zero, mark it paid.

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

**Slice 5 (employees)** changes the login `id`, which is persisted client-side. Every browser
holds a `rec…`. The token carries it too. So the dual handle has to work on a **stale session**,
not just a fresh login — verify by logging in, flipping, and *not* re-logging-in.

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
| 2 — payroll runs + bonuses | ~45 min | ⚠ **prepped 2026-08-21** (`160d944`); ~20 min left | Everything but the create shipped. The flip is gated on the next **real** payroll run exercising the R2 write — see the slice 2 section. |
| **2.5 — convert Make `4723276` to a payload** | — | ✅ **done 2026-08-22** (`eb38e2e` + Make edit) | Was gating slice 3. See below. |
| 3 — estimates, invoices, allocations | ~1.5 h | ✅ **done 2026-08-22** (`db/schema/055`) | Ran long. The 3 clauses were the easy part; `v_invoices` joined on rec ids and would have printed every native T&M invoice at $0, and three Airtable formula columns had to be reproduced. |
| 4 — **expenses** (own session) | ~2–3 h | ~2–3 h | Difficulty was never the SQL. R2 needs no work at all (slice 0). |
| 5 — employees | ~1 h | **~2–3 h** | 19 clauses across 18 functions, plus stale-session verification. |
| 6 — jobs | ~1.5 h | **~3–4 h** | **46 clauses across 37 functions** — the single biggest piece of the whole cutover. |

**~12–15 h.** The risk still concentrates in slice 4; the *labour* concentrates in slice 6.
