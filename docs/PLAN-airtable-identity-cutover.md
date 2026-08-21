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
| `job_estimates`, `invoices`, `job_labor_allocations` | 3 | Money. `sent_estimate_pdfs` is already nullable and points at estimates. |
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

### Slice 0 — verification only, no code (~1 h)

Three things must be *measured* before slice 1, not assumed:

1. **Which Make scenarios still trigger on an Airtable record.** The mirror writes are the Make
   trigger bus. All four job scenarios are payload-driven as of 2026-08-20, but the rest of the
   18 active scenarios have not been re-checked, and the scenario list carries no base ids —
   deciding needs **blueprints**. A retired mirror silently stops whatever still watches it, and
   Make's failure emails are the only monitoring.
2. **Every R2 key-building site.** `jobPrefix()`, `expensePrefix()`, `payrollPrefix()`,
   `liftPrefix()`, `fleetPrefix()` plus the validators (`assertKeyInExpense`, `isPrintKey`,
   `isDeletedReceiptKey`). Each must take the *client handle*, not an assumed `rec…`. Existing
   objects keep their rec-id paths forever — that is correct and must not be "fixed".
3. **Every ETL delete pass.** Only one exists (`_billing-sync.js:157/161`) and it is already
   guarded with `airtable_id IS NOT NULL`, added 2026-08-11 after it deleted every native
   allocation within the hour. Every other sync is upsert-only. **Re-check this before each
   slice** — a new delete pass added without the guard reintroduces the same silent data loss.

### Slices 1–6

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

| Slice | Size |
|---|---|
| 0 — verify (Make blueprints, R2 key sites, delete passes) | ~1 h |
| 1 — reference leaves | ~1 h |
| 2 — payroll runs + bonuses | ~45 min |
| 3 — estimates, invoices, job labor allocations | ~1.5 h |
| 4 — **expenses** (own session: R2 keys, inventory push, material allocations) | ~2–3 h |
| 5 — employees (stale-session verification) | ~1 h |
| 6 — jobs | ~1.5 h |

**~9–11 h**, and slice 4 is where the risk concentrates.
