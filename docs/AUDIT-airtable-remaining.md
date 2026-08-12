# Audit — what is still on Airtable

*Run 2026-08-09 against live code, the Neon schema, all 39 Airtable automations and all 70 Make
scenarios. Not written from `ROADMAP.md` — this file exists because the roadmap says the field-app
migration is complete and it is not.*

Published copy (same content, easier to read):
https://claude.ai/code/artifact/e460eb0e-98c0-4066-8685-2858e882a1c2

---

> **Progress since the audit ran (same day):** item 01 ✅ `26d14c4`, item 08 ✅ `1b9a84d`. Both
> pushed. Everything else below still stands.

## The four things that matter most

**1. ~~Do now — the payroll drill-downs serve a frozen table.~~ ✅ FIXED 2026-08-09 (`26d14c4`).**
Both handlers are Neon-first now. The other four readers of that frozen table were checked at the
same time and are all legitimate fallbacks (`payrollEntriesFromAirtable`, `hoursByJobFromAirtable`,
and two sitting behind a `neonQuery`) — **the bug class is closed, not just the instance.**

**2. Never forget — the Airtable mirror writes are the trigger bus.** All ten Airtable-touching
Make scenarios fire from *Airtable record changes*. Drop the mirror writes before replumbing the
hooks and pCloud folders, Trello cards, QuickBooks Time jobs and Google contact sync all stop —
**silently, no error anywhere**. This is a constraint to carry, not a task to schedule. It has to
survive until step 10 below.

**3. Two ten-minute owner actions gate ~25 h of work.** A PAT scoped to the inventory base, and
finishing the Jobs-mirror sync freeze in the Airtable UI. Neither is code. Both have been open
since 2026-08-08.

**4. ~~Next real build — the billing-allocation write path.~~ ✅ DONE + CUT OVER 2026-08-11.**
Nobody has to open Airtable in normal operation any more.

> ⚠⚠ **And it found the trap that now applies to everything else on this list.** Retiring Make
> from a path silently stops minting the **Airtable rec ids that other paths key on**. Time
> entries have had no twin since 2026-08-07 — 100% of the week of 08-10 — and allocations were
> keyed on those ids, so no labor logged since could be billed by *any* mechanism, old or new.
> **Before flipping anything else here, ask what still keys on a rec id** — R2 receipt keys,
> sent-PDF back-links, and every remaining mirror write do.

> Nothing on this list ends Airtable by itself. That is step 10, and it is gated on 04, 05 and 07
> landing and soaking first. **The finish is a sequence, not a pile.**

---

## 1. One live bug — ✅ FIXED 2026-08-09 (`26d14c4`)

*Kept for the record: it explains the failure mode, which will recur the next time a table is
frozen. **Freezing a table does not find its readers for you.***

`handlePayrollHoursBreakdown` (`airtable.js:3269`) and `handleMyHoursBreakdown` (`:3449`) both call
`fetchAll(TABLES.timeEntries)` — the Airtable Time Entries table, **frozen by Step 3 on
2026-08-07**. The rollup tiles above them serve Neon; tapping a tile to see the detail serves the
frozen copy.

Verified in Neon: of the rows added since 2026-08-07, **none carry an `airtable_id`**. Nothing has
written to Airtable since and nothing will. Two rows today, growing with every timesheet.

`v_hours_daily` and `v_hours_by_job` already exist — this is a query swap, not a migration.

**Related, same slice:** `computePayrollDateRanges` (`:2906`) pages the Airtable **Payroll Runs**
table to find the pay-period boundary, and all four payroll handlers call it first. That is the
400-600 ms gap between `_ms` and wall time the roadmap noticed. It is an unconditional Airtable
round-trip on the hot path, not a fallback.

---

## 2. Field app — six domains that were never on the roadmap

Complete Airtable-native domains with no Neon table behind them at all.

| Domain | Airtable table | Handlers | Neon | Est. |
|---|---|---|---|---|
| **Payroll Runs + Bonuses** | `tbln9nU1BtFmTYMYB`, `tblpE3emzU3J1P5jx` | 5 + `computePayrollDateRanges` | none | 4-6 h |
| **Companies + Contacts** (customer master) | `Companies`, `tbl7vZpySDNfZX9Sq` | 4 | none | 3-4 h |
| **Power Companies + Contacts** | `tblgxHavdZybnuMhM`, `tblvouoPMTYh27FGT` | 4 | none | 2-3 h |
| **Vendors** (main base — *not* the inventory base's Vendors) | `Vendors` | 2 | none | ~2 h |
| **Labor Billable Rates** | `Labor Billable Rates` | 1 | ⚠ **table exists, 6 rows, never read** | ~30 min |
| **Job creation** | `Jobs` | 1 | table exists, not written | 3-4 h |

### Job creation is the awkward one

`handleCreateJob` (`:9132`) POSTs to Airtable and returns. **It never touches Neon.** The job
appears in Neon up to an hour later via `_jobs-sync.js` — which is why a new job shows an empty
Time Entries tab for its first hour.

It is also the most entangled handler in the system. Creating a job fires **five** Airtable
automations: PO number assignment, PO-locked fill, contractor field, contact creation, and the
pCloud folder webhook. Moving it means reimplementing PO numbering *and* replumbing a Make webhook
in the same commit.

### Lower priority — eight handlers that hit Airtable only to resolve an id

The photo, print, doc and receipt handlers each read one Airtable Jobs or Expenses record to turn a
name into a record id for the R2 key. No business data crosses. Near-zero risk, and near-zero value
until the mirror actually goes away. ~2-3 h.

---

## 3. Inventory app — untouched, and the largest single block

`inventory.js` has 49 handlers. **48 call Airtable and not one references Neon.** There are no
inventory tables in the Neon schema. Base `appfsLJwfow4CepCw` holds 18 tables.

`docs/PLAN-inventory-to-neon.md` Steps A-E still hold. Two gates:

- **Step A is an owner action, not code.** Airtable exposes no API for sync configuration, so
  freezing the Jobs mirror's three sync sources is a manual UI change, then a 48 h soak. Pre-flight
  passed 2026-08-08; nothing destroyed.
- **No usable credential.** Re-tested 2026-08-09 — **neither PAT in `.env` can read the inventory
  base**, both return `INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND`. Steps B, C and D all need one.

**Step B0 needed neither and is ✅ BUILT** (`1b9a84d`, 2026-08-09) — the five main-base Jobs reads
plus `handleEmployees`, onto Neon data that already existed. ⬜ Needs a prod smoke test.
⚠ It reads `po`, **not** `po_locked`, which is blank on all 13 New Leads.

---

## 4. The hidden layer — 34 live Airtable automations, 4 of them tracked

The roadmap names four (the billing-allocation set). There are **34 deployed**, holding real
business logic that has to exist somewhere after Airtable stops being the database.

| Group | # | What | Where the logic goes |
|---|---|---|---|
| **Job lifecycle** | 9 | Assign PO Number to New Jobs · Fill Job PO — Locked · …— Locked Service Call · Stamp Project Completed Date · Project Complete Checked · Update Job Estimates Status on Completion · Service Call Ready to Invoice · Update Contractor Field · Power Company — Jobs Linked | Into `handleCreateJob`/`handleUpdateJobStatus`, or Postgres triggers. **PO numbering is job identity — that is the hard part.** |
| **Money & status** | 6 | Invoice Paid · Send Invoice · Time Entries Paid · Create Entry in Job Labor Allocation · Set Expense Status Approved / Not Reviewed | Mostly already duplicated by the app's own writes. Needs a diff, then undeploy. |
| **Billing allocations** ⭐tracked | 4 | `wflTwXb6dG32FFv9s` · `wflNmJsnIhWtSjUlL` · `wflOcxtmkzdxKMVQW` · `wfl7bzJpZY9kcJ27i` | The roadmap's 4-6 h item. Neon holds 2,606 labor + 252 material allocation rows. |
| **Fires a Make webhook** | 10 | see §5 | Replumb the caller, keep the scenario. |
| **Generator service** | 1 | Opens a service call on non-OK generator status | Folds into generators, already in Neon. |
| **Dead wire/pipe path** | 4 | Auto-Assign Wire Price · Auto-Assign Pipe Price · Wire Weigh-In → Expense · Pipe Usage → Expense | Dead since April 2026 — materials come from the inventory app now. Safe to undeploy. ⚠ **Keep `legacy_material_cost`** or the 24 pre-April expenses recompute differently. |

Five further automations are already **undeployed** (three Job Intake helpers, an OLD
labor-allocation one marked DO NOT USE, and Disable Auto Allocate for Contract Invoices). Nothing
to do — but Airtable lists them beside the live ones, so don't let a later audit re-find them.

---

## 5. Make.com — the webhooks

70 scenarios exist; **18 active**. Eight never touch Airtable — the vendor-invoice email runs that
file PDFs into pCloud (Home Depot, CED, Lowe's, Wolff). Outside this migration entirely.

The other ten are all triggered the same way: an Airtable automation runs a script that POSTs to a
Make webhook.

| Make | Scenario | Hook | Fired by | Disposition |
|---|---|---|---|---|
| `4509211` | New Lead → pCloud folders | `2582248` | `wfltqVP8ORwHh2Mnx` | replumb inside `handleCreateJob` |
| `4509804` | Job Awarded → QB Time + Trello | `2582557` | `wfl2KJpZRPK1tDz5D` | replumb inside `handleUpdateJobStatus` |
| `4512438` | Trello → Completed by Year | `2584241` | `wflP3hvinWk4saqmX` | replumb, same handler |
| `4545219` | Service Call | `2605544` | `wflMovlr8seWxSUul` | replumb inside `handleStartServiceCall` |
| `4729925` | Awarded contacts → Google | `2718975` | `wflBmTGXNAwxXG2Fv` | 4c-3 group |
| `4735255` | Power Co. contacts → Google | `2718733` | `wflhZbXfIbns2Bdqk` | 4c-3 group |
| `4739000` | Inspection contacts → Google | `2719349` | `wflbtN86fsDRA9FY3` | 4c-3 group |
| `4739070` | Inspection agencies → Google | `2719380` | `wfliwo7Nbv6yTLZT8` | 4c-3 group |
| `4739137` | Vendor contacts → Google | `2719435` | `wflAtdSKB80kqjvjJ` | 4c-3 group |
| `4723276` | Upload estimates/invoices/generator PDFs to pCloud | `2709756` | *no matching automation* | likely already app-called — `index.html:3780` posts to a Make hook directly. Confirm which. |

### ⚠⚠ The dependency nobody wrote down

Every one of these fires off an **Airtable record change**. The app still mirror-writes jobs,
expenses, estimates, invoices and generators to Airtable, and **that mirror is what keeps them
firing.**

The day the mirror writes are dropped, pCloud job folders stop being created, Trello cards stop
appearing, QuickBooks Time stops getting new jobs, and Google contact sync goes quiet — **with no
error anywhere**. Each webhook must be replumbed to a direct call from the Netlify function
*before* its mirror write is removed, not after.

### A cheaper path than the plan for 4c-3

Step 4c-3 is blocked on creating a Google Cloud project + OAuth consent + refresh token, so one
native Neon → Google sync can replace all five scenarios. That is the right end state but it is
**not required to leave Airtable**.

All five triggers are plain HTTP webhooks. Replumbing them — posting the same payload from the
Netlify function instead of from an Airtable automation — is **~2 h, needs no Google credentials,
and lets Make keep doing the Google half**. The Airtable dependency disappears; the OAuth project
becomes optional cleanup rather than a blocker.

> ⚠ **`isPaused` is NOT Make's activation flag — `isActive` is.** `isPaused` reads `false` on all
> 70 scenarios including long-retired ones. Any audit filtering on it reports every scenario as
> live.

---

## 6. The work list

Ordered by what unblocks what, then by risk. Only 01 and 02 have a real reason to go first.

| # | Item | Size |
|---|---|---|
| ~~**01**~~ | ✅ **DONE 2026-08-09 `26d14c4`** — payroll drill-downs point at Neon. Bug class closed, not just the instance. | — |
| **02** | 🟨 **Payroll Runs + Bonuses → Neon — MOSTLY DONE 2026-08-11 (`5d163dd`, `db/schema/034`).** ✅ `computePayrollDateRanges` (the hot path, called first by all four payroll handlers) and `handleFindMatchingPayrollRun` are Neon-first; `handlePayrollRunCreate` mirrors run + bonuses + supersede in the same commit. 28 runs / 31 bonuses backfilled, verified by three independent sums. ⬜ **Two reads remain:** `handlePayrollRunsList` needs the PDF **attachment URL**, so it is blocked on moving payroll PDFs to R2 (the `copyEstimatePdfsToR2` job); `handlePayrollBonusesRollup` joins employees + role filtering and is its own slice. ⚠ "Five handlers, no derived formulas" was right about the formulas and wrong about the attachments. | ~2-3 h left |
| ~~**03**~~ | ✅ **DONE + CUT OVER 2026-08-11.** All four automations undeployed, `ALLOCATIONS_WRITE=on`, app owns allocations. `docs/PLAN-billing-allocations.md`. ⚠⚠ **It exposed a bigger hole than it closed:** since Step 3 stopped minting Airtable ids (2026-08-07), time entries have no twin — **100% of the week of 08-10** — and allocations were keyed on those ids, so **no labor logged after 08-07 could be billed at all**. Fixed by going Neon-native (`db/schema/033`) + an `airtable_id IS NOT NULL` guard on the sync's delete pass. **Assume the same trap anywhere else still keyed on a rec id.** | — |
| **04** | 🟨 **Replumb the four job-lifecycle webhooks — BUILT, 1 of 4 CUT OVER 2026-08-12** (`f39fb22`, `JOB_WEBHOOKS=app`). ✅ pCloud folders: app-only, automation undeployed, proven on WatersEdge 1. ⬜ Awarded, Completed, Service Call still doubled — safe indefinitely, since Make's guard flags no-op the second call; undeploy each as a real job hits that status. ⚠⚠ **"each is a fetch with a matched payload" was only half right:** three of the four send little more than `recordId` and Make reads the job back **out of Airtable**, so this moves the TRIGGER only. Item 10 additionally needs the Make payloads for Completed + Service Call enriched — a Make-side edit. `docs/PLAN-replumb-job-webhooks.md` | ~1 h left |
| **05** | **Job creation → Neon, with PO numbering.** Reimplements five automations. Kills the one-hour new-job lag. Gated on 04. | 3-4 h |
| **06** | **Reference data:** Companies, Contacts, Vendors, Power Cos, Billable Rates. Five small domains, no money formulas. Billable Rates is nearly free. | 8-10 h |
| **07** | **Replumb the five Google-contact webhooks.** Removes the last Airtable-automation triggers. | ~2 h |
| ~~**08**~~ | ✅ **BUILT 2026-08-09 `1b9a84d`, prod-smoked** — inventory Step B0, the cross-base reads. ⚠ Uses `po`, not `po_locked` — the latter is blank on all 13 New Leads. | — |
| **09** | **Inventory Steps A-E** — 🟨 **A, B0, B, C and E are DONE and prod-smoked (2026-08-10/11); only D (estimating) remains, ~6-8 h.** Being run in a separate session. ⚠⚠ on-hand is **derived** now: the Stock Levels cache had drifted from the ledger on 237 of 269 pairs and was deliberately NOT ported, so stock reads lower and raises more alerts — that is the correction, not a bug. Conduit assemblies not migrated (owner: build native in Neon). | ~6-8 h |
| **10** | **Undeploy the dead wire/pipe automations, then drop the mirror writes.** Only once 04, 05 and 07 have landed *and soaked*. **This is the step that actually ends Airtable's role.** ⚠⚠ **NEW PREREQUISITE, found 2026-08-12:** the Completed and Service-Call Make scenarios receive only `recordId` and read the job **out of Airtable**, so dropping the job mirror breaks them even with 04 complete. **Their Make payloads must be enriched first — a Make-side edit, not code here.** Assume the same shape anywhere else a scenario is handed only an id. | 2-3 h **+ Make edits** |

**Field app remainder: ~22-31 h. Inventory: 23-32 h on top.**

The field app is closer to done than the raw handler count suggests — 64 of 166 handlers are
already Neon-first, and a third of the Airtable-only list is nothing but id lookups for R2 keys.

---

## How this was checked

Every handler in `airtable.js` (166) and `inventory.js` (49) was parsed and classified by whether
its body calls `atFetch`/`fetchAll`, references Neon, or both; the Airtable-only set was then read
individually to separate genuine dependencies from mirror writes and id lookups. Neon's 58 tables
and views were listed live and queried for row counts. All 39 Airtable automations in
`appiqWg6SvKcGfMAu` were listed with triggers and deployment status. All 70 Make scenarios were
listed and filtered on `isActive`. Both `.env` PATs were tested against `appfsLJwfow4CepCw`.

**Two things stated but not proven here**, both five-minute checks, neither changing the plan:

- which Make hook `index.html:3780` targets (one of the pCloud upload scenarios, most likely
  `4723276`)
- what the custom script inside `wflGOWii6JG6qpk21` — "Create Contact from Job Intake" — posts to
