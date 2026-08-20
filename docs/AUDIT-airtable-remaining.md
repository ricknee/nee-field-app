# Audit — what is still on Airtable

*Run 2026-08-09 against live code, the Neon schema, all 39 Airtable automations and all 70 Make
scenarios. Not written from `ROADMAP.md` — this file exists because the roadmap says the field-app
migration is complete and it is not.*

Published copy (same content, easier to read):
https://claude.ai/code/artifact/e460eb0e-98c0-4066-8685-2858e882a1c2

---

> **Progress since the audit ran (same day):** item 01 ✅ `26d14c4`, item 08 ✅ `1b9a84d`. Both
> pushed. Everything else below still stands.

---

## ▶▶ STATE AS OF 2026-08-20 — start here, then read the 08-12 pass for context

Re-measured from the code with the same call-graph fixpoint, and from the live systems.

| | 08-12 | **08-20** |
|---|---|---|
| dispatched actions | 170 | **176** |
| Neon only | 42 | **48** |
| both stores | 97 | **103** |
| **Airtable only** | **28** | **22** |
| Airtable automations DEPLOYED | 26 | **0** |
| job Make scenarios touching Airtable | 4 of 4 | **0 of 4** |

**The 22 remaining Airtable-only actions are now just three things:** 20 R2 id lookups,
`payrollRunsList`, and `unlinkedMaterialAllocations`. Clear those and the field app has **no
Airtable-only reads left at all**.

### Shipped 2026-08-20

- **All four job Make scenarios are Airtable-free.** pCloud and Awarded proven live; Completed and
  Service Call pushed via the Make MCP and **not yet exercised**.
- Payroll bonuses → Neon · **Contacts → Neon (item 06 COMPLETE)** · next-number scans → Neon
- The app now owns what the automations used to do: PO allocation, **PO locking**, completion date,
  service-call status, and the run-once guards (`db/schema/044–046`).
- Google id groundwork (`db/schema/049`) — see the warning below.
- Fixes: Trello button, cross-company contact search, job intake block reaching Neon.

### Corrections to the 08-12 pass — it is wrong on these four points

1. **`Assign PO Number` was NOT harmless.** Its counter had already diverged (Airtable next 286,
   Neon last-used 286), so the next job created outside the app would have been issued a
   **duplicate PO** — and `po_locked` is what the QB puller matches on, so a duplicate silently
   un-costs the hours of *both* jobs.
2. **`unlinkedMaterialAllocations` is NOT on the unsafe `FIND` pattern** — it uses the
   newline-delimited form. `TODO.md`'s "four remaining sites" list is superseded by the completed
   sweep at the top of that same file.
3. **`handleUpdatePowerCo` was already fixed** (`70e4315`); TODO.md still lists it as suspected.
4. **The QB heartbeat is not a heartbeat.** `sync_state` is only written when QuickBooks returns a
   *changed* timesheet, so it is an activity signal. A dead schedule and a quiet afternoon look
   identical. `max(jobs.synced_at)` is written unconditionally and is the real liveness check.

### What is left, in order

| | Do | Size | Why here |
|---|---|---|---|
| **1** | **Prove Completed + Service Call** — one real job each | minutes, owner | Both were pushed blind. Service Call has the largest blast radius of the four (it builds the pCloud tree). |
| **2** | **Generator service call → Neon** | 2–3 h | ⚠ **DECISION FIRST:** the old `service_call_created` latch is PERMANENT — nothing clears it, so a generator gets ONE automatic call ever. Six are overdue (one since 2025-02) and would never be prompted again. Recommend guarding on *"is there an open service-call job for this generator"* instead, so recurring plans actually recur. Neon already computes `service_status` in `v_generators`; no Airtable needed. |
| **3** | **R2 id lookups → Neon** (20 handlers) | 1.5–2 h | **Measured, not estimated:** 10 are the identical 3-line existence check, 1 needs the job name. One shared helper. Lowest-risk item left — no data moves, no writes, and every site fails the same visible way (404). Also deletes an Airtable round-trip from every photo/print tab open. |
| **4** | **Payroll PDFs → R2, then `payrollRunsList`** | 2–3 h | The one genuinely blocked read — it needs a live attachment URL. |
| **5** | **Item 10 — drop the mirror writes** | 2–3 h **+ Make edits** | ⚠ Now much closer than 08-12 assumed: all four job scenarios read the payload, not Airtable. What still keys on a rec id: R2 receipt/photo keys, sent-PDF back-links, and every client-side job/expense/contact id. **Do item 3 first** — those 20 lookups are Airtable reads that must go before the mirror can. |
| **6** | **Google contacts direct (item 07)** | own session, owner-gated | ⚠⚠ **230 of 240 contacts ALREADY EXIST IN GOOGLE across TWO destinations.** Their ids are captured in `contacts.google_person_id_1/2` (schema 049). Syncing without them creates ~230 duplicates **twice over**. ⚠ Power contacts have **no per-person id at all** — that half must match by name/phone/email. ⚠ Which Google account is destination 1 vs 2 is recorded nowhere; resolve one known id in each before writing anything. Needs a Google Cloud project + OAuth + refresh token(s) from the owner. |
| **7** | **Inventory: archive the Airtable base** | ~1 h, owner | No code. Nothing reads or writes it. |

**Not on the list, deliberately:** `Invoice Paid`, `Send Invoice` and the auto-invoice half of
`Service Call Ready to Invoice`. The owner sets job status and marks invoices paid by hand in the
All Invoices tab, and the app writes "Sent" directly — the automation made **Draft** invoices and
there is exactly one Draft among 55. Reinstating automatic creation of financial records needs a
deliberate decision, not an assumption.

**Also open, low value:** 27 estimates on completed jobs still read "Approved" rather than
"Archived/Completed". Pre-dates today — the old automation used `limit: 1` and only ever archived
ONE estimate per job. Decide whether that distinction matters to GP before replacing it.

---

## RE-AUDITED 2026-08-12 (evening) — read this before the item table below

Second full pass, run from the code and the live systems rather than from this file. Every
dispatched action in both functions was re-parsed and classified by call-graph fixpoint (so a
handler counts as touching a store if *anything it calls* does), then every surprise was read by
hand. Airtable automations, Make scenarios, the Neon schema and row counts were all queried live.
The item table further down is a plan; this is the measurement.

**Both apps are much closer to done than the "~22-31 h field / 23-32 h inventory" line at the end
of that table suggests — the honest remaining figure is ~10-14 h of code across both.**

> ⚠ **Method note, because it changed an answer.** A naive "does this handler mention Airtable?"
> count is wrong in both directions. `addLiftExpense` / `addGeneralExpense` / `updateExpense` look
> Airtable-only until you follow `syncExpenseToNeon` through an **aliased import**; conversely
> `payrollBonusesRollup` looks migrated because it resolves employees from Neon, while it still
> pages Payroll Runs and Bonuses out of Airtable. Follow the calls.

### 🔴 Live, and not a migration item: FOUR job Make scenarios are OFF — one more than previously recorded

| Scenario | Last success | Then |
|---|---|---|
| `4509211` Automation – New Lead → Estimating (pCloud Folders) | **2026-08-12 09:18Z** (24 ops) | ❌ 10:17:24Z `BundleValidationError` |
| `4509804` Airtable – Job Awarded | **2026-08-12 10:03Z** (8 + 3 ops) | ❌ 10:17:24.955Z `BundleValidationError` |
| `4512438` Airtable → Trello → Completed by Year | **2026-08-11 21:33Z** (6 runs, 39 ops each) | ❌ 10:17:24.948Z `BundleValidationError` |
| `4545219` Airtable – Service Call | — | ❌ 10:17:36Z `BundleValidationError` |

All four carry Make's *"Fix the error or clear the queue"* warning and are **inactive**, so
`JOB_WEBHOOKS=app` is firing at four hooks that cannot receive: **no pCloud job folders, no Trello
card, no QuickBooks Time job, no Trello completed card.** The earlier record listed three and
missed `4512438` — which matters, because `4512438` is the one with the clearest proof the replumb
works: **six successful app-triggered runs on 2026-08-11 21:30–21:33**, then dead.

**This is not our payload, and that is worth knowing before anyone "fixes" the code.** All four
failed inside 12 seconds — 10:17:24.948 / .955 / :24 / :36 — with the identical error, **2
operations and 5 bytes transferred** against 15–71 KB on healthy runs. `_job-webhooks.js` cannot
produce that: `fireJobStatusWebhooks` fires **at most one** branch per call (status is one of
Estimating / Awarded / Completed) and the service-call hook is a separate entry point, so four
near-empty POSTs to four different hooks in the same second is an **external ping, not app
traffic**. Recovery is Make-side — clear each queue, reactivate — with **no code change**.

> ⚠ **The real code lesson is the silence.** `post()` in `_job-webhooks.js` swallows every failure
> to `console.error` on purpose, so that a dead webhook can never fail the status change the user
> actually asked for. That is the right trade — and it means this outage ran for **a day with no
> signal anywhere**. Whatever else happens, the four webhooks need something that notices.

**Good news in the same data:** the 09:18Z, 10:03Z and 08-11 21:33Z successes close every ⬜
*"never fired since undeploy"* gap on item 04 — **the replumb is proven**, contractor-name fix
included.

### ✅ One latent bug, same class as `createCompany` — FIXED 2026-08-12

**`createPowerCompany` wrote Airtable only, but `getPowerCompanies` reads Neon first, and nothing
anywhere writes `power_companies`.** A power company added in the app was therefore invisible to the
picker that created it — permanently, not for an hour. It had not bitten only because nobody had
added a utility since the flip (9 rows in each store). Now mirrors to Neon in the same request,
failing soft, `ON CONFLICT` so a retry is safe.
`createPowerContact` and `createContact` are the *other* two Airtable-only writes, but both are
**consistent** — their reads are still Airtable too — so they are unmigrated, not broken.

### Measured state

| | field app (`airtable.js`) | inventory app (`inventory.js`) |
|---|---|---|
| dispatched actions | **170** | **58** |
| Neon only | 42 | **48** |
| both stores | 97 | 8 |
| **Airtable only** | **28** | **1** (a debug endpoint) |
| neither (pure R2/config) | 3 | 1 |
| **Airtable-only writes** | **2** (`createContact`, `createPowerContact`) | **0** |
| reads/writes to the Airtable **inventory** base | — | **0** |

**The field app's 28 Airtable-only actions are four domains, and only one of them is real work:**

- **20 are R2 id lookups** — `jobPhotos*`, `jobPrints*`, `jobDocs*`, `expenseReceipt*`. Every one
  does the same thing: `fetchAll(TABLES.jobs, {filter: RECORD_ID()="…"})` purely to prove the job
  or expense exists before hitting R2 (see `handleJobPhotos`, `airtable.js:10533`). **No business
  data crosses.** Neon holds all 113 jobs and 401 expenses with `airtable_id`, so this is a query
  swap — and it also deletes a **full Airtable round-trip from every photo/print tab open**, which
  makes it a latency fix as much as a migration one. ~2-3 h for all twenty.
- **4 are Contacts** — `listContactsByCompany`, `createContact`, `getContactsForPowerCompany`,
  `createPowerContact`. ⚠ **This is the only remaining domain with no Neon home at all** —
  confirmed against the live schema: 61 tables and 27 views, and neither `contacts` nor
  `power_contacts` is among them. Item 06's last slice, and it needs a loader, not transcription.
- **2 are next-number scans** — `getNextEstimateNumber` pages Airtable's *Sent Estimate PDFs*
  (`airtable.js:6015`), `getNextInvoiceNumber` the same shape. Neon already holds both
  (`sent_estimate_pdfs` 25, `invoices` 54). Same problem the PO counter solved with
  `job_po_counters`; safe **today** only because estimates and invoices are still created
  Airtable-first. ⚠ **Whichever of those goes Neon-native first breaks the numbering**, exactly as
  allocations broke when time entries lost their twin.
- **1 is `payrollRunsList`** — genuinely blocked, and it is the *only* genuinely blocked one. It
  reads the run's **PDF attachment URL** off the Airtable record, so it cannot move until payroll
  PDFs move to R2.
- **1 is `unlinkedMaterialAllocations`** — verified consistent: `createMaterialAllocation` is
  Airtable-first and mirrors, so this read cannot miss rows. ⚠ It does still use the unsafe
  `FIND(jobName, ARRAYJOIN({Job}))` cross-job pattern — one of the sites in `docs/TODO.md`.

**Two more that a handler count hides.** `payrollBonusesRollup` (`:3639`) and
`payrollEmployeeBonusHistory` (`:3692`) classify as "both" because they resolve employees from
Neon — but they still `fetchAll` **Payroll Runs and Payroll Bonuses out of Airtable** for the
actual figures. Neon already has that data backfilled (`payroll_runs` 28, `payroll_bonuses` 31),
so these are a **query swap of ~1-2 h**, not a migration, and they are the cheapest real win left
in the field app. The previous pass counted them with `payrollRunsList` as "gated on payroll PDFs";
they are not — only `payrollRunsList` is.

**Inventory is finished, not "effectively finished."** `INVENTORY_BASE_ID` is no longer read
anywhere in `inventory.js` — the only mention is the comment recording its retirement. **Zero reads
and zero writes to the Airtable inventory base.** Its single Airtable-only action,
`getExpenseFields`, is a **debug endpoint** that calls the Airtable *metadata* API to print field
ids (`inventory.js:1132`); it is not a data dependency and can be deleted whenever. The eight
main-base actions (`jobs`, `employees`, `login`, `estimatingJobs`, `awardedJobs`,
`templateContractors`, `pendingExpenses`, `pushExpenses`) are all Neon-first with an Airtable
fallback. **What is left for this app is not code — it is archiving the base.**

**Make: 14 active of 70.** 8 are the vendor-invoice/pCloud email robots (Home Depot, Lowe's,
CED ×3, Wolff ×3), 5 are the Google contact syncs (item 07), 1 is the grandfathered
browser→pCloud upload hook `4723276` — which is the single Make URL in `index.html`, confirmed
alongside the fact that **neither frontend calls `api.airtable.com` directly at all**.
**None of the four job-lifecycle scenarios is active** — see the red
block above.

### Airtable automations: 26 deployed, not 34

Re-listed live. 39 exist; **13 are undeployed**, including all four billing-allocation ones (item
03) and all four job-webhook triggers (item 04) — both cutovers confirmed from the Airtable side,
not just from the commit messages.

| Group | # | Note |
|---|---|---|
| Job lifecycle | 9 | PO numbering, PO-locked ×2, completed date, complete-checked, estimate status, service-call-ready, contractor field, power-company link. ⚠ `Assign PO Number` is still deployed but **stands down by itself** — item 05 writes the PO in the same POST, so its "PO is empty" condition is false. |
| Google contact sync | 6 | The 5 syncs plus `Create Contact from Job Intake`. Item 07. |
| Money & status | 6 | Invoice Paid · Send Invoice · Time Entries Paid · Set Expense Approved / Not Reviewed · **`wflMz8yO4iqhoy3cq` "Create Entry in Job Labor Allocation Table copy"** — still deployed, firing on Airtable Time Entry changes, writing the **labor-COST** allocation table. `006_true_labor_cost` computes labor cost from the time entries directly and nothing in `airtable.js` reads that table any more (`TABLES.laborAllocations` is the *billing* table, and only as a fallback). Dead weight rather than a hazard, but confirm before undeploying. |
| Dead wire/pipe | 4 | Auto-Assign Wire/Pipe Price · Wire Weigh-In → Expense · Pipe Usage → Expense. Dead since April 2026. **The free win: ~10 min, zero risk.** ⚠ Keep `legacy_material_cost`. |
| Generator service | 1 | Folds into generators, already in Neon. |

### The mirror-write surface — what item 10 actually costs

The field app still writes **~28 distinct Airtable tables**, concentrated rather than spread:
Jobs (10 write sites) · Expenses (5) · Time Entries (4) · Employees (4) · Job Estimates, Fleet
Maintenance, Scissor Lifts, Invoices, Generators, Warranties, Schedule Entries (3 each) · then a
long tail of ones and twos. That is the trigger bus described in point 2 below, and the reason
item 10 is a sequence rather than a commit.

### Health, checked live

- **QB puller heartbeat is good** — `sync_state.qb_timesheets` updated 26 min ago,
  `fetched=2 upserted=2 deleted=0`. This is the daily 2-minute check and it passes.
- ⚠ **`qb-time-pull` is the only scheduled function, and it now carries three jobs**: the QB
  timesheet pull, `_jobs-sync.js`'s hourly jobs refresh, and `_billing-sync.js`. One dead schedule
  silently stops all three, and only the first has a documented heartbeat.
- Row counts, all as expected: 14,657 time entries (**47 with no Airtable twin** — the post-08-07
  native rows, growing as designed) · 401 expenses · 113 jobs · 54 invoices · 2,794 labor + 302
  material billing allocations (6 Neon-native) · 866 inventory items · 869 ledger transactions.

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
| **Companies + Contacts** (customer master) | `Companies`, `tbl7vZpySDNfZX9Sq` | 4 → **5** (`createCompany` added; there was no create path at all) | ✅ Companies read Neon-first + mirrored on create; ⬜ Contacts | ~2-3 h (Contacts) |
| **Power Companies + Contacts** | `tblgxHavdZybnuMhM`, `tblvouoPMTYh27FGT` | 4 | ✅ done `1554909` | — |
| **Vendors** (main base — *not* the inventory base's Vendors) | `Vendors` | 2 | ✅ done `e957527` | — |
| **Labor Billable Rates** | `Labor Billable Rates` | 1 | ✅ done `e728274` | — |
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
| **02** | 🟨 **Payroll Runs + Bonuses → Neon — MOSTLY DONE 2026-08-11 (`5d163dd`, `db/schema/034`).** ✅ `computePayrollDateRanges` (the hot path, called first by all four payroll handlers) and `handleFindMatchingPayrollRun` are Neon-first; `handlePayrollRunCreate` mirrors run + bonuses + supersede in the same commit. 28 runs / 31 bonuses backfilled, verified by three independent sums. ⬜ **Two reads remain:** `handlePayrollRunsList` needs the PDF **attachment URL**, so it is blocked on moving payroll PDFs to R2 (the `copyEstimatePdfsToR2` job); `handlePayrollBonusesRollup` joins employees + role filtering and is its own slice. ⚠ "Five handlers, no derived formulas" was right about the formulas and wrong about the attachments. ⚠ **Re-audit 2026-08-12 evening splits this row**: `handlePayrollBonusesRollup` **and** `handlePayrollEmployeeBonusHistory` are NOT blocked on the PDFs — they `fetchAll` Payroll Runs + Bonuses from Airtable while the data already sits in Neon, so they are a ~1-2 h query swap. Only `handlePayrollRunsList` needs the attachment URL. | ~1-2 h + ~2-3 h (PDFs) |
| ~~**03**~~ | ✅ **DONE + CUT OVER 2026-08-11.** All four automations undeployed, `ALLOCATIONS_WRITE=on`, app owns allocations. `docs/PLAN-billing-allocations.md`. ⚠⚠ **It exposed a bigger hole than it closed:** since Step 3 stopped minting Airtable ids (2026-08-07), time entries have no twin — **100% of the week of 08-10** — and allocations were keyed on those ids, so **no labor logged after 08-07 could be billed at all**. Fixed by going Neon-native (`db/schema/033`) + an `airtable_id IS NOT NULL` guard on the sync's delete pass. **Assume the same trap anywhere else still keyed on a rec id.** | — |
| ~~**04**~~ | ✅ **DONE 2026-08-12** (`f39fb22`, `JOB_WEBHOOKS=app`). All four undeployed; **no Airtable automation triggers a job webhook any more.** Awarded was proven by counting **Make executions** — both POSTs arrive, one works and one no-ops on the flags — which beats the circular "undeploy and hope" the plan originally proposed. ⚠⚠ **Still true and now urgent for item 10:** three of the four Make scenarios read the job back **out of Airtable**, so this moved the TRIGGER only. ⚠ The same award exposed a 4th "flip a read without its write" — `handleUpdateJobStatus` was Airtable-only against a Neon-first read (`ff21d46`). ⚠⚠ **"each is a fetch with a matched payload" was only half right:** three of the four send little more than `recordId` and Make reads the job back **out of Airtable**, so this moves the TRIGGER only. Item 10 additionally needs the Make payloads for Completed + Service Call enriched — a Make-side edit. `docs/PLAN-replumb-job-webhooks.md` | ~1 h left |
| ~~**05**~~ | ✅ **DONE 2026-08-12** (`JOB_CREATE_SOURCE=neon`, `db/schema/039`). Job creation writes Neon and allocates the PO number; the one-hour new-job lag is gone. ⚠ **The gate on 04 was wrong** — pCloud triggers on *Estimating* and jobs are created as *New Lead*, so creation never fired that webhook. ⚠ **No undeploy was needed:** writing the PO in the same POST makes the automation's "PO is empty" condition false, so it stands down by itself — reusable pattern. ⚠⚠ It surfaced that we were sending Make a **rec id where it expected a name** (linked fields are ids over REST, names via `getCellValueAsString`), which broke pCloud folder creation live. | — |
| **06** | 🟨 **Reference data — four of five slices DONE 2026-08-11\12.** ✅ Billable Rates (`e728274`), expense Vendors read+write (`e957527`), Companies + the contractor picker (`d00f2c0`), Power Companies (`1554909`). ✅ **`createCompany` (2026-08-12)** — the slice-3 flip exposed that **nothing in either app could create a company**, only read one, while `handleCreateJob` *requires* a contractorId: the first new contractor after Airtable closed would have been unable to have a job created at all. Added behind the New Project picker's "+ Add new contractor". ⚠ It also surfaced that **`Contractor (Intake)` is a singleSelect with typecast off**, so an unknown contractor name 422s the whole job create — guarded by `CONTRACTOR_INTAKE_OPTS`; all 24 current contractors happen to be options, which is why it never bit. ⬜ **Contacts remain** — needs a loader, not transcription, and both Contacts tables are **Google-sync triggers**, so their Airtable mirror writes stay until item 07. | ~2-3 h left |
| **07** | ⚠ **OWNER PREFERS THE OTHER PATH — 2026-08-12:** *"on the google contacts if we can go direct with a google api i would rather move that way. but thats another session."* So the ~2 h replumb-to-Make is **not** what to build; the target is **4c-3**, a native Neon → Google People API sync replacing all five scenarios. That needs a Google Cloud project + OAuth consent + refresh token **created by the owner** before any code. Replumbing remains the cheap fallback if the OAuth route stalls. ⚠ Note two of the five scenarios have **never once fired**, so nothing is currently working that would stop working. | deferred — own session |
| ~~**08**~~ | ✅ **BUILT 2026-08-09 `1b9a84d`, prod-smoked** — inventory Step B0, the cross-base reads. ⚠ Uses `po`, not `po_locked` — the latter is blank on all 13 New Leads. | — |
| **09** | **Inventory Steps A-E** — ✅ **ALL DONE, and the write cutover with them.** Re-verified 2026-08-12 evening: `inventory.js` makes **zero reads and zero writes** to the Airtable inventory base, and no longer references `INVENTORY_BASE_ID`. Only archiving the base is left (~1 h, no code). ⚠⚠ on-hand is **derived** now: the Stock Levels cache had drifted from the ledger on 237 of 269 pairs and was deliberately NOT ported, so stock reads lower and raises more alerts — that is the correction, not a bug. Conduit assemblies not migrated (owner: build native in Neon). | ~6-8 h |
| **10** | **Undeploy the dead wire/pipe automations, then drop the mirror writes.** Only once 04, 05 and 07 have landed *and soaked*. **This is the step that actually ends Airtable's role.** ⚠⚠ **NEW PREREQUISITE, found 2026-08-12:** the Completed and Service-Call Make scenarios receive only `recordId` and read the job **out of Airtable**, so dropping the job mirror breaks them even with 04 complete. **Their Make payloads must be enriched first — a Make-side edit, not code here.** Assume the same shape anywhere else a scenario is handed only an id. | 2-3 h **+ Make edits** |

~~**Field app remainder: ~22-31 h. Inventory: 23-32 h on top.**~~ **Superseded by the
2026-08-12 evening re-audit — see the running order below.**

### ▶ What is actually left, in order (re-audited 2026-08-12 evening)

Sized honestly. Rows 1-2 are not migration work at all, and they come first because one is broken
in production right now and the other is free.

| | Do | Size | Why here |
|---|---|---|---|
| **1** | 🔴 **Recover the four dead Make scenarios** — clear each queue, reactivate | ~10 min, **owner, in the Make UI** | Broken in production *today*: no pCloud folders, no Trello cards, no QB Time jobs. Not a code fault — see the red block. Then give the four webhooks something that **notices**, because this ran a full day silently. |
| **2** | **Undeploy the 4 dead wire/pipe automations** | ~10 min | Dead since April 2026. Zero risk, zero dependency. ⚠ Keep `legacy_material_cost`. |
| **3** | **Payroll bonuses → Neon** (`payrollBonusesRollup`, `payrollEmployeeBonusHistory`) | ~1-2 h | Data is **already backfilled** (28 runs / 31 bonuses). Pure query swap; the cheapest real win left. |
| **4** | **R2 id lookups → Neon** (20 handlers) | ~2-3 h | Mechanical, no data moves, and it removes an Airtable round-trip from every photo/print tab open. |
| **5** | **Next-number scans → Neon** | ~1 h | ⚠ **Do this BEFORE estimates or invoices go Neon-native**, or the numbering breaks the way allocations did. |
| **6** | **Contacts → Neon** (item 06 last slice) | ~2-3 h + loader | The **only** domain with no Neon table. Its mirror writes must survive until item 07. |
| **7** | **Payroll PDFs → R2, then `payrollRunsList`** | ~2-3 h | The one genuinely blocked read, and the block is the attachment URL. |
| **8** | **Google contacts (item 07)** | own session | Owner-gated on a Google Cloud project. Nothing currently working stops working. |
| **9** | **Item 10 — drop the mirror writes** | 2-3 h **+ Make edits** | Still last, still gated on 6 + 8 soaking, still needs the Completed/Service-Call Make payloads enriched first. |
| **10** | **Inventory: archive the Airtable base** | ~1 h | No code. Nothing reads or writes it. |

**≈10-14 h of code across both apps**, plus the owner actions and the soaks. The old
"~22-31 h field / 23-32 h inventory" figure predates the inventory write cutover and counted the
20 R2 id lookups and the payroll bonus reads as domain migrations rather than query swaps.

---

## How this was checked

**First pass (2026-08-09)** classified every handler by whether its body calls `atFetch`/`fetchAll`,
references Neon, or both.

**Second pass (2026-08-12 evening)** re-did it properly: both functions were parsed into a **call
graph** and the store flags propagated to a **fixpoint**, so a handler counts as touching a store
if anything it transitively calls does — including through **aliased imports**, which the first
pass missed and which changed three answers. All 170 + 58 dispatched actions were classified; every
disagreement with this document was then read by hand at the source. Airtable write sites were
extracted by table to size the mirror surface. Live: Neon's 61 tables + 27 views listed and queried
for row counts and the puller heartbeat; all 39 Airtable automations re-listed with deployment
status; all 70 Make scenarios filtered on `isActive`, and execution histories pulled for the four
job scenarios; Make hooks listed; both frontends grepped for direct Airtable and Make calls.

**Resolved since the first pass:**

- `index.html` posts to **exactly one** Make hook, and neither frontend calls `api.airtable.com`.
- `inventory.js` no longer references `INVENTORY_BASE_ID` at all, so the inventory-base PAT that
  gated ~25 h of work **no longer gates anything**.

**Still stated but not proven**, neither changing the plan:

- what the custom script inside `wflGOWii6JG6qpk21` — "Create Contact from Job Intake" — posts to
- what sent the four near-empty POSTs at 10:17:24Z. Ruled out as app traffic on the code; not
  traced to its actual source.
