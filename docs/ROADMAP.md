# Roadmap — getting off Airtable

**The goal:** Airtable stops being the database. Neon (Postgres) holds the data, the two apps
read and write it directly, and Make.com is reduced to the handful of jobs only it can do.

**This file is the running order.** If something isn't on it, it's a detour — see §7.

*Last updated 2026-08-05.*

---

## 1. Where you actually are

| System | State |
|---|---|
| **Time entries** | ✅ In Neon, and **authoritative for writes since 2026-08-05** — all four app write paths are Neon-first, Airtable is the mirror. QB Time pulls straight in. |
| **Jobs master data** | ✅ In Neon (112 jobs), read by 4 app endpoints. Identity columns refresh **hourly** since 2026-08-05; the other ~22 master columns still need a hand-run ETL. |
| **Payroll reads** | ✅ **Served by Neon — confirmed on production 2026-08-05.** All three of `handlePayrollHoursRollup` (`_ms` 90), `handleMyHoursRollup` (65) and `handlePayrollEntries` (60) returned `_source:"neon"`, with the rollup figures matching Neon exactly. |
| **Job list / GP** | 🟨 Whole GP layer ported to Neon views and diffed to **zero** mismatches — but `handleJobs` still reads Airtable, so none of it serves the app yet. ~1 h to flip. |
| **Everything else** (estimates, invoices, expenses, fleet, generators, inspections) | ⬜ Still Airtable |
| **Jobsite photos** | ✅ Never touched Airtable — R2 from day one |
| **Inventory app** | ⬜ Still Airtable, still coupled to the main base via the Jobs mirror |

**You are mid-flight on one track and haven't started the other.** That is the state to be aware
of: two systems hold time data, kept honest by a reconciler. It's safe, but it isn't finished.

---

## 2. The one rule that sets the order

> **Make cannot be retired until payroll reads leave Airtable.**

Kill Make first and Airtable stops receiving QB time, while the payroll rollups are still reading
Airtable. So the order is forced: **linkage → payroll reads → payroll writes → retire Make.**

Everything else is negotiable. That isn't.

---

## 3. The path — field app

### ▶ NOW — verification only (~1 h, spread out) · until ~Aug 9

No building. Confirms what shipped on 2026-07-30 actually works before anything is stacked on it.

- ✅ **Payroll smoke test + PDF — DONE 2026-08-03.** A run was created and
  `NEE_Payroll_2026-08-08.pdf` saved. Every line reconciled against Neon: Jeff Koehn 49.3167
  actual → 49.50 paid (Reg 40 / OT 9.50); Patrick Gingerich 40.9833 → 41.00 (Reg 40 / OT 1, city
  tax 39.50 A-No-Tax + 1.50 Columbiana = 41.00); three salaried employees rendered correctly;
  week 2 empty and Neon agrees. Reg+OT sums to the total and city-tax hours sum to the paid
  figure in every case. The weekly round-up is therefore smoke-tested and lands in the
  employee's favour, as intended.
- ✅ **City-tax requirement — CLARIFIED 2026-08-03.** All the business needs is **hours worked per
  city**; the tax accountant handles withholding itself. The current rule (assign the weekly
  round-up to the jurisdiction with the most hours) satisfies that, and the round-up is ≤0.25 h.
  > ⚠ **Still untested by real data:** nobody this period had a round-up *and* split
  > jurisdictions at once. Jeff had a round-up but one jurisdiction; Patrick had two jurisdictions
  > but needed no round-up. The allocation rule has never actually fired. Worth a glance the first
  > period it does.
- ✅ **Reconciler run and CLEAN — 2026-08-05.** All 9 acceptance checks pass across a Make 21:00
  boundary: 14,590 rows and 51,570.25 h on both sides, 0 bucket mismatches, 0 field-level drift.
  **No double-counting.** Getting there took fixing check 5 — see the job-link bug in `3f3048a`.
  Keep running it; this is the gate for retiring Make.
- ✅ **Write mirror exercised — DONE 2026-08-05.** Closed by the Step 2 smoke test, which ran all
  four write paths on production and confirmed each in Neon. It had been open since the mirror
  shipped alongside the broken driver.

> 🛑 **STOP POINT — and a good one.** Everything works, Make is intact, nothing is half-done in a
> way that decays. This state is stable indefinitely.

### ✅ Step 1 — Payroll reads move to Neon — **DONE 2026-08-05**

*Why now:* it's the forced next link (§2), and it's smaller than it looks — `handlePayrollEntries`
returns no billing fields, so the labor-billing layer isn't involved.

1. ~~Backfill `airtable_id` onto puller-created rows by natural key~~ — **done**, linker lives in
   `db/etl/time-entries-full.mjs` and runs in both modes
2. ~~Decide the canonical entry id~~ — **settled by what shipped**: Neon uuid is the PK, `airtable_id`
   is carried alongside for as long as Make writes the mirror
3. ~~Flip `handlePayrollEntries` to Neon-first + Airtable fallback + `_source`~~ — **done**
4. ~~Flip the payroll rollups and my-hours reads the same way~~ — **done**
5. ~~Confirm `_source:"neon"` on real production responses~~ — **done 2026-08-05.** All three
   returned `_source:"neon"` against `hub.northeasternelec.com`: `payrollHoursRollup` `_ms` 90
   (716 ms wall), `myHoursRollup` 65 (473), `payrollEntries` 60 (218). Figures reconciled against
   Neon directly — YTD 6466.25 h, month 49.75, week 49.75, and my-hours 466.75 all exact.

> **Two things learned doing the verification, worth keeping:**
>
> - **`_source:"neon"` is weaker evidence than it looks on the two rollups.** Their guard is
>   `if (q?.rows?.length)`, but the queries are pure aggregates — Postgres returns exactly one row
>   even when it sums nothing, so those two can *never* fall back and an empty Neon would report
>   `"neon"` with zeros. Always reconcile the hours, not just the label. `handlePayrollEntries` has
>   the honest form of the guard (`q?.rows` on a row-returning query) — though note `if (q?.rows)`
>   still treats zero rows as authoritative, which is correct here only because an empty window is
>   a legitimate answer.
> - **`_ms` is the Neon leg only, not the request.** Both rollups call `computePayrollDateRanges`
>   first, which still pages Airtable's Payroll Runs table to find the pay-period boundary. That's
>   the ~400-600 ms gap between `_ms` and wall time — not a fallback. It's also the next thing that
>   would have to move if these reads are ever to be Airtable-free.
>
> ⚠ **One check still owed:** the production `payrollEntries` call landed on pay period
> **2026-08-09 → 2026-08-22**, which hasn't started, so it correctly returned 0 entries — meaning
> its row *mapping* is still unproven on prod. Next time the payroll screen is open, run it over
> `2026-07-26 → 2026-08-08` and expect **67 entries / 201.75 h**. Wiring is proven; this is data.
>
> **Why the current pay period is empty:** the last non-superseded payroll run has Pay Period End
> 2026-08-08 — the run created during the 2026-08-03 smoke test — so `computePayrollDateRanges`
> points "current" at the *next* period. The Pay Period tile therefore reads 0 until Aug 9. That is
> pre-existing Airtable-driven logic, nothing to do with Neon.

> 🛑 **STOP POINT.** All time reads served by Neon, writes still Airtable-first. Fully reversible —
> the fallbacks stay in place.

### ✅ Step 2 — Payroll writes move to Neon — **DONE + SMOKE-VERIFIED ON PROD 2026-08-05**

All four write paths (`handleCreateTimeEntry`, `handleUpdateTimeEntryPayroll`,
`handleUpdateTimeEntry`, `handleDeleteTimeEntry`) now write **Neon first**; Airtable is the
fail-soft mirror. Verified by running the real handlers against a **Neon branch** with Airtable
stubbed — 28 checks, all passing, every statement actually executed.

**Three contract changes came with it. All three matter more than the flip itself:**

1. **Writes now fail CLOSED** (`neonWrite` in `_neon.js`). Reads still fail soft — that is
   correct for reads, where Airtable serves a correct-but-slow answer. It is *wrong* for writes
   now: a write that lands in Airtable but not Neon is invisible, because every payroll read
   comes from Neon. Adding time with Neon down returns an error instead of quietly half-saving,
   and leaves nothing behind in Airtable.
2. **The entry id the UI edits by is now the Neon uuid**, not the Airtable rec id. This was
   pulled forward deliberately — see the note below. Write handlers resolve **either** form, and
   that is permanent, not a shim: the Airtable read fallback still returns `rec…` ids by design.
3. **`--repair` is DISABLED** in `db/etl/time-entries-full.mjs`. It updates Neon *from* Airtable,
   which was right when Airtable was authoritative and is now precisely backwards — it would
   overwrite correct payroll data with stale mirror values, from a routine daily command. Drift
   detection is untouched. Repairing Airtable *from* Neon is the correct direction now, but the
   ETL holds a read-only PAT, so that needs a write credential and its own decision.

> **Why the uuid switch happened now rather than at Step 4.** The Airtable rec id stops being
> minted at **Step 3**. The puller never creates Airtable time-entry rows — Make does. Turn Make
> off and every new QB timesheet lands in Neon with `airtable_id` NULL permanently, so a UI keyed
> on the Airtable id would find every new entry uneditable the day Make is retired. Keeping the
> old contract would have meant rewriting the same four payroll write paths twice, two hours
> apart. `unlinked` therefore demotes from "uneditable" to a plain health signal, and is expected
> to be permanently non-zero after Step 3.

**Schema:** `te_has_a_key` relaxed to accept `source = 'Manual'` as a third origin — it required
an Airtable or QB id, which a Neon-native row does not have at insert time (`002_qb_puller.sql`).

**✅ Smoke-tested on production 2026-08-05.** All four paths exercised on the live payroll screen
and each confirmed in Neon by direct SQL, not by what the screen said:

- **create** → `840e3efb-c93d-41d6-84f4-6058ef7dca56`, Rick Unruh 2026-08-05, 9000 s = 2.5 h.
  **The first time entry in the system's history born in Neon rather than copied into it.** It
  carries a uuid id, `airtable_id` stamped back from a successful mirror, `source = 'Manual'`
  (without today's constraint change this insert would have been rejected), `job_name` snapshotted
  from `po_locked`, and the generated `hours` column correct.
- **delete** → tombstoned into `time_entries_deleted` at 11:11:39, not silently dropped.
- **Labor Reviewed** and **edit hours** → both persisted across a refresh.

Totals reconcile exactly: 14,590 rows before and after (−1 delete, +1 create), hours
51,570.25 → 51,571.25 = −2.0 deleted + 2.5 added + 0.5 from the edit. `unmirrored` is 0, so
Airtable is fully in step.

> **The smoke test ran against live data in the open pay period** (`2026-07-26 → 08-08`) — a 2.0 h
> deletion on 07-30, a 2.5 h addition on 08-05, and a 0.5 h edit. **Owner reviewed and accepted
> these 2026-08-05: they do not matter, do not re-raise them.** Noted only so a later reconcile
> does not "discover" them as unexplained drift. The deleted row is tombstoned in
> `time_entries_deleted` (`rec8ocf0vN7Ijagvl`) if it is ever wanted back.

**This also finally closes the "watch the write mirror" item** that had been open in §3 NOW since
the mirror shipped alongside the broken driver and had never been exercised.

> 🛑 **STOP POINT.** Neon authoritative for time. Airtable still written, still correct, still a
> working fallback.

### Step 3 — Make leaves the time path (~2 h) 🎉

Only after **several consecutive clean reconciler days.** Turn off scenario `4546051`. Keep the
Airtable table and Make's other ~69 scenarios. Rotate the QB `tsheets` credentials once it's off —
rotating sooner breaks Make.

> 🛑 **STOP POINT — the original goal of this whole migration.** Time data flows QB → Neon → app,
> with no Airtable and no Make in the path.

### Step 4 — The remaining field-app domains

Each is its own slice on the same pattern: **mirror → read-flip → write-flip → retire.** Ordered
by risk, cheapest and safest first, so the pattern is proven on something that can't cost money
before it's used on something that can.

| # | Domain | Why here | Rough size |
|---|---|---|---|
| **4a** | **Fleet + Lifts** | Simplest in the app. Few fields, no money formulas, no Make involvement, no cross-table rollups. If the migration pattern has a flaw, find it here. | ~3-4 h |
| **4b** | **Inspections, Generators, Warranties** | Reference-shaped data with dates and links. Still no GP maths. Generators carry the service history, so slightly more relational. **The job service visit log (`PLAN-job-warranty-service-log.md` §3) belongs here** — build it Neon-native as part of this slice rather than in Airtable first. | ~4-5 h + ~4-6 h |
| **4c** | **Expenses** | Money, but plain arithmetic rather than rollup formulas. Already has the `Push ID` idempotency pattern. Receipts (`PLAN-expense-receipts.md`) land here, so do them together if receipts hasn't shipped by then. | ~4-6 h |
| **4d** | **Estimates + Invoices** | **LAST, deliberately.** These carry the GP and live-profit formulas — the numbers the business runs on. | large |

> ⛔ **Hard constraint on 4d:** every GP and live-profit formula must be reproduced as Neon views
> and reconciled against Airtable *before* anything Airtable-side is retired. Not after, not
> alongside. `docs/GP-FORMULA-INVENTORY.md` is the checklist; a previous sweep found the inventory
> was silently dropping 5 of 28 rollups, two of them GP-critical — so the checklist itself has
> been wrong before and gets re-verified, not trusted.

**Can 4a start before Step 3?** Technically yes — fleet has nothing to do with the time path or
Make. But it would mean two half-migrated domains at once, which is the same argument that puts
the inventory app after the field app (§6). Finish the time track first; it's three sittings.

---

## 4. The path — inventory app

Independent of §3 once C3 lands. Don't start until field-app Step 3 is done — see §6.

### Step A — Drop the Jobs mirror, Step C3 (~1 h hands-on, 3-4 days of soaks)

Plan: `docs/BET-drop-jobs-mirror-C3.md`. Removes the only hard coupling between the two Airtable
bases, so inventory can migrate on its own timeline. Irreversible — needs a deliberate go-ahead.

### Step B — Reference data (Vendors, Locations, Inventory Items)
### Step C — The ledger (Inventory Transactions; Stock Levels becomes a view, not a table)
### Step D — Estimating (Estimates, Templates, Conduit Assemblies)
### Step E — The expense push **last** — it's the only path that writes across bases

---

## 5. What stays on Make.com — permanently

Not everything should move. These have no API route for us:

- **pCloud folder creation** (`4509211`) — pCloud's app registration has been dead for months, so
  there is no way for us to hold a token. Make has one from before it broke.
- **pCloud PDF filing** (`4723276`)
- **QB Time job creation + Trello cards** (`4509804`) — could move eventually; no reason to.
- **Google contact sync** (`4729925`)

Getting off Airtable does **not** mean getting off Make. Different goals.

---

## 6. Why the field app goes first

The field app is **half-migrated right now** — two systems hold time data. Starting inventory now
would mean two half-migrated systems at once. Finishing §3 collapses one back to a single source.

Also: the field app is where the money is. A mistake there pays someone wrong. A mistake in
inventory means a stock count is off.

**Exception:** C3 (§4 Step A) is mostly waiting, not working. Slot it into any quiet few days —
it doesn't compete for the same attention.

---

## 7. Not on the path (things that will try to look urgent)

These are real, and none of them moves you off Airtable. Do them when there's appetite, not
because they're next:

| Item | Size | Notes |
|---|---|---|
| ~~`FIND` substring sweep~~ | — | ✅ **Done 2026-08-03** — 7 sites fixed, 3 regression tests. `docs/TODO.md` |
| Job warranty clock | ~1-1.5 h | Owner idea 2026-08-04. `docs/PLAN-job-warranty-service-log.md` §2. Two dead Airtable fields + a badge; Neon side is 2 columns on the `jobs` table that already exists. Cheap enough to slot in any time. |
| Job service visit log | ~4-6 h | Same plan, §3. **Do this at Step 4b, not before** — 4b already covers Generators/Warranties, so building it in Airtable now means building it twice. |
| ~~Receipts on expenses~~ | — | ✅ **Done 2026-08-03** — slices 1-3 shipped. Photos + ScanSnap PDFs, visible in the approval list, manager-only delete. |
| ~~Job prints in the field app~~ | — | ✅ **Shipped 2026-08-05**, un-parked on request. Upload/open/download smoke-tested on prod. 📐 button in the job action row with a count badge, drag-and-drop upload, ⬇ download → opens in the device's own PDF app **and works offline afterwards**, admin/office delete + **permanent** delete (storage). Readable by every role, unlike `jobDocs`. `docs/PLAN-job-prints.md` |
| R2 lifecycle rule | ~15 min | Add prefix `_deleted/` in the Cloudflare dashboard |
| Retire JotForm photos | ~10 min | ~Aug 8, after a week's soak. Pause form + scenario `4522457` |
| Offline photo upload queue | ? | Wait until crews actually hit it |
| Unify estimates bet | large | `docs/BET-unify-estimates.md`. Blocked on GP formulas reaching Neon |
| Conduit assemblies | — | Two rollup fields need adding by hand in Airtable first |

---

## 8. How to use this file

- **"What's next?"** → the first unticked thing in §3.
- **"Can I stop here?"** → if you're at a 🛑, yes, indefinitely.
- **"Should I do X first?"** → if X is in §7, no.
- **Update it when a step lands**, not when it's planned.

**Current answer to "what's next" (revised 2026-08-05, second pass):** **Steps 1 and 2 are both
done** — payroll reads confirmed served by Neon on production, and the four write paths flipped
Neon-first and verified on a branch. The §2 gate is therefore **cleared**: time has left Airtable
in both directions.

**Both are smoke-verified on production.** The next thing is **soak, then Step 3 — retire Make**,
the original goal of this whole migration. Run the reconciler (no flags — `--repair` is disabled)
for several consecutive days first; that is the gate, and it is the only thing standing between
here and turning off scenario `4546051`.

⚠ **What the reconciler will do during the soak.** App-created rows still reach Airtable — the
mirror ran and stamped `airtable_id` on the smoke-test entry, and `unmirrored` is currently 0 — so
both sides stay in step and the checks should stay green. Neon only runs ahead when a mirror
*fails*, which is now the signal worth investigating rather than something to repair away.

**But plan for the reconciler to stop being meaningful at Step 3.** Once Make is off, the puller
writes QB timesheets to Neon and *nothing* writes them to Airtable, so Neon goes permanently and
correctly ahead — by every new timesheet. Comparing whole-table totals against Airtable stops
being a useful check at that moment. Decide then whether it is reworked to reconcile against
**QuickBooks** instead (the actual upstream source) or simply retired with Make.

The remaining jobs-flip work, for when you come back to it:

- ✅ **The full-field jobs sync is DONE 2026-08-05.** `_jobs-sync.js` carries all 37 master
  columns hourly and stamps `synced_at`. Verified on a Neon branch: 112 jobs, **zero differences**
  against the already-verified ETL output across every column.
- ⬜ **The `handleJobs` flip itself — RE-PRICED at 4-6 h, not the "~1 h" this file used to say.**
  The GP half really is ready (`v_job_financials` + `v_job_rollups` cover every financial key).
  The descriptive half is not, and nobody had costed it: **`mapJob` returns 89 keys and roughly
  35 have no Neon source at all**, because the jobs port deliberately excluded links and external
  refs. Missing: the whole **power-company block** (8 keys, lookups into Power Companies /
  Contacts), the **inspection block** (10 keys, through Inspection Agencies / Contacts), **7
  external refs** (wire/pipe/photo links, pCloud + Trello ids), **customer mailing address** (4 —
  Neon has the *job site* address, which is a different thing), and flags like `workflowStatus`,
  `projectComplete` and the five `all*Reviewed` formula bools.
  >
  > **And it is not just the list that would thin out.** Opening a job uses the **cached list
  > record** (`state.jobs.find(...)` → `selectJob`, `index.html:3667`), not a refetch — `jobById`
  > only runs on `refreshSelectedJob()` after estimate writes. So a partial flip blanks the power
  > company and inspection blocks **on the job detail screen**.
  >
  > Real scope: 3 small dimension tables (Power Companies, Inspection Agencies, Inspection
  > Contacts) + ~15 flat columns + sync + flip. Also worth knowing the latency case is weak —
  > `handleJobs` pages 112 records in 2 Airtable requests, not the 146-page scan that made
  > `hoursByJob` take 15 s. This buys migration progress, not speed.
