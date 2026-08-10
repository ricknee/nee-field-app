# Roadmap — getting off Airtable

**The goal:** Airtable stops being the database. Neon (Postgres) holds the data, the two apps
read and write it directly, and Make.com is reduced to the handful of jobs only it can do.

**This file is the running order.** If something isn't on it, it's a detour — see §7.

*Last updated 2026-08-08.*

---

## 1. Where you actually are

| System | State |
|---|---|
| **Time entries** | ✅ **DONE — Make left the time path 2026-08-07 (Step 3).** In Neon, authoritative for writes since 2026-08-05. QB → Neon → app, no Airtable and no Make. The Airtable table is now a frozen historical copy. ⚠ The **QB Time puller is the only writer of time data now** — if it stops, nothing catches it. |
| **Jobs master data** | ✅ In Neon (112 jobs), read by 4 app endpoints. Identity columns refresh **hourly** since 2026-08-05; the other ~22 master columns still need a hand-run ETL. |
| **Payroll reads** | ✅ **Served by Neon — confirmed on production 2026-08-05.** All three of `handlePayrollHoursRollup` (`_ms` 90), `handleMyHoursRollup` (65) and `handlePayrollEntries` (60) returned `_source:"neon"`, with the rollup figures matching Neon exactly. |
| **Job list / GP** | ✅ **`handleJobs` IS Neon-first** — it joins `v_job_rollups_true`, `v_job_financials_true` and `v_job_labor_cost_true_by_job`, so GP and true labor cost serve from Neon today. *(This row previously said "still reads Airtable" — that was stale and caused a real debugging detour on 2026-08-07.)* ⚠ But its **inputs** — `job_estimates`, `expenses`, `invoices`, `wire_weigh_ins`, `pipe_usage` — are loaded by a **hand-run ETL**, so GP revenue is only as fresh as the last run. See §3 Step 4d. |
| **Crew Schedule** | ✅ In Neon since 2026-08-05 (Step 4a) — reads, writes and the crew picker |
| **Fleet + Lifts** | ✅ In Neon since 2026-08-05 (Step 4b), **photos in R2** — off Airtable's expiring attachment URLs |
| **Generators, Warranties, Inspections** | ✅ **In Neon since 2026-08-07 (Step 4c-1 + 4c-2)** — reads and writes. Commissioning is now **one atomic statement**; you can **edit** a generator instead of re-commissioning it; the Generator pill toggles the tab on. |
| **Expenses** | ✅ **In Neon since 2026-08-07 (Step 4d)** — read (with the employee authz scope) and all five write paths. The four Airtable money formulas are ported and diffed **to the cent**. ⚠ Airtable stays the **identity** authority: R2 receipt keys are built from the expense rec id. |
| **Estimates** | ✅ **In Neon since 2026-08-07 (Step 4e, estimates half)** — read + all four writes, templates, sent-estimate PDFs, and the snapshot cascade. **Estimate PDFs are in R2**, off Airtable's expiring URLs. |
| **Invoices** | ✅ **In Neon since 2026-08-08 (Step 4e)** — both reads (job tab + the all-invoices view) and both write paths. The **contract-billing chain is ported and reproduces 51/51 on every field** (`db/schema/015`). Totals serve the **computed** figure, not the stored copy that goes stale when an allocation changes. |
| **Jobsite photos** | ✅ Never touched Airtable — R2 from day one |
| **Employees + LOGIN** | ✅ **In Neon since 2026-08-08 — both apps.** `LOGIN_SOURCE=neon` is an env-var **kill switch** (rollback in ~30 s, no code revert). Verified on prod: `_source:"neon"`, the **Airtable rec id** not the Neon uuid, right person and role. Employees were the last Airtable-owned dimension. **Cost rates are Neon's too** — the ETL no longer loads `labor_cost_rates`. ⬜ ~16 secondary read sites still hit Airtable (cleanup, §4). |
| **People admin** | ✅ **New 2026-08-08** — there was no employee screen in either app. `👥 People`: Active/Former roster, **access toggle that actually ends live sessions** (30-day stateless tokens meant unchecking `Active` did nothing to a signed-in phone — a leaver kept access for a month), Show/Change PIN, full edit, add a person, and cost-rate history with raise-vs-correct. `docs/PLAN-employee-admin.md` |
| **Inventory app** | 🟨 Still Airtable, but **decoupled from the main base since 2026-08-10** — Step A (drop the Jobs mirror) is ✅ **DONE**, and **Step B0** ✅ (the five main-base `Jobs` reads serve from Neon). B/C/D/E not started. ⚠ **Step E is now urgent, not last** — see the note under §4 Step E. |

## 🎉 THE FIELD-APP MIGRATION IS COMPLETE — 2026-08-08

**Steps 1-3 and 4a-4e are all done.** Time, payroll, schedule, fleet, lifts, generators,
warranties, inspections, expenses, estimates and invoices all read and write Neon. Make is off
the time path. Every money formula that moved was **diffed row by row against Airtable before
anything read it** — `db/schema/012`, `013`, `014`, `015`.

**Airtable is not gone, and was never meant to be.** It remains the *identity* authority for
expenses, estimates and invoices (R2 receipt keys, sent-PDF back-links and billing allocations
all key on rec ids), and every write still mirrors to it. What changed is that **nothing reads
Airtable to answer a question any more** unless Neon fails.

**What is left is no longer the field app:** the inventory track (§4, not started), and giving
the billing allocations a write path in the app — the only thing still requiring anyone to open
Airtable in normal operation. The hourly sync (2026-08-08) covers them meanwhile.

*The `handleJobs` full flip that used to be listed here is **already done** — see §8.*

---

## 2. The one rule that set the order — ✅ SATISFIED 2026-08-07

> ~~**Make cannot be retired until payroll reads leave Airtable.**~~
> **Done.** Payroll reads moved (Step 1), then writes (Step 2), then Make left the time path
> (Step 3), in exactly that order.

Kept for the record because it explains why the first three steps happened in the order they did:
killing Make first would have stopped Airtable receiving QB time while the payroll rollups still
read it.

**This rule no longer constrains anything. Everything remaining is negotiable** — pick by value,
not by dependency. §8 makes the recommendation.

---

## 3. The path — field app

### ✅ Verification phase — CLOSED 2026-08-07

*Historical. Everything below was the gate before Steps 1-3; all of it passed and Make is off.
**For "what do I do now", go to §8.***

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

### 💰 TRUE LABOR COST — built 2026-08-05, owner-approved, not yet on screen

Owner's call, verbatim: *"I want true profit numbers."* Airtable's labor costing is wrong in
four ways, and `db/schema/006_true_labor_cost.sql` replaces it by computing from the time
entries directly. **Owner reviewed the numbers 2026-08-05 and confirmed them correct.**

| | Airtable today | True |
|---|---|---|
| Labor cost counted at closeout | **$89,406** | **$268,721** (approved only) |
| Total labor cost, 56 jobs | — | **$336,440** |
| Overtime hours | — | 1,340.52 |

**The ~$179,000 hole:** `Reviewed Labor Cost = IF(Reviewed, cost, 0)`, where `Reviewed` is a
**manual checkbox on Job Labor Allocation that nothing ever ticks** — approving hours writes
`Labor Reviewed` on the TIME ENTRY, a different record. So `Total Labor Cost (Final)` was 0 on
31 of 55 jobs and final GP was overstated by the whole labor cost of each.

**The allocation rows also silently miss hours** — Shop Work 581.5 real vs 252.25 allocated
(329.25 h ≈ $10,300 never costed), Adena DG 412.5 vs 396.5, Cambridge 488.75 vs 478.75.

**And old work was priced at today's wage** (the rate lookup filters to *current*), so the
2025-12-30 raise from $25 → $26 retroactively rewrote finished jobs. Now joined per week.

> **The overtime maths is UNCHANGED and was never wrong.** Hours over 40 in the week, spread
> across that week's jobs in proportion to hours, at 1.5×. Reproduced exactly and validated
> against Airtable's own weekly rollup: **297 of 304 employee-weeks match to the cent**. Of the
> 7 that differ, 5 are hours Make missed and the puller caught, 1 is same-day activity before
> Make's 21:00 run, and 1 is Airtable's rollup disagreeing with its own time entries (Miles
> Unruh, week 2026-03-09 — rollup 40.5, entries 36.5).

> ⚠ **Migration dependency:** the OT denominator was `Employee Weekly Time`, **maintained by
> Make**. Retiring Make at Step 3 would have silently degraded labor cost. The new view computes
> it from the entries, so that trap is now closed rather than pending.

**Known, accepted, do not re-raise:** 3 employees have no wage history for part of 2025 —
Patrick Gingerich and Scott Koehn (rates start 2025-12-01, working since January) and Nicholas
Stoltzfus (starts 2025-10-01, working since August). 1,314.5 h / **$42,386** across 12 jobs is
priced at each person's *earliest known* rate. Wages rise, so this **overstates** cost — the true
profit on those jobs is slightly better than shown. Owner saw the list and accepted it. Fixing it
is data entry in Airtable's **Labor Cost Rates**, not code; the view prefers an exact rate match
automatically and flags every fallback row via `used_earliest_rate_fallback`.

✅ **LIVE since 2026-08-05** — `handleJobs` and `handleJobById` serve these numbers.

> **Zero-revenue jobs — REVIEWED AND ACCEPTED 2026-08-05, do not re-raise.** Costing labor
> correctly made **14 jobs show a final loss because they have no recorded revenue** (~$12,766
> of cost). They read $0 profit before only because labor cost $0 too. This is a **separate
> pre-existing gap the fix exposed**, not a fault in the labor maths — "no revenue" means no
> invoice *in this system*; work billed directly in QuickBooks looks identical.
>
> Owner reviewed the full list with dates and chose to leave them: they are old records from
> before actual data was being entered. **MaryAnn McReady (2026-08-03) is being billed by the
> owner — labor only, no supplies.** Biggest: Jenny Ln 1 ($5,027, 164.5 h) · Wayne TWP Fire
> Dept. ($2,464) · Alliance Stone ($1,843) · Gus Tar ($1,426, materials only).
>
> **Strongsville DG (Contract, MES 394) is NOT in that list and is still unexplained** — it has
> real recorded revenue of $1,800 against $29,562 of cost, so it is not the zero-revenue
> artifact. Worth a look if job profitability is ever questioned.

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

### ✅ Step 3 — Make leaves the time path — **DONE 2026-08-07** 🎉

**Scenario `4546051` ("QB Time, airtable (copy)") is deactivated.** Time data now flows
QB → Neon → app, with no Airtable and no Make in the path. **This was the original goal of the
whole migration.**

Verified before switching off: the other four QB Time → Airtable scenarios (`4484414`, `4484649`,
`351560`, `2036155`) were all already **inactive**, so `4546051` really was the only live pipe.
The Airtable Time Entries table stays as a frozen historical copy; Make's other ~69 scenarios are
untouched.

> ⛔ **DO NOT ROTATE THE QB `tsheets` CREDENTIALS. This file used to say to — that instruction was
> wrong and has been removed.**
>
> It assumed the credential was exclusive to the retired scenario. It is not: connection **24601
> "My TSheets connection"** is shared by **eight** scenarios, including `4509804` *Airtable – Job
> Awarded* (a §5 **permanent** keep), `4545219` *Airtable – Service Call* and `4512438`
> *Airtable → Trello*. Revoking or deleting it **breaks scenarios that are meant to run forever**.
>
> Worse, regenerating the underlying QuickBooks Time token risks invalidating **`QB_TIME_TOKEN`**
> — the Netlify env var the puller uses. Since 2026-08-07 the puller is the **only** thing writing
> time data anywhere: no Airtable copy, no Make. If it dies it is **silent**, and the reconciler
> will not catch it, because the Airtable side it compares against is now frozen by design.

> ⚠ **The reconciler stops being meaningful from today.** Nothing writes QB timesheets to Airtable
> any more, so Neon runs permanently and correctly ahead — the same shape as the Larry Unruh 6.5 h
> discrepancy on 2026-08-07, but growing daily. Whole-table comparison is no longer a health
> check. Decide whether to repoint it at **QuickBooks** (the actual upstream) or retire it.
>
> **What replaces it as the thing to watch:** that new timesheets are still reaching Neon. A dead
> puller now has no second copy to fall back on.

> 🛑 **STOP POINT — the original goal of this whole migration, reached.**

### Step 4 — The remaining field-app domains

Each is its own slice on the same pattern: **mirror → read-flip → write-flip → retire.** Ordered
by risk, cheapest and safest first, so the pattern is proven on something that can't cost money
before it's used on something that can.

> **⚠ RE-LETTERED 2026-08-05.** Schedule was missing from this file entirely — it has a top-bar
> tab and a whole backend family and appeared nowhere in the running order. Adding it at the
> front pushed every other letter along one. **Older notes saying "Step 4b" for the job service
> visit log now mean 4c, and "4d" for estimates/invoices now means 4e.**

| # | Domain | Why here | Rough size |
|---|---|---|---|
| **4a** | **Schedule** ⬅ *owner's chosen next project (2026-08-05)* | **The smallest slice in the app, and the best place to prove the pattern.** One table, **7 fields, 64 rows** (Title, Job, Start/End Date, Crew, Notes, Entry Type). No money, no formulas, no rollups, no Make. Its only two links — **Job and Employee — are already in Neon**, so the FKs resolve on day one. | ~2 h |
| **4b** | ✅ **Fleet + Lifts — DONE 2026-08-05** | **Lifts**: 10 rows, 9 photos in R2, natural sort, plus three capabilities that never existed (add a lift, retire a sold one with its photos, add/remove a photo). **Fleet**: 11 vehicles + 91 service records + 8 mileage entries, 9 photos in R2. Service history now hangs off a **real FK** instead of Airtable's unescaped `{Vehicle}="<name>"` filter, and logging mileage is **atomic** instead of two round-trips that could half-succeed. `Job Vehicle Trips` skipped — 0 rows, no handler reads it. | done |
| **4c** | ✅ **Inspections, Generators, Warranties — DONE 2026-08-07** | 4c-1 + 4c-2 shipped and smoke-tested on prod: generators, generator service, **atomic** commissioning, warranties, templates, job inspections, inspection agencies + contacts — all read and write Neon. Also gained an **Edit Generator** form (editing used to mean re-running commissioning) and an admin toggle on the Generator pill (the tab was previously unreachable). ⬜ **4c-3** (one generic Neon→Google contact sync replacing 5 Make scenarios) and ⬜ **4c-4** (job service visit log) remain — both deferred on purpose, see §8. | done |
| **4d** | ✅ **Expenses — DONE 2026-08-07** | Read (with the employee authz scope preserved) and all five write paths on Neon. ⚠ The roadmap used to call this *"plain arithmetic rather than rollup formulas"* — **wrong**: 14 of 33 Airtable fields are derived and `Total Cost (Actual)` sits inside a 4-level chain. It only collapsed to plain arithmetic because the **wire/pipe path is dead** (owner, 2026-08-07 — that data comes from the inventory app now; last wire 2026-04-14). All four money formulas ported and diffed **to the cent** across 386 rows. | done |
| **4e** | 🟨 **Estimates + Invoices — ESTIMATES DONE 2026-08-07, INVOICES NOT STARTED** | **Estimates ✅** read + 4 writes, templates, sent-estimate PDFs, snapshot cascade, and PDFs moved to **R2**. **Invoices ⬜** — the last field-app slice. Blockers are cleared (`material_billing_allocations` migrated; `Invoice Material Amount` reproduces 51/51), but the **contract-billing chain** — `Contract Invoice Amount` → `Contract Remaining` → `Final Contract Invoice Amount` → `Remaining Percent to Bill` — is the deepest formula chain in the system and decides what customers are billed on contract jobs. Give it a fresh session and the `013` diff treatment. | ~4-6 h |

> ⛔ **Hard constraint on 4e:** every GP and live-profit formula must be reproduced as Neon views
> and reconciled against Airtable *before* anything Airtable-side is retired. Not after, not
> alongside. `docs/GP-FORMULA-INVENTORY.md` is the checklist; a previous sweep found the inventory
> was silently dropping 5 of 28 rollups, two of them GP-critical — so the checklist itself has
> been wrong before and gets re-verified, not trusted.

**Not in this list, because it is already done:** the **panel schedule builder** is a different
thing from the crew Schedule above — it is the electrical panel/circuit layout, and
`handlePanelSchedules` reads `panel_schedules` / `panel_circuits` **directly in Neon**, returning
503 if Neon is unavailable. Built Neon-native from day one like the jobsite photos, so it never
needs migrating. Both tables are still empty; nobody has used it yet.

**Can a Step 4 slice start before Step 3?** This file used to say no — two half-migrated domains
at once, the same argument that puts the inventory app after the field app (§6). **That was
written about 4a, before Steps 1 and 2 landed, and it no longer holds.** Time is now on Neon in
both directions and nothing about it is half-done; Step 3 is gated on *soak days*, which is
waiting rather than working — exactly the condition under which §6 already allows inventory C3
to fill quiet days.

**So 4c started 2026-08-06, mid-soak.** It also has a positive reason to go first: 10 of the ~35
`mapJob` keys blocking the `handleJobs` flip are the inspection block, and 4c builds precisely
those dimension tables.

**The one carry-over caution:** if the reconciler goes red, time data wins. Keep 4c in reviewable
chunks so it can be set down, and never let it block a Step 3 go-ahead when the gate clears.

---

## 4. The path — inventory app

**The §6 gate is fully open.** It said don't start until field-app Step 3 was done; the *whole* field
app finished on 2026-08-08, so there is no half-migrated system to collide with any more. **This is
now the main track, not the one waiting its turn.**

**Detail: `docs/PLAN-inventory-to-neon.md`** (written 2026-08-07). Letters below match that plan;
don't re-letter. Rough total **~23-32 h** plus soaks, plus a shared login step owned by neither app.

### ✅ Step A — Drop the Jobs mirror (C3) — **DONE 2026-08-10**

Plan: `docs/BET-drop-jobs-mirror-C3.md`, execution log in its §9. **The `Job` link field and the
mirror table are both deleted.** The only hard coupling between the two Airtable bases is gone, so
inventory now migrates on its own timeline. The inventory base drops from **17 tables to 16**.

Proven after the deletes, not assumed: a real push created main-base expense `recgkGpRDCONTGjbQ`
on the right job at the right markup, with the link field gone.

> ⚠ **The lesson worth carrying: a field/rollup sweep is NOT a dependency sweep.** The delete dialog
> reported 5 dependencies, two of them **Airtable Interface elements** the audit never considered —
> two forgotten "Use Material" forms. They happened to be already-dead, but that was luck.
> `list_pages_for_base` answers it in one call. **Check Interfaces before any irreversible Airtable
> delete.**

**Pre-flight record (all passed):**

- ✅ **Make audit re-run — 0 references** to `appfsLJwfow4CepCw` or the mirror table. 70 scenarios,
  **18 active**; the 22 that use an `airtable` or `http` module (the only ways to reach a base) all
  had their **blueprints** fetched and grepped. Every one points at the main base.
  > ⚠⚠ **`isPaused` is NOT Make's activation flag — `isActive` is.** `isPaused` is `false` on all 70,
  > including retired scenarios. Any audit filtering on it reports every scenario as live.
- ✅ **Orphan check — 0.** **116** transactions carry the legacy `Job` link and **all 116** also carry
  `Job ID (Main)`, every value a well-formed `rec…` id. **Deleting the link field loses no job
  history.** (The filter was proved to discriminate, not silently return nothing.)
- ✅ **Snapshot taken** — 26 rows, to `C:\Users\irick\projects\nee-backups\`. **Not in the repo**:
  netlify publishes the repo root, so a CSV committed there is served publicly. Verified rather than
  trusted — the Jobs-side link count reconciles exactly with the Transactions side.

**Execution, 2026-08-08 → 2026-08-10:** sync frozen (the mirror had **three** sync sources, not one
— *Project is Awarded* / *Service Calls* / *Project is Complete*) → soaked with a real `submitCart`
and push → `Job` link field deleted → soaked again with a second real push → mirror table deleted.
Both deletes verified by API `422`.

### ✅ Step B0 — The cross-base reads — **DONE 2026-08-10** (`1b9a84d`)
Five handlers now serve from Neon with Airtable as fallback: `handleJobs`, `handleEstimatingJobs`,
`handleTemplateContractors`, `handleAwardedJobs`, and the job index inside `handlePendingExpenses`.
`handleEmployees` was already Neon-first from the login flip. Smoke-verified on prod: the USE cart
shows 26 jobs, the estimates picker 43, and the New Lead jobs are present.

> ⚠ **Display name reads `po`, NOT `po_locked`.** The PO only locks at award time, so `po_locked`
> is blank on **all 13 New Lead jobs** — exactly the ones the estimating picker exists to show.
> Reading it would have dropped them, and a short list looks identical to a complete one.

> ⚠ **Offline tests cannot reach the Neon path** — `_neon.js` lazy-imports the driver, so with no
> `DATABASE_URL` they only ever prove the Airtable fallback. The suite now mocks the driver's HTTP
> transport (rows must be **value arrays**, not objects). The SQL was also checked against the real
> database, because a parse-time type error is invisible offline.

*Original scope, for reference:* the five main-base `Jobs` reads + `handleEmployees`. That data is **already in Neon**, so this
touches no inventory table and needs no schema. **Not gated on the field-app `handleJobs` flip** —
verified 2026-08-07, the four handlers need only name/PO/status/tax status/contractor and Neon
`jobs` carries all five. The field-app flip is blocked on ~35 *other* `mapJob` keys inventory
never reads.
> ⚠⚠ Must return **`airtable_id` as `id`**, not the Neon uuid — the id flows cart → `Job ID (Main)`
> → a *linked-record field* on main-base Expenses (`inventory.js:1116`). Uuids there write garbage
> into an Airtable link. Resolves at Step E, not before.

### Step B — Reference data (Vendors, Vendor Pricing, Locations, Inventory Items) (~5-7 h)
### Step C — The ledger (Inventory Transactions + Stock Levels) (~6-8 h)
> **Correction to the old line here:** Stock Levels does **not** become a pure view. It carries
> `Reorder Point` and `Notes`, which are real user data — it splits into a small `stock_settings`
> table plus a `v_stock_on_hand` view. Also: `Quantity On Hand` is an Airtable-automation-maintained
> **cache**, and `Inventory Items` derives the same number a second, independent way. Check whether
> they agree before migrating — a disagreement is a live data bug, not a migration detail.

### Step D — Estimating (Estimates, Templates, Material Orders, Conduit Assemblies) (~6-8 h)
> ⚠ Neon already has `job_estimates` — the **main base's** estimates, which feed the GP views. The
> inventory base's `Estimates` is a different thing. Name them `material_estimates`.

### 🔴 Step E — The expense push (~4-6 h) — **NO LONGER LAST. This is a LIVE GAP.**

**Found 2026-08-10 by pushing an expense and looking for it in the field app. It wasn't there.**

The inventory push writes the expense to **Airtable only**. The field app has read expenses from
**Neon** since Step 4d (2026-08-07). Nothing connects the two:

- Airtable Expenses: **392**. Neon `expenses`: **390**. The two missing are the only pushes since
  the last load.
- All 390 Neon rows share **one** `synced_at` (2026-08-09 19:37) — a hand-run snapshot, not a feed.
- **Nothing reloads it on a schedule.** The only `@hourly` function is the QB time pull;
  `_billing-sync.js` handles allocations, not expenses; there is no expenses loader in `db/etl/`.
  The only code that writes Neon `expenses` is `airtable.js` — the field app's own write paths.

So this is **not** "stale until the next sync". **Material cost pushed from the inventory app does
not reach the field app or GP at all**, and won't until someone hand-runs a load. Nothing is wrong
with the books today only because the two affected pushes were $7.76 of testing — the next real
one would vanish just as silently.

**The fix is Step E itself:** make the push write Neon in the same transaction it writes Airtable,
the same Neon-first pattern every other slice uses. Its old blocker (field-app 4d) cleared on
2026-08-07.

> ⚠⚠ **Do not hand-insert the missing rows into Neon as a stopgap.** `expenses` is a full-reload
> mirror; anything written ahead of a reload is erased, silently. Same trap as the pCloud folder ids.

### ✅ (Shared) — Login — **DONE 2026-08-08. Both apps serve from Neon.**

*This section used to say login was "the last step, and the moment Airtable actually goes dark."
It happened, out of order, because the People screen needed it. Kept in §4 because it is still
shared by both apps — but it is no longer a gate on anything.*

**`LOGIN_SOURCE` is an env-var KILL SWITCH, not a code path.** `neon` = Neon decides with an
Airtable fallback when Neon is unreachable; unset/`airtable` = the old behaviour with Neon shadowing
and logging disagreements. Production runs `neon`.

```
netlify env:unset LOGIN_SOURCE && netlify deploy --build --prod   # rollback, ~30 s, no revert
```

**Verified on production**, not just by the `_source` label: `_source:"neon"` on both apps,
`id:"recxH3WzXlvhl7z9u"` — the **Airtable rec id, never the Neon uuid** — correct name and role, and
`last_login_at` moving in Neon seconds later. Every downstream call succeeded on the issued token.

Sequence used, and worth reusing for any change with this blast radius:
**1** widen + backfill (inert) → **2** shadow every real login and log disagreements → **3** flip
behind a switch that ships OFF → turn on → verify.

> ⚠⚠ **The two apps had never agreed on how to match a login.** Field app took
> name|username|email; inventory took name|**first name**|username. So `patrick` logged into one and
> not the other, unnoticed because everyone uses their username. The rule is now the **union** of
> both (a union can only accept logins that already worked somewhere). **Ambiguity is REFUSED** —
> Airtable's `Array.find()` silently took the first match, which with first-name matching would hand
> one person another's session. All 24 combinations (8 active × 3 identifier forms) verified to
> resolve to exactly one, correct person.

> ⚠ **PINs are stored PLAINTEXT in Neon, by owner decision 2026-08-08.** The plan was to hash in
> this pass; the owner uses the People screen's *Show PIN* to read a forgotten PIN back to someone,
> and a hash cannot be un-hashed. Not a downgrade — they were already plaintext in Airtable. To hash
> later: add `pin_hash`, migrate on next login or reset, drop `pin`, retire `handleEmployeePin`.

**What moved with it:** `db/schema/017_employees_full.sql` gave Neon a complete employee record
(pin, email, phone, employee_no, labor_type, notes, names) on top of `016`'s admin columns
(`hired_on`, `terminated_on`, `token_valid_from`, `last_login_at`). All 11 employees backfilled;
Neon and Airtable agree exactly (11 rows, 8 active) after deleting a phantom `Viewer` row that
had been deleted from Airtable but left in Neon.

> ⚠⚠ **Neon now owns `labor_cost_rates`, and `db/etl/time-entries-full.mjs` NO LONGER LOADS IT.**
> The People screen writes cost rates directly. Re-enabling that upsert would overwrite every
> app-entered rate with the stale Airtable row — and `true_cost_rate` drives `v_job_labor_cost_true`,
> so a raise would vanish and **every job's labor cost would jump back to the old number**. App-created
> rate rows carry a synthetic `app:<rec>:<date>` id because they have no Airtable counterpart.

**⬜ Still open (cleanup, not risk):**
- **Stage 4** — the ~16 remaining Airtable `Employees` read sites (crew pickers, scheduling picker,
  payroll rollups, bonus lists). All have Neon data already; mechanical.
- **Stage 5** — retire the `employees` dimension load in the ETL and drop the Airtable half of the
  People writes, in one commit. Until then both stores are written together and agree.

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
| Job service visit log | ~4-6 h | Same plan, §3. **Do this at Step 4c, not before** (was 4b before the 2026-08-05 re-lettering) — that slice already covers Generators/Warranties, so building it in Airtable now means building it twice. |
| ~~Receipts on expenses~~ | — | ✅ **Done 2026-08-03** — slices 1-3 shipped. Photos + ScanSnap PDFs, visible in the approval list, manager-only delete. |
| ~~Job prints in the field app~~ | — | ✅ **Shipped 2026-08-05**, un-parked on request. Upload/open/download smoke-tested on prod. 📐 button in the job action row with a count badge, drag-and-drop upload, ⬇ download → opens in the device's own PDF app **and works offline afterwards**, admin/office delete + **permanent** delete (storage). Readable by every role, unlike `jobDocs`. `docs/PLAN-job-prints.md` |
| ~~Panel schedules~~ | — | ✅ **Built 2026-08-05**, ⬜ needs a prod smoke test. `⚡ Panels` beside Prints: name + voltage + circuit count, odd-left/even-right grid, fill-down, PDF export. **First domain born in Neon rather than migrated to it** — `db/schema/007_panel_schedules.sql`, no Airtable table, writes fail closed. Slices 3-4 (watts/amps/poles, save-PDF-to-Prints) not built. `docs/PLAN-panel-schedules.md` |
| ~~Job checklists~~ | — | ✅ **Built 2026-08-05**, ⬜ needs a prod smoke test. `✅ Lists` in the job action row, badge = items still outstanding. Name a list, type items one per line, tick them off loading the truck; ticked drops to a collapsed "Loaded" section rather than being deleted. **Second domain born in Neon** — `db/schema/008_job_checklists.sql`. Ticks are optimistic with a localStorage replay queue, because the shop is where signal dies. **First feature that takes work away from Trello rather than sitting beside it.** `docs/PLAN-job-checklists.md` |
| R2 lifecycle rule | ~15 min | Add prefix `_deleted/` in the Cloudflare dashboard |
| Retire JotForm photos | ~10 min | ~Aug 8, after a week's soak. Pause form + scenario `4522457` |
| Offline photo upload queue | ? | Wait until crews actually hit it |
| Unify estimates bet | large | `docs/BET-unify-estimates.md`. Blocked on GP formulas reaching Neon |
| Conduit assemblies | — | Two rollup fields need adding by hand in Airtable first |
| **Employee admin ("People" screen)** | Slice 1 ✅ **built 2026-08-08**, ⬜ needs prod smoke; slices 2-4 ~5-8 h | Owner idea 2026-08-07. `docs/PLAN-employee-admin.md`. **Slice 1 shipped the security half**: a `👥 People` tab (strict-admin) with an Active/Former roster and an access toggle that actually ends live sessions — `token_valid_from` in Neon, checked against the token's `iat` behind a 60 s cache, in **both** functions. Schema applied 2026-08-08; 0 revoked, so it is inert until used. Slices 2-4 (editing, hire/term dates, wage history, add-an-employee) are ordinary feature work and can wait. **Show PIN / Change PIN shipped 2026-08-08** on owner's request — Change PIN refuses duplicates, which closed a live privilege escalation (an admin and two office users all shared one PIN, so either office user could log in as the admin). **Slice 5, self-service "forgot PIN", is deferred by the owner and blocked on data, not code:** all 11 employee records have an empty Primary Email *and* Primary Phone, so a reset code has nowhere to go. Fill those in during onboarding and the blocker clears itself; best done at/after the PIN-hashing pass. Original scoping notes follow. There was **no employee screen in either app** — every hire, raise, role and leaver is done in the Airtable grid. Wanted: a roster with an Active/Former toggle so a leaver loses app access. ⚠ **Slice 1 is a real security gap, not a convenience feature:** session tokens are stateless HMAC with a **30-day TTL** and `verifyToken` reads no database (`_auth.js:18,49`), so unchecking `Active` blocks a *new* login and does **nothing** to a phone already logged in — a quitter keeps full field-app access for up to a month. Needs a `token_valid_from` column + a 60 s-cached revocation check. The rest (hire/term dates, wage history off `labor_cost_rates`, add-an-employee) is ordinary feature work and can wait. Scoping also turned up two live bugs to fix in the same pass: the two apps read **different role fields** (`Role` vs `Role New`), and `F.emp.email` names a column that doesn't exist (`Primary Email`), so **email login has never worked**. |
| **Time clock in the app** | ~~~10-14 h~~ **BUILT 2026-08-08, shipped INERT** | The park is over — its gate (Step 3) cleared 2026-08-07. Owner's call 2026-08-08: *"just build the app and replace qb time later on. not going to use time tracking in the app until its complete and ready for use."* So it is built and **switched off**: `TIME_CLOCK` and `TIME_CLOCK_PAYROLL` both default off, QB Time is still the book of record, and nothing counts until someone decides it does. `db/schema/018_time_clock.sql`, `docs/PLAN-time-clock.md` §11 (the operating manual + what diverged from the draft). ⬜ **Needs a browser smoke test** — no UI path has been exercised, and the offline queue has never met a real phone losing signal. **The §3 fork is still open and is now the only thing left to decide: does the app REPLACE QuickBooks Time, or FEED it?** |

---

## 8. How to use this file

- **"What's next?"** → read the box below. It is the only thing you need.
- **"Can I stop here?"** → if you're at a 🛑, yes, indefinitely.
- **"Should I do X first?"** → if X is in §7, no.
- **Update it when a step lands**, not when it's planned.

---

# ▶ START HERE — what's next (2026-08-07)

> ## 📋 READ `docs/AUDIT-airtable-remaining.md` FIRST — added 2026-08-09
>
> A live audit of the code, the Neon schema, all 39 Airtable automations and all 70 Make
> scenarios. **It corrects this file in four places** and carries the ordered work list.
>
> 1. ~~**A live bug.**~~ ✅ **FIXED same day (`26d14c4`).** `handlePayrollHoursBreakdown` +
>    `handleMyHoursBreakdown` were reading the Airtable Time Entries table — the one Step 3 *froze*
>    on 2026-08-07 — while the tiles above them served Neon. Both Neon-first now, and the other
>    four readers of that table were checked and are legitimate fallbacks, so the **bug class** is
>    closed. ⚠ **Freezing a table does not find its readers for you** — remember this at step 10.
> 2. **Six domains were never on this file.** Payroll Runs + Bonuses, Companies + Contacts,
>    Vendors, Power Companies + Contacts, Labor Billable Rates, and **job creation** — which is
>    still a pure Airtable write with no Neon leg at all. "The field-app migration is complete"
>    below is therefore **wrong**; ~22-31 h remain.
> 3. ⚠⚠ **The Airtable mirror writes are the Make trigger bus.** All ten Airtable-touching Make
>    scenarios fire from *Airtable record changes*. Drop the mirror writes before replumbing the
>    hooks and pCloud folders, Trello cards, QB Time jobs and Google contact sync all stop —
>    **silently**. Replumb each hook *before* removing its mirror write, never after.
> 4. **34 Airtable automations are deployed; §8 tracks 4.** Nine hold job-lifecycle logic
>    including PO number assignment (job identity). Four are the dead wire/pipe path.
>
> Also in there: a **~2 h path to unblock 4c-3** that needs no Google Cloud project, and the two
> ten-minute owner actions (inventory-base PAT, Jobs-mirror sync freeze) gating ~25 h of work.

## The migration's original goal is DONE.

Steps 1, 2 and 3 are complete. **Make has left the time path.** Time flows QB → Neon → app.
Step 4a (Schedule), 4b (Fleet + Lifts) and 4c-1 + 4c-2 (Generators, Warranties, Inspections)
are also done. **The §2 forced-order rule no longer constrains anything — everything left is
negotiable.**

## Do this every day (2 minutes, not a project)

> **Confirm new timesheets are still reaching Neon.**
>
> Since 2026-08-07 the **QB Time puller is the only thing writing time data anywhere**. No
> Airtable copy, no Make. If it stops, it is **silent**, and the reconciler cannot catch it
> because the Airtable side it compares against is frozen by design.
>
> ```sql
> SELECT key, updated_at, watermark, note FROM sync_state WHERE key = 'qb_timesheets';
> ```
>
> **`updated_at` is the heartbeat — that is the number to look at.** The puller runs `@hourly`
> (`netlify.toml`), so anything older than ~2 hours means it is not running. `note` reports what
> the last run did, e.g. `fetched=1 upserted=1 deleted=0`.
>
> ⚠ **Do NOT health-check this by counting rows or reading `max(work_date)`.** That was the
> original advice here and it is WRONG: it cannot tell a quiet day from a dead puller, which is
> the entire failure mode being watched for. A day with no new timesheets looks identical to a
> puller that stopped a week ago. `sync_state` distinguishes them.
>
> **Do NOT keep running the old reconciler as a health check.** Neon now runs permanently and
> correctly ahead of a frozen Airtable table, so it will read red for the rest of time. Either
> repoint it at **QuickBooks** (the real upstream) or retire it. Until then its output is noise.

## ✅ Step 4e — invoices — DONE 2026-08-08. The field app is finished.

## ▶ The one real piece of work left: give allocations a write path

**Scoped 2026-08-08, deliberately not started. ~4-6 h.** This is **feature work, not migration
work** — nothing is broken, and the hourly sync covers it meanwhile.

**The gap:** the app has no write path for billing allocations. It *reads* unlinked ones so the
invoice builder can compose a draft, but nothing creates or links one. **Four deployed Airtable
automations do that**, and `v_invoices.invoice_total_calc` is computed *from* those allocations.
They are the only reason anyone still has to touch Airtable in normal operation.

| id | fires when | does |
|---|---|---|
| `wflTwXb6dG32FFv9s` | Time Entry: billable, hours > 0, unallocated, **Labor Reviewed ✓** | creates labor allocation |
| `wflNmJsnIhWtSjUlL` | Expense: billable, unbilled > 0, unallocated, **Reviewed ✓** | creates material allocation |
| `wflOcxtmkzdxKMVQW` | Invoice saved, **Auto Allocate? ✓** | links unlinked labor allocations |
| `wfl7bzJpZY9kcJ27i` | Invoice saved, **Auto Allocate? ✓** | links unlinked material allocations |

**What to build:**

1. **Create on review** — `handleUpdateTimeEntryPayroll` and `handleApproveExpense` already write
   exactly those fields. Add the allocation insert to the same write; the values are already known.
2. **Attach on invoice save** — `handleSaveInvoice` with `autoAllocate` claims the job's unlinked
   allocations. The frontend **already fetches that exact list** to build the draft.
3. Turn the four automations off. `_billing-sync.js` becomes a safety net rather than a dependency.

> ⚠ **Idempotency is the trap.** Review → un-review → re-review must not create a second
> allocation. Airtable guards on "allocation link is empty"; Neon needs the same guard or a unique
> constraint. Allocations decide what a customer is billed — give it the `013`/`015` diff treatment.
>
> ⚠ **Doing it synchronously kills a real lag.** `index.html` carries a documented fallback for
> "brief automation lag between Review and allocation row creation". Writing allocations in the
> same transaction removes that lag — and makes the fallback dead code to delete deliberately.

## Or pick one of these — nothing is forced any more

| Option | Size | Why / why not |
|---|---|---|
| **Smoke-test what shipped** | ~20 min | ⬅ **Do this first.** Invoices went live today with no prod exercise. See the list below. |
| ~~**`handleJobs` full flip**~~ | ✅ **ALREADY DONE** | **This file was wrong.** It claimed ~35 of `mapJob`'s 89 keys had no Neon source. Checked 2026-08-08: `mapJob` returns **87** keys and `mapJobFromNeon` returns **87** — the two apparent differences were comment text, not keys. `handleJobs` is Neon-first and the mapper is complete. Nothing to do. |
| **Decide the ETL's future** | ~1 h thought | GP inputs are now Neon-**written** rather than loaded, so the hand-run ETL is mostly redundant for them — but it still backfills, and it is the only thing that would catch a mirror that silently stopped. Schedule it, shrink it, or retire it deliberately. |
| **Inventory track** | ~19-26 h left | `docs/PLAN-inventory-to-neon.md`. **Step A ✅ and Step B0 ✅ both done 2026-08-10.** 🔴 **Step E is the one to do next** — not because it's next in the letters, but because it's a live gap: inventory pushes never reach the field app or GP (§4 Step E). Then B/C/D. ⚠ B/C/D need a **PAT scoped to the inventory base** — neither PAT in `.env` can read it (both 403). |
| **Employee admin slices 2-4** | — | Separate track, and it carries **two live bugs**: `Role` vs `Role New` differing per app, and email login that has never worked (`Email` ≠ `Primary Email`). |

## ⬜ Owed smoke tests — things that shipped without being exercised on prod

- **Invoices (2026-08-08)** — the job **Invoice** tab and the **Invoices** top-bar view. Check
  totals, and that invoices on **archived/completed** jobs still appear: 32 of 51 are, and a
  status filter would have hidden them.
- **Contract invoices specifically** — the chain caps a contract invoice at
  `MIN(ContractRemaining, ExpectedRevenue × PercentToBill)`. Josh Astorino has three.
- **Estimate + expense writes** — edit, save, then **hard-refresh**. Before the write flips these
  reverted on reload.
- **Employee admin People tab** — needs a two-device check (a session must end when someone is
  deactivated).

---

## ✅ Step 4d — Expenses — DONE 2026-08-07

Shipped: read (with the employee authz scope preserved) and all five write paths.

**The reason it was picked, which still applies to 4e:** `handleJobs` serves GP from Neon views,
but those views read `expenses`, `job_estimates`, `invoices`, `wire_weigh_ins` and `pipe_usage` —
dimension tables loaded by a **hand-run ETL**. So GP revenue is only as fresh as the last time
somebody ran a script. Migrating each one to Neon-native writes removes a stale input permanently.
**The migration path and the GP-freshness problem are the same work.**

> ⚠ **Two corrections this slice produced, both worth carrying to 4e:**
>
> 1. **"Plain arithmetic rather than rollup formulas" was wrong.** 14 of 33 Airtable fields on
>    Expenses are derived, and `Total Cost (Actual)` sits inside a 4-level chain. It only
>    collapsed to plain arithmetic because the **wire/pipe path is dead** — that data comes from
>    the inventory app now (owner, 2026-08-07; last wire-costed expense 2026-04-14, last pipe
>    2026-02-18, all 362 since are manual). `legacy_material_cost` preserves the 24 pre-April
>    rows, and **any cleanup of those Airtable tables must keep that fallback** or they recompute
>    to a different number.
> 2. **Money needs more than 2 decimal places.** Wire costs carry sub-cent precision (weight ×
>    price), and storing at `numeric(14,2)` threw 4 rows off by a cent. Round only the final
>    result. And Airtable computes in IEEE-754: five expenses hit an exact half-cent residue and
>    it reported 0.00 on four, 0.01 on the fifth, from identical inputs. `013` treats a sub-cent
>    residue as fully billed rather than emulating that.

Estimates followed the same reasoning and shipped the same day. Invoices are the one input still
loaded rather than written.

## Explicitly NOT next, and why

| | |
|---|---|
| **4c-3** Google contact sync | Blocked on **you** creating a Google Cloud project + OAuth consent + refresh token before any code can be written. And it replaces two scenarios that have **never once fired** — an Airtable trigger nobody wired, not a broken integration. Nothing is currently working that would stop working. Low urgency. |
| **4c-4** Job service visit log | A genuinely new feature (~4-6 h), not a migration. Nothing depends on it. Do it when it's wanted, not because it's listed. |
| ~~**`handleJobs` full flip**~~ ✅ **DONE — ignore the rest of this row** (checked 2026-08-08: `mapJobFromNeon` returns all 87 keys). Historical: | Re-priced at **4-6 h, not the "~1 h" this file used to claim** — ~35 of `mapJob`'s 89 keys have no Neon source. It buys migration progress, not speed: `handleJobs` pages 112 records in 2 requests. |
| **Inventory track** | Independent, and §6's "never two half-migrated systems" argument is now weaker since the field app's time track is finished. Still lower value than 4d. |

## The rules that were learned the hard way — apply them at 4d and 4e

1. **Write Neon, not just Airtable.** `handleJobs`/`handleJobById` are **Neon-first** over an
   **hourly** mirror, so an Airtable-only write silently reverts on refresh. This was hit
   **three times** in one day. It hides because the frontend patches local state after saving,
   so it looks fine until you reload.
2. **Flipping a read changes the ID FORM the client holds.** Grepping `startsWith("rec")` is
   **not sufficient** — a live regression shipped that way. Handlers that never validate an id,
   and just forward it to `atFetch`, are invisible to that grep. **Move read and write in the
   same commit.**
3. **Partial results are worse than none.** A Neon-first read that falls through only on *zero*
   rows will show **half** a list when some rows are in Neon and some in Airtable — and half a
   list looks complete.
4. **An Airtable LOOKUP returns record IDs over the REST API**, but renders as a display name
   inside an Airtable formula. Use the sibling *formula* field. Cost a full ETL run.
5. **Moving a write to Neon removes Airtable's `typecast` guard**, so single-select whitelists
   have to move into code or the column quietly becomes free text.
