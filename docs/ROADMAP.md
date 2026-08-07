# Roadmap — getting off Airtable

**The goal:** Airtable stops being the database. Neon (Postgres) holds the data, the two apps
read and write it directly, and Make.com is reduced to the handful of jobs only it can do.

**This file is the running order.** If something isn't on it, it's a detour — see §7.

*Last updated 2026-08-07.*

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
| **Invoices** | ⬜ **The last field-app slice.** The blockers are gone — `material_billing_allocations` is migrated and `Invoice Material Amount` reproduces on 51/51 — but the **contract-billing chain is not ported**. See §3 Step 4e. |
| **Jobsite photos** | ✅ Never touched Airtable — R2 from day one |
| **Inventory app** | ⬜ Still Airtable, still coupled to the main base via the Jobs mirror |

**The time track is FINISHED.** One system holds time data now — Neon — and the reconciler that
kept the two honest has done its job and is retired as a health check (see §8). What remains in
the field app is money: expenses, then estimates and invoices. The inventory track has not
started.

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

Independent of §3 once C3 lands. Don't start until field-app Step 3 is done — see §6.

**Detail: `docs/PLAN-inventory-to-neon.md`** (written 2026-08-07). Letters below match that plan;
don't re-letter. Rough total **~23-32 h** plus soaks, plus a shared login step owned by neither app.

### Step A — Drop the Jobs mirror, Step C3 (~1 h hands-on, 3-4 days of soaks)

Plan: `docs/BET-drop-jobs-mirror-C3.md`. Removes the only hard coupling between the two Airtable
bases, so inventory can migrate on its own timeline. Irreversible — needs a deliberate go-ahead.

### Step B0 — The cross-base reads (~1-2 h) — **independent, can go NOW**
The five main-base `Jobs` reads + `handleEmployees`. That data is **already in Neon**, so this
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

### Step E — The expense push **last** — it's the only path that writes across bases (~4-6 h)
> ⚠⚠ **Gated on field-app Step 4d.** Neon's `expenses` table is a full-reload one-way mirror from
> Airtable — an expense written there early is silently erased by the next ETL run. Same trap as the
> pCloud folder ids: flip *inside* the field-app expenses flip, never before it.

### (Shared, last) — Login
`handleLogin` lives in **both** functions and both read the main base's `Employees`. Neon's
`employees` has no PIN and no email, so it can't flip as an inventory slice — it moves once, for
both apps, and that is the moment Airtable actually goes dark. The plaintext-PIN compare should be
hashed in the same pass rather than moved twice.

> **Interacts with the People screen** (§7, `docs/PLAN-employee-admin.md`). That screen writes
> `active`/role/PIN to **Airtable** on purpose, because that is where both `handleLogin`s read —
> and because `db/etl/time-entries-full.mjs:241-247` is a **live dimension load** that overwrites
> Neon `employees.name/username/role/active` from Airtable on every run. Writing `active` to Neon
> before this login flip retires that load gets it silently erased. Its *new* columns
> (`hired_on`, `terminated_on`, `token_valid_from`, `last_login_at`) are safe — the upsert names
> its columns explicitly. **When login flips, drop the Airtable half of those writes and retire
> the dimension load in the same commit.**

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
| **Employee admin ("People" screen)** | ~8-12 h, **but Slice 1 is ~3-4 h** | Owner idea 2026-08-07. `docs/PLAN-employee-admin.md`. There is **no employee screen in either app today** — every hire, raise, role and leaver is done in the Airtable grid. Wanted: a roster with an Active/Former toggle so a leaver loses app access. ⚠ **Slice 1 is a real security gap, not a convenience feature:** session tokens are stateless HMAC with a **30-day TTL** and `verifyToken` reads no database (`_auth.js:18,49`), so unchecking `Active` blocks a *new* login and does **nothing** to a phone already logged in — a quitter keeps full field-app access for up to a month. Needs a `token_valid_from` column + a 60 s-cached revocation check. The rest (hire/term dates, wage history off `labor_cost_rates`, add-an-employee) is ordinary feature work and can wait. Scoping also turned up two live bugs to fix in the same pass: the two apps read **different role fields** (`Role` vs `Role New`), and `F.emp.email` names a column that doesn't exist (`Primary Email`), so **email login has never worked**. |
| **Time clock in the app** | ~10-14 h | Owner idea 2026-08-07, and **explicitly parked behind the migration** — owner's words: *finish migrating first, then we'll look at time.* `docs/PLAN-time-clock.md`. Smaller than it sounds (Neon already authoritative for time writes, `createTimeEntry` already Neon-first and already employee-writable), but it needs a decision first — **does the app replace QuickBooks Time or feed it?** — and that question is much clearer once Step 3 is done and QB is the only thing left upstream. Building it *before* Step 3 puts a third writer of hours into the soak that is meant to prove the reconciler clean. |

---

## 8. How to use this file

- **"What's next?"** → read the box below. It is the only thing you need.
- **"Can I stop here?"** → if you're at a 🛑, yes, indefinitely.
- **"Should I do X first?"** → if X is in §7, no.
- **Update it when a step lands**, not when it's planned.

---

# ▶ START HERE — what's next (2026-08-07)

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
> SELECT max(work_date), count(*) FROM time_entries WHERE work_date > CURRENT_DATE - 3;
> ```
>
> **Do NOT keep running the old reconciler as a health check.** Neon now runs permanently and
> correctly ahead of a frozen Airtable table, so it will read red for the rest of time. Either
> repoint it at **QuickBooks** (the real upstream) or retire it. Until then its output is noise.

## Then build: ▶ Step 4e — INVOICES (the last field-app slice)

**Everything else in the field app is done.** Steps 1-3, 4a, 4b, 4c, 4d, and the estimates half
of 4e all shipped. Invoices are what remain.

**The hard dependencies are already cleared:** `material_billing_allocations` is migrated, and
`Invoice Material Amount` reproduces on **51 of 51** invoices. So this is no longer excavation —
it is porting a formula chain whose inputs are all present.

**What makes it the last one, and worth a fresh head:** the contract-billing chain —
`Contract Invoice Amount` → `Contract Remaining` → `Final Contract Invoice Amount` →
`Remaining Percent to Bill` — is the deepest chain in the system, each formula consuming the
previous plus job-level lookups, and it decides **what a customer actually gets billed on a
contract job.** Give it the `013` treatment: port, then diff every row against Airtable, before
anything reads it.

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
| **`handleJobs` full flip** | Re-priced at **4-6 h, not the "~1 h" this file used to claim** — ~35 of `mapJob`'s 89 keys have no Neon source. It buys migration progress, not speed: `handleJobs` pages 112 records in 2 requests. |
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
