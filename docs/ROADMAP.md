# Roadmap — getting off Airtable

**The goal:** Airtable stops being the database. Neon (Postgres) holds the data, the two apps
read and write it directly, and Make.com is reduced to the handful of jobs only it can do.

**This file is the running order.** If something isn't on it, it's a detour — see §7.

*Last updated 2026-08-04.*

---

## 1. Where you actually are

| System | State |
|---|---|
| **Time entries** | ✅ In Neon. QB Time pulls straight in. Airtable still written as a mirror. |
| **Jobs master data** | ✅ In Neon (110 jobs), read by 4 app endpoints |
| **Payroll reads** | 🟨 **Already flipped in code** — `handlePayrollEntries`, `handlePayrollHoursRollup` and `handleMyHoursRollup` are all Neon-first with an Airtable fallback (verified in source 2026-08-04). Confirm `_source:"neon"` on a real prod response before calling it done. |
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
- ⬜ **Run the reconciler daily** — `node db/etl/time-entries-full.mjs`. The run the morning after a
  Make 21:00 is the one that settles double-counting.
- ⬜ **Watch the write mirror** on the next real time-entry add or Labor-Reviewed tick — it shipped
  alongside a broken driver and has never been exercised.

> 🛑 **STOP POINT — and a good one.** Everything works, Make is intact, nothing is half-done in a
> way that decays. This state is stable indefinitely.

### Step 1 — Payroll reads move to Neon (~3-4 h, one sitting)

*Why now:* it's the forced next link (§2), and it's smaller than it looks — `handlePayrollEntries`
returns no billing fields, so the labor-billing layer isn't involved.

> ⚠ **Mostly already done — re-scoped 2026-08-04.** Substeps 3 and 4 are flipped in the deployed
> source, and the `airtable_id` linker was folded into the reconciler (first run: 8 unlinked → 8
> matched, 0 ambiguous). What remains is **verification, not building**. Check this against the
> code before planning a sitting for it.

1. ~~Backfill `airtable_id` onto puller-created rows by natural key~~ — **done**, linker lives in
   `db/etl/time-entries-full.mjs` and runs in both modes
2. Decide the canonical entry id — Neon uuid, carrying `airtable_id` while Make lives (~30 min)
3. ~~Flip `handlePayrollEntries` to Neon-first + Airtable fallback + `_source`~~ — **done**
4. ~~Flip the payroll rollups and my-hours reads the same way~~ — **done**
5. **Confirm `_source:"neon"` on real production responses** for all three. Code-complete is not
   the same as serving — a broken driver once made every Neon read fall back silently for three
   days while returning correct answers.

> 🛑 **STOP POINT.** All time reads served by Neon, writes still Airtable-first. Fully reversible —
> the fallbacks stay in place.

### Step 2 — Payroll writes move to Neon (~3 h)

Flip the four write paths to Neon-first; Airtable becomes the mirror — the exact reverse of
today's arrangement. Then soak and reconcile.

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
| Job prints in the field app | ~4 h | Owner idea 2026-08-03. Crews open drawings without a pCloud login.  |
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

**Current answer to "what's next" (revised 2026-08-04):** further along than this file said. The two
remaining ⬜ items in §3 NOW are still *observational* — run the reconciler, watch the write mirror.
**Step 1 is now mostly verification, not building** (see its note). The next thing that is actually
a *build* is one of:

- **Schedule the jobs sync, then flip `handleJobs` to Neon** (~2-3 h) — cashes in the entire GP
  layer, which is ported and diffed to zero mismatches but serving nothing today.
  > ⚠ **The sync is the prerequisite, and it is not optional.** `jobs` in Neon refreshes only when
  > someone runs `db/etl/time-entries-full.mjs` by hand. Checked 2026-08-04: **110 rows, last synced
  > 2026-07-31 — 4.5 days stale, against 112 in Airtable.** Flipping today would drop the two newest
  > jobs off the job list and show four-day-old statuses and GP. Time entries avoid this because the
  > QB puller runs hourly as a scheduled function; jobs has no equivalent yet. Put the refresh on
  > that same mechanism first, then flip.
- **Step 2 — payroll writes → Neon** (~3 h), the forced next link in §2.

Either is a reasonable next sitting. The `handleJobs` route is the one that shows up on screen —
leaving proven work switched off is how it rots — but price it at 2-3 h, not 1.
