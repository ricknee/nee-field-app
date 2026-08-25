# Prevailing Wage & the GP layer — recommended approach

*Written 2026-08-20; revised 2026-08-21 after settling overtime and time-entry attribution.
The Service Calls prerequisite is built; the PW machinery is not. This is the recommended design,
not a menu of options.*

---

## STATUS — read this first (2026-08-25)

**Step 0 done. Steps 1-4 not started — there is no PW code.** `grep prevailing_wage|pw_rates|
app_settings` across every `.sql`/`.js`/`.mjs`/`.html` returns nothing but this file and
SYSTEM-MAP. Effort below (~10-12 h) still reads right.

**Nothing blocks Step 1.** Its gate is *"no number moves"*, which needs no PW job to exist and no
rate from the township. Step 2 alone waits on the Seneca determination sheet.

**Two things moved underneath this plan after it was written. Both are folded into the sections
below — this list is only so a returning reader knows to trust the revisions:**

1. **`job_estimates.labor_burden_rate` now exists** (`db/schema/065`, 2026-08-25). The 32.50 is no
   longer trapped in an Airtable formula, and §2's "est GP must be *derived*" recommendation is
   **superseded** — see the revision box in §2. This is the single biggest change to the plan.
2. **Jobs are born in Neon** (`JOB_CREATE_SOURCE=native`, 2026-08-24). Trap 7.2's file, line
   number and premise were all invalidated; it is rewritten.

⚠ **`db/schema/051` is taken** (generator service calls). §6's file is now **`066`**, and the tree
may have moved again — check `ls db/schema/ | tail -1` before writing it.

---

**Scope, settled over three rounds with the owner:** a PW flag on the job (at creation, and
retro-fixable), a per-job rate, and est GP / live GP / closeout GP all reflecting it — plus a way to
create a billable rate, which turns out not to exist.

**Deliberately out:** rates on the payroll PDF, certified payroll, per-person PW hour splits.
*"What days did each person work on PW"* is already answered by the job's **Time Entries** tab.

### Airtable boundary — settled

**This build requires no Airtable schema, field, formula, automation or data edits.** PW state,
rates, settings and diagnostics are Neon-only. Existing Airtable references in this document are
historical explanations or read-only baseline comparisons; they are not implementation targets.
The existing create-job path may continue creating its ordinary Airtable mirror while that broader
dependency still exists, but it sends no PW field or rate to Airtable. `_jobs-sync.js` must not own
or overwrite any PW column.

---

## 1. The reframe: this is not a prevailing-wage feature

PW is worth building properly, but calling it a "PW feature" leads to the wrong design. The honest
description of the problem is:

> **The system has exactly one answer to "what does an hour of labor cost," and it is company-wide.
> Nothing anywhere can say "on THIS job, an hour costs more."**

Worse, there are **two** company-wide answers and they do not know about each other:

| | Where it lives | What it says |
|---|---|---|
| **Estimating** | a hardcoded literal inside an Airtable formula — `{Estimated Labor Hours} * 32.50` | every hour costs $32.50, forever |
| **Actuals** | `labor_cost_rates`, per employee, per week | $22.50 to $33.13 depending on who worked and when |

Neither can be overridden per job. **That is the hole. PW is simply the first thing to fall into it.**

So what I recommend building is **a per-job labor cost rate**, with `prevailing_wage` as the flag
that explains *why* a job has one. Nearly the same work as a PW-specific build, less code, and it
does not need redesigning the next time a job has unusual labor cost for some other reason.

I would **not** generalise further than that. No pluggable rate-rule engine, no per-classification
matrix. One rate per job, shaped the way a wage determination is shaped (base + fringe), because
that is the form the numbers arrive in.

---

## 2. The approach to GP — one principle, and it is the whole design

> **Build ONE rate resolver. Have all three GP numbers read it. Never patch the three separately.**

Three GP figures, and today they get labor from two unrelated places:

```
EST GP       Projected GP $ = Expected Revenue - Projected Est. Total Cost
               └── SUM(job_estimates.estimated_labor_cost)      ← Airtable formula, hours × 32.50

LIVE GP      = total_revenue_live - total_materials_live - total_labor_cost_live
               └── v_job_labor_cost_true_by_job.labor_cost_live ← per employee, per week

CLOSEOUT     = expected_revenue - actual_job_cost_cogs
               └── the SAME v_job_labor_cost_true_by_job        ← per employee, per week
```

The resolver answers one question — *what does an hour on job J, on work date D, for employee E
cost?* — with a precedence chain. Work date matters for PW weeks because overtime is chronological,
not proportionally spread back over the week:

```
1.  employee-week contains NO PW time
        → preserve today's calculation exactly: employee rate in force that week, with weekly
          overtime distributed proportionally over that employee's worked jobs
2.  employee-week contains PW time
        → order worked entries chronologically and split the entry that crosses 40 hours
        → PW hour before 40: the job's supplied PW straight-time cost
        → PW hour after 40:  the job's supplied PW overtime cost
        → normal hour before 40: the employee's normal cost rate
        → normal hour after 40:  the applicable normal-job overtime cost
3.  estimating, where the employee is unknown by definition
        → the job's PW straight-time rate if flagged, else the company estimating rate
```

### Overtime allocation — settled 2026-08-21

NEE schedules PW work at the beginning of the week and does not plan overtime on it. The existing
view proportionally spreads a week's overtime back across every job touched that week. That remains
the rule for an employee-week with **no PW work**, so ordinary-job GP is bit-identical to today.

For an employee-week containing PW work, use chronological allocation for the whole week. If an
employee has 24 PW hours Monday-Wednesday and then 21 normal hours, the PW job gets 24 straight
hours and the normal job gets 16 straight + 5 overtime hours. Do **not** spread 2.67 of those OT
hours back onto the PW job merely because it held 24/45 of the week's time.

This changes a normal job only when it shares a week with PW work, and then only because the later
job actually contains the hours after 40. Company-wide weekly labor cost must not change merely
because it was allocated differently: the sum of PW jobs, normal jobs and unlinked worked time must
reconcile to the employee's actual weekly payroll cost. A mismatch is a loud diagnostic, never an
amount silently forced into one job.

Time entries currently carry a work date but not reliable start/end timestamps. Ordering therefore
uses `work_date`; when more than one job is recorded on the threshold day and their order cannot be
proved, split only that day's ambiguous overtime proportionally across those entries and expose an
`ot_order_ambiguous` diagnostic. Do not invent chronology from Airtable record order or UUIDs.

The supplied PW overtime figure is authoritative data. Do not derive it as `base × 1.5 + fringe`:
the rate schedule may state an explicit OT rate and fringe does not necessarily receive the same
premium. The UI may show a calculated reasonableness check, but it must store and use the supplied
number. Mixed PW/non-PW weeks must also be reconciled to the overtime calculation actually used by
payroll; job costing must not invent a weekly paycheck independently of QuickBooks/accounting.

### How a time entry becomes PW — settled 2026-08-21

**PW belongs to the job, not to a second crew-entered toggle.** QuickBooks Time/TSheets and the
in-app clock already identify the job for every job-linked entry. The resolver joins that `job_id`
to `jobs.prevailing_wage` and the effective-dated job rate. That job association is the PW record.

Do not ask the employee to choose both a job and Normal/PW. Those two answers can disagree, and the
most dangerous mismatch looks plausible: the right hours on the right job at the wrong cost. The
crew workflow stays exactly the same — choose the job and enter/punch time. The UI displays a clear
`PW` badge beside PW jobs in both job pickers and on the active-clock card so the employee can see
the classification without making a second decision.

This deliberately supports the retro-fix requirement: marking a job PW re-resolves its linked
historical time using work date and the effective rate. The confirmation shows the number of
affected hours before doing so.

If NEE later takes a contract where only part of one job is PW-covered, do not overload the global
job flag or add an informal checkbox. Add an explicit job scope/cost-code model, then require the
time entry to select that scope. That exception is out of scope now because current PW jobs are
covered as jobs in their entirety.

**Payoff: live GP and closeout GP already share one input**, so making that input job-aware moves
both, correctly, with no other view touched. This is exactly the substitution point `db/schema/006`
was built around — labor enters the GP layer at two named columns and nowhere else.

### Est GP is the one that will not fix itself, and that matters for the retro-fix

Live and closeout GP are computed by views, so ticking the PW box restates them instantly. **Est GP
will not**, because `estimated_labor_cost` is a *stored* number frozen when the estimate was saved.
Flip the flag and it keeps the old figure forever — the "if I forget" requirement would silently
half-work.

So est GP has to be **derived** — `hours × the resolver's rate` — rather than summing the stored
column.

**I checked this is lossless before recommending it.** All 89 estimates in the database:

| check | result |
|---|---|
| rows where `cost ÷ hours` ≠ 32.50 | **0** |
| rows with a cost but no hours (a value that would be lost) | **0** |
| rows with a NULL cost | **0** |

Every stored cost is *exactly* hours × 32.50. With the default rate at 32.50, **every existing
estimate produces a bit-identical number.** Only PW jobs move.

> ### ⚠ REVISED 2026-08-25 — do NOT derive est GP. The column now exists.
>
> Everything above was true when written and is now **half-obsolete, in our favour**.
> `db/schema/065` added **`job_estimates.labor_burden_rate`** — what an hour COSTS, stamped per
> estimate at create, NULL falling back to 32.50. Its own column comment names this project:
> *"this column is where its per-job resolver lands."* The hardcoded Airtable literal that §1
> called "the hole" is already gone.
>
> **So the recommendation inverts.** Do not compute est GP as `hours × resolver_rate` at read
> time. Instead **have the resolver answer at estimate-create time and stamp the result into
> `labor_burden_rate`.** Deriving at read time would silently undo 065's snapshotting, whose
> whole purpose is that a later rate change cannot rewrite an old quote — the same rule
> `db/schema/006` fought for on the actuals side, and the same rule §6 wants for effective dating.
>
> **The lossless proof above now covers legacy rows only.** "All 89 rows are exactly hours ×
> 32.50" was measured on 2026-08-20. Estimates written since 065 carry their *own* burden rate by
> design, so a from-32.50 derivation would be wrong on exactly the newest rows. Re-run the three
> checks before Step 1 and split them: `labor_burden_rate IS NULL` (legacy, must stay bit-identical)
> vs `IS NOT NULL` (already rate-stamped, must round-trip its own number).
>
> **This does not fix the retro-flip problem, and that is still the hard part.** A stamped rate is
> as frozen as a stored cost was. Ticking PW on a job with saved estimates must **re-stamp**
> `labor_burden_rate` on those rows, inside the same confirmation that already reports the affected
> hour count. What 065 bought is somewhere for the answer to go, not the restatement itself.
>
> **Knock-on for §6:** the proposed `app_settings.estimating_cost_rate` is now partly redundant.
> The 32.50 fallback lives in code (`airtable.js:7104`, `COALESCE($8, labor_burden_rate)`, and the
> legacy CASE arms around `airtable.js:7048`). Either make `app_settings` the single source those
> read, or drop it from this build — but do not leave the company default in two places.

---

## 3. Sequencing — this is the part I feel strongest about

### ✅ Step 0 — fix `Service Calls` first — DONE 2026-08-20 (`92f2d11`)

An unrelated bug found while tracing the revenue path. The revenue formula tests for job type
`"Service Call"` — **singular**. The actual Airtable option is `"Service Calls"` — **plural**
(`fldakkMuWEelHheqr`: Contract / Time & Material / Warranty / **Service Calls**).

It has never matched — not in Neon, and not in Airtable either, whose original formula carries the
same typo. All 20 Service Call jobs fall to `ELSE expected_revenue`, which is $0 on every one.

| | |
|---|---|
| Service Call jobs | 20 |
| Revenue they report today | **$0.00** |
| Revenue if the string matched | **$5,540.11** |
| Labor cost already charged against them | $1,618.67 |

**Every service call currently reads as a pure loss.** The dollars are small; the sign is wrong on
all 20. This is very likely part of what `db/schema/006` noticed and guessed at — *"14 of the 15 jobs
that end up negative have ZERO recorded revenue — mostly Service Calls … possibly uninvoiced work."*

It shipped independently in `db/schema/050_service_calls_gp_typo.sql`. The measured result was
$5,540.11 of restored service-call revenue, with T&M and Contract unchanged to the cent. The PW
baseline in step 1 therefore starts from the corrected definitions.

### Step 1 — ship the whole machine INERT, and prove it changes nothing (~7-8 h)

Add the flag, the rate table, the resolver, and rewire **all three** GP numbers through it — with
**no job flagged PW**.

> **The gate: every one of the 117 jobs' est GP, live GP and closeout GP must be identical to the
> cent, before and after.**

If a single number moves, the resolver is wrong and you find out with **zero exposure** — no job is
PW yet, so there is nothing to get wrong except the plumbing.

This is the house pattern already: the time clock shipped inert behind `TIME_CLOCK`, and every money
formula that moved was diffed row-by-row against Airtable before anything read it. It is the reason
those migrations did not produce a single wrong invoice.

### Step 2 — flip Seneca, and only Seneca (~30 min)

Exactly one job's GP should move, and you can check it by hand: hours × the PW rate. Everything else
stays frozen.

**Steps 1 and 2 are separated on purpose.** "Did I build the plumbing right?" and "did I type the
right rate?" are two different failure modes, and debugging them together is precisely what makes
money bugs expensive. Split, each one has an obvious answer.

### Step 3 — the billable-rate create path (~2 h, independent)

Does not gate anything for Seneca and does not touch GP cost. Slot it wherever convenient.

---

## 4. The billable rate — what you found, and what it needs

You are right that there is **no create path anywhere.** The six rows (65 / 70 / 75 / 85 Regular,
90 / 350 Service) were loaded by the ETL from Airtable (`db/etl/time-entries-full.mjs:287`); the app
can only pick from them. So: a **+ Add rate** button on the existing 💰 Billable Rate bar, admin only.

**But keep it clearly separate from everything above, because it is the *other* rate.** Billable =
what you charge → revenue. Cost = what the hour costs → the side PW changes. And note:

- **`labor_billable_rates.airtable_id` is `UNIQUE NOT NULL`, and the picker keys on it.**
  `handleUpdateJobBillableRate` resolves `WHERE airtable_id = $2`. A natively-created rate has no rec
  id → use the **dual handle** (`coalesce(airtable_id, id::text)`) already established in
  `db/schema/041`/`043` for items. ⚠ **Grep every where-clause, not the table name** — that is the
  lesson from the inventory cutover, where three readers keyed on the rec id alone and stock "saved
  fine then could not be found again."
- **The native create path makes Neon authoritative for job billable-rate selection.** Change
  `handleUpdateJobBillableRate` to update Neon only; do not attempt to put a native UUID into the
  Airtable linked-record field. Remove the current Airtable PATCH from that handler and stop the
  Jobs sync leg from overwriting `labor_billable_rate_at_id` / `billable_hourly_rate`. Existing
  Airtable values become frozen legacy mirrors until Airtable is retired; no Airtable edit is part
  of this cutover.
- **The ETL still loads this table.** Checked: `upsertBatch` is `INSERT … ON CONFLICT DO UPDATE`
  with **no delete**, so native rows survive a run untouched. Safe today — but per the standing rule
  that *giving a read-only reference table a write path turns its ETL from preserving into
  overwriting*, retire that leg in a **second** commit.

⚠ **It will not affect Seneca.** Seneca is a lump-sum **Contract** job, and the revenue formula
ignores the billable rate for anything that is not Time & Material or Service Calls. 78 of your 117
jobs are Contract. Worth having for the next T&M prevailing-wage job; it does nothing for this one.

---

## 5. What I recommend NOT doing

| Not this | Why |
|---|---|
| Rates on the payroll PDF | QuickBooks Time is still the book of record and the accountant runs pay. You already descoped it — I agree |
| Certified payroll in the app | Needs deductions and net pay, which this app has never held. The accountant files it from QuickBooks |
| Per-classification / apprentice PW rates | You have one classification. Build one rate per job with a unique index so a second one **errors** instead of being silently picked between |
| PW as a `labor_cost_rates` row | 🔴 The landmine. `labor_type` already offers "Prevailing Wage" there, but **`v_job_labor_cost_true` does not filter on `labor_type` at all** — it takes the newest effective row per employee. A second open row would make the lookup choose arbitrarily and **silently reprice that person's work on every job they touch** |
| Changing the estimating rate for normal jobs | See below — it is accidentally right, and now is not the moment |

### The estimating rate is right by luck, and that is worth knowing

I expected to find $32.50 badly wrong. Measured against 2026 actuals, it is not:

| | |
|---|---|
| Job-linked hours, 2026 | 6,709.8 |
| Actual labor cost | $220,827.55 |
| **Effective cost per hour, including overtime** | **$32.91** |
| Blended straight-time rate | $31.01 |
| Overtime share of hours | 12.3% |

$32.50 against $32.91 — **1.2% low.** But it is right by coincidence, not design: it is one
journeyman's current true cost rate, and it lands near the true figure only because the apprentice
discount and the OT premium currently cancel each other. Nicholas is **14.9% of 2026 hours at
$22.50/hr** while overtime pushes the other way.

**Recommendation: keep 32.50, but move it out of the Airtable formula into a visible setting**
(a small `app_settings` key/value table — no such table exists today). Do not change the value now.
The point is that today it cannot be changed at all, and it did not move when wages went $25 → $26.

**One related caution, not worth fixing yet:** per-job est-vs-actual labor variance is distorted by
crew mix. A job Nicholas worked heavily reads "under budget" for reasons that have nothing to do with
performance. So do not over-trust *Labor Cost % Over / Under* or the 🔴 OVER / 🟢 UNDER BUDGET pill
at the job level. Estimating against a planned rate is the correct model — this is a reading caution,
not a bug.

---

## 6. Schema

```sql
-- db/schema/066_prevailing_wage.sql   ⚠ 051 is TAKEN (generator service calls).
--    Confirm the next free number at write time: `ls db/schema/ | tail -1`.
ALTER TABLE jobs ADD COLUMN prevailing_wage boolean NOT NULL DEFAULT false;

CREATE TABLE pw_rates (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_id          uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  classification  text,                    -- your own label, e.g. "Electrician (Journeyman)"
  base_hourly       numeric(10,4) NOT NULL CHECK (base_hourly >= 0),
  fringe_hourly     numeric(10,4) NOT NULL DEFAULT 0 CHECK (fringe_hourly >= 0),
  straight_hourly   numeric(10,4) NOT NULL CHECK (straight_hourly >= 0),
  overtime_hourly   numeric(10,4) NOT NULL CHECK (overtime_hourly >= 0),
  burden_pct        numeric(6,4)  NOT NULL CHECK (burden_pct >= 0),
  effective_start date NOT NULL,
  effective_end   date,
  notes           text,
  CHECK (effective_end IS NULL OR effective_end >= effective_start)
);

-- Makes the "one classification" simplification SAFE rather than merely convenient:
-- a second rate raises an error instead of the lookup quietly picking one of two.
CREATE UNIQUE INDEX pw_rates_one_open_per_job ON pw_rates (job_id) WHERE effective_end IS NULL;

-- The write handler also rejects any date range that overlaps an existing row for the job.
-- One-open-row alone does not prevent two closed/future rows from overlapping.

CREATE TABLE app_settings (key text PRIMARY KEY, value text NOT NULL, notes text);
-- seeded: ('estimating_cost_rate', '32.50', 'Company default $/hr for estimated labor cost')
```

**Keep base, fringe, straight and overtime visible.** Base + fringe explain the determination;
`straight_hourly` and `overtime_hourly` are the explicit amounts the costing resolver uses. Do not
derive the official OT number. Burden is likewise explicit — no silent 25% production default — and
the create form shows the resulting loaded straight/OT cost before saving.

**Effective-dated**, because Ohio determinations get adjusted annually and a six-month job will see
one. This preserves the rule `db/schema/006` fought for: *the rate in force on the work date*, so a
later change never rewrites a finished job. **The UI shows one rate box** — the dating stays
invisible until a second row is actually needed.

### Where the box goes

| Place | What |
|---|---|
| **New Job form** | `Prevailing Wage` yes/no beside Job Type. Choosing yes reveals base / fringe / supplied straight rate / supplied OT rate / burden inline — a flag with no complete rate is useless, so ask for all at once |
| **Job detail** | A **🏛 Prevailing Wage** bar on the Time Entries tab, matching the existing 💰 Billable Rate and 🏛 City Tax bars. Admin only |
| **Job list + header** | A `PW` badge, so it is obvious which jobs are prevailing wage |

**Retro-flip confirms with the count** — *"This job has 214 hours booked. Turning on prevailing wage
re-costs all of them and will change this job's GP."* Restating is the point; being surprised by it
is not.

The QuickBooks Time/TSheets job mapping and the in-app clock remain the time-entry authority. Add a
visible `PW` badge to PW jobs in both job pickers and to the active-clock card, but add no separate
Normal/PW selector. Selecting the job is selecting the wage treatment.

**PW on with no rate must fail loudly.** Otherwise the job silently costs at $32.50 and looks fine.
Visible warning on the job, flag on the GP figures — same spirit as the existing
`used_earliest_rate_fallback`.

---

## 7. Traps carried forward

1. **Rewrite the cost view from `pg_get_viewdef('v_job_labor_cost_true'::regclass, true)`, never from
   `006_true_labor_cost.sql`.** That file is superseded twice (024 excludes PTO from the OT
   denominator; 030 is the current fast rewrite). Starting from the file already reinstated the
   pre-024 overtime bug once, during 030 — caught only by a per-employee-week diff.
2. **The create-time Neon job INSERT is a COLUMN SUBSET** — `prevailing_wage` must be added there,
   in the `ON CONFLICT` SET list, in `JOB_SELECT`, and in `mapJobFromNeon`. It is deliberately
   **Neon-only**, matching `city_tax`, `clock_visibility` and `overhead`; do not create a new
   Airtable field or let `_jobs-sync.js` own or overwrite it.
   ⚠ **REVISED 2026-08-25 — the pointer and the reasoning were both invalidated.** The create
   moved out of `airtable.js` into **`createJobNative` in `netlify/functions/_jobs.js`** (~:223)
   when `JOB_CREATE_SOURCE=native` went live 2026-08-24. The old sentence — *"Airtable is created
   first only because the client job identity remains its rec id"* — is **no longer true**: the job
   is born in Neon and Airtable gets a fail-soft mirror nothing reads back. Good news for this
   build (§"Airtable boundary" now gets what it wanted for free), but **verify the line number
   before trusting it; it has already moved once.**
   ⚠⚠ **Do not let `prevailing_wage` reach the Airtable mirror.** The mirror is a real record
   carrying no Neon row's id, and `_jobs-sync.js` re-imported one as a duplicate job until
   `jobs.airtable_mirror_id` was added (`db/schema/062`). A PW flag mirrored out is a PW flag that
   can come back as a second, un-flagged job.
3. **Offline tests cannot reach the Neon SQL.** `tests/handlers.test.mjs` mocks Airtable and runs
   without a database, so a broken parameterised write passes and fails in production. `PREPARE`
   every new statement first.
4. **Do not create a second PW answer on a time entry.** `job_id` is authoritative. A duplicated
   boolean/type field will eventually disagree with the job and silently price correct hours at the
   wrong rate.
5. **Do not infer same-day order.** Work date is sufficient on the normal Monday-first path, but
   multiple jobs on the 40-hour threshold day are ambiguous without timestamps. Proportion only
   that ambiguous day's premium and surface the diagnostic.
6. **PW on + no usable rate is an invalid state.** Enable the flag and insert the rate in one Neon
   transaction/statement; reject missing straight/OT values. The GP read also exposes
   `pw_rate_missing` so a future writer cannot make the failure look like normal cost.

---

## 8. Effort

| Step | | Est. |
|---|---|---|
| 0 | `Service Calls` fix, standalone | ✅ done (`92f2d11`) |
| 1 | Schema + resolver + chronological mixed-week allocation + all three GP paths, **shipped inert**, diffed to the cent | ~7-8 h |
| 2 | Flip Seneca, hand-verify one job | ~0.5 h |
| 3 | Billable-rate create path (dual handle) | ~2 h |
| 4 | *(second commit)* retire the `labor_billable_rates` ETL leg | ~0.5 h |

**≈ 10-12 h remaining**, and the risky half is fully verifiable before a single job is flagged.

---

## 9. What I need from you to start — and what I do NOT

### ⚠ Correction: the rate does not block the build

An earlier draft of this section listed Seneca's hourly rate as something needed "to start." That was
wrong, and the owner was right to push on it.

**The rate is per-job DATA, not configuration.** Seneca's rate is simply the first row in `pw_rates`.
The next PW job — different county, different year, different determination — gets its own row with
its own numbers. Nothing in the code knows or cares what any particular number is.

What the build actually depends on is the **shape** of a rate, not its value:

```
base_hourly + fringe_hourly + supplied straight_hourly + supplied overtime_hourly,
with an explicit burden percentage
```

And that shape is fixed by how Ohio publishes determinations, not by what Seneca's figures turn out
to be. So it is already known, and it is already in the schema.

| Step | Needs Seneca's rate? |
|---|---|
| 0 — `Service Calls` fix | **No** |
| 1 — schema, resolver, all three GP paths, shipped inert and proved a no-op | **No** — the gate is that *nothing moves*, which needs no PW job to exist |
| 3 — billable-rate create path | **No** |
| 2 — flip Seneca | **Yes.** ~30 minutes: type the supplied base, fringe, straight and OT figures into a form, confirm burden, tick the box |

**So: build everything now; enter the number whenever the township hands it over.** Step 2 is a form,
not a code change.

### Actually needed to start

No product decision remains before the inert build. Step 0 is already complete. The supplied
Seneca straight/OT rate is needed only to activate Seneca, and the payroll/accounting calculation
for a mixed PW/non-PW week must be confirmed before that activation gate.

### The one thing the rate *does* gate — and it is not software

**Do not send the Seneca bid before you know the determination rate.** Estimated at the default
$32.50/hr, a PW job's labor cost is understated by a wide margin — PW all-in cost commonly lands
somewhere near double the standard rate, though that is a guess until you have the sheet in hand.
That risk exists whether or not any of this gets built; no amount of code substitutes for the number.

### The one thing that could still change the schema

If the determination turns out to name **more than one classification you will actually staff**
(a journeyman *and* an apprentice), the single-rate-per-job model needs the `pw_assignments` upgrade
noted in §6. That is guarded, not assumed: `pw_rates_one_open_per_job` makes a second classification
**raise an error** rather than being silently chosen between. So the risk is contained, and it is
visible the moment it happens rather than three months later in a GP number.
