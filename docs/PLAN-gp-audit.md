# Plan — a real GP audit: can the numbers be trusted?

**Status: ✅ COMPLETE 2026-08-12. Slices 1 + 2 run → `docs/AUDIT-gp-findings.md`; slice 3 is NOT
required — the owner closed the question it existed to answer.**

> **The answer, in one line: per-job figures on tracked jobs are trustworthy; the company-wide
> total is not, and that is now a deliberate choice rather than a defect.** Finding 1 was fixed
> (final GP counts all labor), overhead jobs are flagged out, and the eight jobs with no recorded
> material cost are owner-accepted as-is. Nothing here is left to investigate.

*Original status line:* Owner's ask, 2026-08-10: *"some day we'll need to do a real gp audit
to see if all the numbers really make sense. i want to know if i can trust them."*

> **Answer so far: the arithmetic is trustworthy, the inputs are not complete, and every gap
> found pushes profit UP.** $34,584.50 of real labor never reaches final GP because the hours
> were never approved; $362,471.95 of revenue sits on jobs with zero recorded material cost;
> Shop Work invents $38,155 of revenue nobody was invoiced. Read the findings file, not this
> plan, for the current picture.

**Size: ~8-10 h across three slices.** Slice 3 needs the owner (a QuickBooks export) and is the
only one that actually answers the question.

**One-line:** Every verification done so far proves the numbers match **Airtable**. Airtable was
demonstrably wrong. So nothing done so far answers "can I trust them."

---

## 1. Why the existing verification does not answer this

This is the whole reason the audit is worth doing, and it is easy to miss because the migration
produced a *lot* of green ticks:

| What was proven | What it means |
|---|---|
| 2,970 rollup comparisons, 0 mismatches (`005`) | Neon reproduces Airtable's rollups |
| 1,540 formula comparisons, 0 mismatches (`005`) | Neon reproduces Airtable's GP formulas |
| Expenses diffed to the cent, 386 rows (`013`) | Neon reproduces Airtable's expense maths |
| Invoices 51/51 on every field (`015`) | Neon reproduces Airtable's billing chain |
| GP views 0 differences at 4 grains (`030`) | The perf rewrite changed nothing |

**Every one of those is a fidelity check against Airtable.** And `006` exists precisely because
Airtable's labor costing was wrong in four ways at once — a ~$179,000 hole from a checkbox
nothing ticked. A perfect reproduction of a wrong number is a wrong number.

So the question splits into three, and they need different methods:

- **A. Do the formulas mean what the BUSINESS means?** (not: do they match the old system)
- **B. Are the INPUTS complete?** GP can be arithmetically perfect and still wrong because
  something never arrived — hours with no job, an expense never pushed, an invoice never entered.
- **C. Does it reconcile to money that ACTUALLY MOVED?** The only independent anchor is
  **QuickBooks**. Everything else in this system descends from the same inputs.

**C is the one that answers the owner's question.** A and B are how you avoid wasting C's time.

---

## 2. Scoping numbers, measured 2026-08-10

Run before writing this so the plan targets something real. **These date fast — re-run them.**

| Check | Result | Read |
|---|---|---|
| Job-weeks with no cost rate | **0** | ✅ every costed hour has a rate |
| Jobs with NULL markup % | **0** | ✅ no silent zero-markup |
| Hours on the earliest-rate fallback | 1,314.50 h | known + accepted, matches `006` exactly |
| T&M / Service Call jobs with **no billable rate** | **3** | ⚠ labor revenue computes to 0 on these |
| Jobs with a negative FINAL GP | 15 | 14 are the known zero-revenue set; 1 is Strongsville |
| Time rows with no employee | 7,448 rows / 28,116 h | contribute nothing to labor cost |
| Time rows with no job | 11,173 rows / 41,186.50 h | **see below — this looks alarming and isn't** |

### The unlinked hours are frozen history, not a live leak

Split by year, it resolves completely:

| Year | Unlinked hours | % of that year unlinked |
|---|---|---|
| 2021 | 5,024.25 | **100%** |
| 2022 | 10,812.00 | **100%** |
| 2023 | 9,155.50 | **100%** |
| 2024 | 9,839.50 | **100%** |
| 2025 | 6,325.75 | 61.6% |
| **2026** | **29.50** | **0.4%** |

2021-24 predates job linking entirely — those hours were never costed to a job and never will be.
**2026 is 0.4% unlinked**, so current GP is not missing labor in any material way. This is the
single most reassuring number in the audit, and it means the audit is a **confirmation exercise,
not a rescue.**

> ⚠ **The exception worth chasing: 2025 at 61.6%.** Any job that ran through 2025 and is *still
> open or recently closed out* is under-costed by whatever share of those 6,325 hours belongs to
> it. Slice 1 finds out whether any such job exists. This is the most likely place a real error
> is hiding.

---

## 3. Slice 1 — input completeness (~2-3 h, SQL only, no code)

Does every job have everything it should? Pure queries, nothing changes.

1. **The 2025 unlinked hours** — do any belong to a job still Awarded / Ready to Invoice /
   recently Completed? Match on `job_name` text where the link is missing. If yes, that job's
   labor cost is understated and its GP is wrong today.
2. **The 3 T&M jobs with no billable rate** — is that deliberate (never billed hourly) or a
   missing field? `labor_revenue_tm = hours × rate`, so a blank rate silently zeroes the revenue
   side while cost stays real. Names them.
3. **Expenses that never reached a job** — expenses with no `job_id`, and inventory pushes whose
   expense exists in Airtable but not Neon. Step E (2026-08-10) closed the ongoing gap and
   backfilled; confirm 393 = 393 still holds and nothing new has diverged.
4. **Invoices vs allocations** — `v_invoices.invoice_total_calc` is computed FROM allocations,
   which are created by four Airtable automations. An allocation that never fired means revenue
   understated. Compare computed vs stored totals across all 51 and list any that disagree.
5. **Estimates in a status that hides them** — Est GP deliberately reads the FILTERED rollups
   (Sent / Approved / Archived-Completed). A job whose only estimate sits in Draft shows zero
   expected revenue, correctly but confusingly. List them so the zero is understood, not feared.
6. **Jobs stuck in the wrong status** — GP (Final) is NULL unless the job is Ready to Invoice or
   Completed. A finished job left as Awarded never produces a final number.

## 4. Slice 2 — does it pass the smell test? (~2 h)

Outliers and plausibility, on the whole book at once.

- **GP % distribution.** Sort every job by GP%. Anything above ~60% or below 0 gets read
  individually. A believable book has a cluster and a few tails; a broken one has neither.
- **Cost with no revenue and revenue with no cost** — both directions.
- **Live vs Final GP disagreeing wildly** on the same job.
- **Est GP vs actual GP** per job. Bethel School was checked by hand 2026-08-10 and came out
  51,820 estimated vs 49,740 live — 0.07 of a point apart. That is what a healthy job looks
  like; the audit is asking how many others do.

> **Known and ACCEPTED — do not re-raise these, only flag if the list has GROWN:**
> - **14 zero-revenue jobs** (~$12,766 of cost). Owner reviewed the full list with dates
>   2026-08-05 and chose to leave them: old records from before real data was entered.
> - **1,314.5 h / $42,386 on the earliest-rate fallback** (Patrick, Scott, Nicholas). Overstates
>   cost, so true profit is slightly better than shown. Owner accepted. Fixing it is data entry
>   in Labor Cost Rates, not code.
> - **Scott Koehn's 8 unlinked hours.** Owner 2026-08-10: leave them, he no longer works here.
>
> **STILL UNEXPLAINED and genuinely open: Strongsville DG** (Contract, MES 394) — $1,800 of real
> recorded revenue against $29,562 of cost. It is NOT part of the zero-revenue set. This is the
> one existing anomaly the audit should actually resolve.

## 5. Slice 3 — reconcile against QuickBooks (~3-4 h + owner time)

**This is the slice that answers the question.** Everything else is internal consistency; this is
the only external anchor.

1. Owner picks (or the audit proposes) **5-8 finished jobs** spanning the types that matter —
   at least one Contract, one T&M, one Service Call, one large, one small.
2. Owner exports the matching **QuickBooks** figures: what was actually invoiced, what was
   actually paid, and material/subcontract cost as the books have it.
3. Line them up against `v_job_financials_true` for the same jobs: revenue, COGS, labor, GP.
4. **Every difference gets a named cause** — not a tolerance. "QB includes a change order the
   app never saw" is an answer; "close enough" is not.
5. Write the result up as a short verdict per job: trustworthy / trustworthy with a caveat /
   not yet.

> ⚠ **Expect the answer to be "the app is right and QuickBooks is differently scoped"** at least
> once — work billed directly in QB looks identical to work never billed. That is exactly the
> gap the zero-revenue jobs exposed. Finding it again is a result, not a failure.

## 6. What the audit is NOT

- Not a re-diff against Airtable. That has been done to death and cannot answer the question.
- Not a rewrite. It produces a **verdict and a punch list**, and any fixes are separate work
  decided after the findings are read.
- Not blocked on anything, and it blocks nothing. It can be picked up in any quiet block, and
  slices 1 and 2 need nobody but the person running them.
