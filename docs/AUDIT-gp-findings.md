# GP audit — findings, slices 1 and 2

*Run 2026-08-11 against production Neon. Plan: `docs/PLAN-gp-audit.md`.
Slice 3 (the QuickBooks reconcile) NOT run — it needs an owner export.*

---

## The verdict, in one line

**The arithmetic is trustworthy. The inputs are not complete — and every gap found
pushes profit UP, never down.**

Reported total final GP across 61 finished jobs is **$998,953.10**. At least
**$34,584.50** of that is real labor cost that was never counted, and **$38,155.00** is
revenue nobody was ever invoiced. Both figures are exact.

A further **$362,471.95 of revenue sits on jobs with zero recorded material cost**. The
owner has confirmed (2026-08-11) that some of these are **deliberately incomplete
historical records**, kept for reference from before expenses were tracked, and will not
be corrected. That resolves it as a *defect* — it does not resolve it as a *total*, and
it is still unknown which of the eight are pre-tracking records and which are current
jobs genuinely missing cost.

Nothing here is a calculation error. Every formula checked reproduces what it is
meant to reproduce. What the audit found is **cost that never arrived** — and the single
most useful thing to know is that **the book contains jobs that were never meant to be
complete.** A headline GP figure is only meaningful with those excluded.

---

## What came back clean

Worth stating, because it narrows where the risk actually is:

| Check | Result |
|---|---|
| Invoices: stored total vs computed total, all 51 | **0 differences**, $1,188,617.34 both ways |
| Expenses with no job attached | **0** of 393 |
| Job-weeks with no cost rate | **0** |
| Jobs with a NULL markup % | **0** |
| Earliest-rate fallback exposure | 1,314.50 h — matches the accepted figure exactly, has not grown |
| The 14 accepted zero-revenue jobs | **$12,772.64** vs ~$12,766 accepted. Intact, has NOT grown |

> On the zero-revenue set: a raw count now says **16**, not 14. The two extra are
> jobs still in progress with no final GP yet — normal, not new losses. The
> owner-accepted 14 are unchanged. **Nothing to re-raise.**

---

## Finding 1 — unreviewed hours silently drop labor out of final GP

**$34,584.50 across 23 of 61 finished jobs. Exact, not an estimate.**

`Actual Job Cost (COGS)` uses `actual_labor_cost_reviewed`, and `v_job_labor_cost_true_by_job`
derives "reviewed" with `bool_and(labor_reviewed)` over a job-week. That is strict on
purpose — one unapproved entry holds the whole week open — but the consequence at
closeout is that **unapproved labor is not counted as cost at all**, so the job reports
a higher profit than it earned.

Where a job has no fully-reviewed week at all, `labor_cost_reviewed` is NULL and COGS
gets **zero labor**:

| Job | Status | Hours | Real labor | Counted in COGS | Reported GP% |
|---|---|---|---|---|---|
| Shop Work | Ready to Invoice | 587.00 | $19,166.09 | **$0.00** | **100.0%** |
| Jenny Ln 2 | Ready to Invoice | 153.75 | $4,952.20 | **$0.00** | **100.0%** |
| Adam Burton | Completed | 20.00 | $677.04 | **$0.00** | **100.0%** |
| David Hodges | Completed | 124.50 | $3,920.66 | **$0.00** | 69.7% |
| Blue Ridge Poultry | Ready to Invoice | 197.50 | $6,028.24 | $594.16 | 95.8% |

Jenny Ln 8 shows how sharp the edge is: **one** unreviewed entry out of 165 hours
removes $284 of cost.

**This is not a bug — it is the Airtable rule, ported faithfully.** It is the question
the audit was actually asked, though: *does the formula mean what the business means?*
With 23 of 61 finished jobs carrying unapproved hours, the review step plainly is not
being done consistently, so the rule and reality have come apart. **The number is
correct per the rule and wrong per the world.**

Two ways out, and it is a business decision, not a technical one:
1. Make approving hours part of closing a job, so "reviewed" means something; or
2. Change final GP to cost **all** labor and keep "reviewed" for the closeout gate only.

Until one of them happens, treat final GP as a **ceiling**, not a figure.

## Finding 2 — eight jobs, $362,471.95 of revenue, zero material cost

> ### ✅✅ CLOSED — owner, 2026-08-12. All eight. **Do not re-raise, do not investigate.**
> *"forget the 8 jobs with no expense. i know that. im not gonna fix em. leave em be."*
>
> This supersedes the "largely explained / slice 3 should still establish which" note below.
> There is no longer a question outstanding: the owner knows which jobs these are, knows the
> material cost was never recorded, and has decided not to correct them. **A future audit that
> "discovers" $362,471.95 of revenue with no material cost has found a decision, not a defect.**
>
> What it does NOT change: any company-wide profit total that includes these eight is still
> overstated. That is now a known property of the book rather than a gap to close — see the
> summary at the end of this file.
>
> *Superseded detail from 2026-08-11, kept for the reasoning:*
> *"some of the jobs on record are in here for records sake. it was before i kept track of
> expenses so they will be wrong and im not gonna fix them."*
>
> These are **deliberately incomplete historical records**, kept for reference, from before
> expenses were tracked in this system. The missing material cost is known and accepted, and
> the jobs will not be corrected.
>
> **What this changes:** this stops being an open question about data integrity and becomes a
> known property of the book. **What it does NOT change:** any total that includes these jobs
> still overstates profit, so a headline GP figure is only meaningful with them excluded.
>
> ⚠ The owner said **"some"**, not "all". Eight jobs carrying $362,471.95 of revenue is a lot
> to write off wholesale, and the two biggest — Cambridge DG and Wheeling DG — have **fully
> reviewed labor**, which is not the profile of an untracked job. **Slice 3 should still
> establish which of the eight are pre-tracking records and which, if any, are current jobs
> genuinely missing cost.** That is a question for the QuickBooks reconcile, not a defect to fix.

Eight finished jobs with revenue over $10k record **no materials, no wire and no pipe at all**:

| Job | Revenue | Materials | COGS is entirely | GP% |
|---|---|---|---|---|
| Cambridge DG (31593) | $106,500.00 | **$0.00** | labor, $14,530.40 | 86.4% |
| Wheeling DG 31538 | $100,920.00 | **$0.00** | labor, $19,422.49 | 80.8% |
| Jenny Ln 14 | $28,733.15 | **$0.00** | labor, $5,351.33 | 81.4% |
| Jenny Ln 2 | $28,733.15 | **$0.00** | nothing at all | 100.0% |

A $106,500 Dollar General build with 488.75 hours of labor and **$0 of material** is not
a profitable job — it is an incompletely recorded one. Note these are not the
unreviewed-hours cases: Cambridge, Wheeling and Jenny Ln 14 have **all labor reviewed**.
The labor side is complete and the material side is empty.

Most likely explanations, in order, none yet confirmed:
- materials bought and billed by the general contractor, never NEE's cost at all;
- materials recorded only in the inventory app before the expense push existed
  (that path only started writing Neon on 2026-08-10 — `ROADMAP` §4 Step E);
- materials invoiced directly in QuickBooks and never entered here.

**Slice 3 answers this and nothing else can.** If the first explanation holds, these
margins are real and the book is fine. If the third holds, GP is overstated by six
figures.

## Finding 3 — two T&M jobs bill nothing for their hours — ✅ FIXED by the owner 2026-08-11

`Labor Revenue (T&M) = Hours Rollup × Billable Hourly Rate`, so a blank rate makes the
revenue side zero while cost stays real. Both jobs report a loss they may not have made:

| Job | Hours | Billable rate | Labor revenue | Reported GP |
|---|---|---|---|---|
| Lance Koehn | 90.75 | **not set** | $0.00 | **−$2,469.57** |
| Kenny Barkan | 18.50 | **not set** | $0.00 | **−$645.93** |

(A third, Andy Alleman, has no hours and is harmless.) At a typical billable rate this
is roughly $8–10k of revenue not being counted. **Either the rate is missing or those
hours are genuinely not billable — a two-minute answer from the owner, then it is fixed
in the job record, not in code.**

## Finding 4 — 6,310 hours belong to jobs that are not in this system

Neon's `jobs` table holds **112 jobs, every one of them `job_year = 2026`.** There are
no 2025-or-earlier jobs in it at all.

So the 6,355 unlinked hours from 2025 onward are not a broken link — **74 of those 78
job names do not exist in Neon**, because those jobs were never migrated. Their
profitability is not understated; it is simply **absent**. Airtable still holds that
history.

> This is a scope fact, not an error, and it is the single most important thing to know
> before trusting a total: **this book covers 2026.** Any question about 2025 or earlier
> profitability cannot be answered from this system today.

The genuinely broken links are small: **4 names / 36.75 hours** matching 3 jobs Neon
does know (Tim Yoder 18.0 h, Office Work 10.0 h, Hardwood Solutions 8.75 h across two PO
spellings). ~$1,200 of labor. Tim Yoder is the only one that matters — it currently has
**no costed hours at all** against a −$195.00 final GP.

## Finding 5 — nine expenses carry impossible dates — ✅ FIXED 2026-08-11

Eight sat at **1969-12-31** — the Unix epoch, i.e. a null timestamp that became a date —
and all eight are the dead wire/pipe path (`"Job (PO) | 2\" PVC SCH40"`). One sat at
**0004-03-04**, $291.74 of Materials on Bethel School with no description. $1,776.12 total.

**The bad dates were in AIRTABLE, not a Neon conversion artifact.** Checked before changing
anything: `fldCCPYdyWAOGchWb` on those records literally held `"1969-12-31"`. Neon had copied
them faithfully, so fixing only Neon would have been undone by any re-sync.

**The true date does not exist anywhere.** The upstream `Pipe Usage` rows that generated these
expenses carry `usage_date = 1969-12-31` as well — the date was never recorded, so there was
nothing to recover.

**What was used instead: each record's own creation date** (eight at 2026-04-01, one at
2026-05-22). That is a real, verifiable fact about the record rather than an invented date, and
it is within days of when the cost was booked. **It is a proxy, not the purchase date** — if the
real dates ever surface, these nine should be corrected rather than trusted.

Fixed in **both stores**, Airtable first. Verified after: 0 impossible dates, 0 nulls, still 393
expenses, and **total final GP unchanged at $998,953.10** — confirming the prediction that GP
sums by job and never by date.

> ✅ **CLOSED 2026-08-12 — the 8 upstream `pipe_usage` rows are fixed too.** Airtable first
> (`tblgxbgpovXj6myZB`), then Neon, same as the expenses.
>
> ⚠⚠ **`Pipe Usage` IS A DEAD TABLE — owner 2026-08-12:** *"i no longer use that, it all comes
> from inventory as an expense so i dont need them."* Nothing will ever be added to it again.
> **But do NOT delete it.** `pipe_cost` still feeds `actual_job_cost_cogs` for the historical
> jobs that used it, so dropping the table silently changes the closed profit on those jobs —
> the same trap `legacy_material_cost` exists to prevent for the 24 pre-April expenses
> (`ROADMAP.md` §4d). Frozen history, not live data. Same applies to `wire_weigh_ins`.
>
> ⚠ **They did NOT all get the same date, and that mattered.** The first pass set all eight to
> `2026-04-01` to match the expenses they generated — then their Airtable `createdTime` showed
> three of them (the Harlin Smith rows) were created **2026-01-30**. The pipe was used in
> January; the automation only turned it into an expense in April. Each row now carries **its
> own** creation date, which is the better proxy for when the pipe was actually used.
>
> **Every money table is now free of impossible dates:** expenses 0, pipe_usage 0, and
> `wire_weigh_ins` never had any (39 rows, 2026-01-01 → 2026-04-14). Total final GP unchanged at
> **$964,332.04** either side, confirming again that GP sums by job and never by date.

## Finding 6 — Strongsville DG, confirmed and still unexplained

$1,800.00 revenue against $29,562.32 of cost, final GP **−$27,762.32**. Open since
2026-08-05 and not part of the zero-revenue set. Also worth knowing: there are **two**
jobs named Strongsville DG (one Contract, one Service Calls, the latter with $638.35 of
cost and no revenue), which is exactly the duplicate-name shape that has caused
cross-job leakage elsewhere. **Put it in the slice 3 sample.**

## Finding 7 — Jenny Ln 1 has been "Ready to Invoice" for seven months

164.50 hours, $5,026.67 of cost, no revenue, last time entry **2026-01-20**. It is on the
accepted zero-revenue list, so the number is known — but the *status* is a live signal
that it was either billed in QuickBooks or never billed at all. It is the only job in
the book that is stale in this way.

## Finding 8 — Shop Work invents $38,155 of revenue

Shop Work is overhead, but it is typed **Time & Material** and carries a billable rate,
so `hours × rate` = 587.00 × $65.00 = **$38,155.00** of "revenue" that nobody was ever
invoiced for. Combined with Finding 1 zeroing its labor, it reports **$38,155 of pure
profit that does not exist.** Any total that includes it is wrong by that amount.

---

## What this means for the totals

| | |
|---|---|
| Reported total final GP, 61 jobs | **$998,953.10** |
| Less labor never counted (Finding 1) | **−$34,584.50** — exact |
| Less Shop Work phantom revenue (Finding 8) | **−$38,155.00** — exact |
| Less unrecorded materials (Finding 2) | unknown; **owner-accepted** for the pre-tracking jobs, unquantified for the rest |
| T&M revenue not billed (Finding 3) | ✅ resolved — rates set by the owner 2026-08-11 |

**A defensible working figure is under $930,000**, against $998,953 reported, and that is
before any materials adjustment. The direction is consistent and one-way: **this system
reports more profit than the business made.**

The honest summary for anyone asking "can I trust the GP numbers?":

- **Per-job, on a current job with reviewed hours and tracked expenses — yes.** Bethel
  School checked out by hand and by diff.
- **As a company-wide total — no, not yet.** It includes jobs that were never meant to be
  complete, labor that was never approved, and at least one overhead job inventing revenue.
- **Against the actual books — unknown.** That is slice 3, and nothing internal can answer it.

## ⛔ Slice 3 is NO LONGER REQUIRED — the question it existed to answer is closed

Slice 3 (the QuickBooks reconcile) was scoped to settle **Finding 2**, the eight jobs with no
recorded material cost. The owner closed that by decision on 2026-08-12, so **there is nothing
left for it to establish** and no export is needed.

Run it only if the question ever changes from *"is the material cost missing?"* (answered: yes,
knowingly) to *"do this system's figures agree with the books?"* — a different and much larger
question that nobody has asked. The sample below is kept because it is still the right sample
if that day comes.

*Original scope, retained:*

## What slice 3 would settle

Only a QuickBooks reconcile can close Finding 2, which is the big one. Recommended
sample, chosen to hit each failure mode rather than at random:

1. **Cambridge DG (31593)** — zero materials, all labor reviewed, $106.5k
2. **Wheeling DG 31538** — same shape, second instance
3. **Strongsville DG (Contract)** — the standing anomaly
4. **Jenny Ln 1** — Ready to Invoice for 7 months, was it billed?
5. **Shop Work** — is any of that $38,155 real?
6. **Bethel School** — a job that looks healthy, as a control

Ask of each: what did QuickBooks actually invoice, and what did materials actually cost?
Every difference gets a named cause, not a tolerance.
