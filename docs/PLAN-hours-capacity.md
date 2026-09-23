# Hours: job burn-down, and the year's workload

**Owner's question, 2026-09-23:** *"How many hours do I have left working in the year, and how do my
awarded projects' estimated hours compare?"* Two stages — one job, then all of them.

Both stages shipped 2026-09-23.

---

## The measurement that shaped this (taken before writing any code)

| | |
|---|---|
| Awarded jobs | **21** |
| …carrying an estimate | **9** |
| Estimated on those 9 | 4,616 h |
| Worked on those 9 | 3,124 h |
| **Remaining** | **~2,814 h** |
| Working days left in 2026 (weekends + `company_holidays` excluded) | **69** |
| Capacity at 4 people × 8 h | **2,208 h** |
| **Gap** | **~600 h over** — 17.6 weeks of work against 13.8 weeks left |

**And that is before the twelve awarded jobs with no estimate at all**, five of which have never been
worked (Andy Alleman, Craig Davidson Garage, Neal's Shop, New Shop, Classical Fuel Tank). Trail
Cabinet has **667 hours booked against no estimate**.

> ⚠⚠ **This is the whole design constraint.** A backlog built from estimates counts 9 of 21 jobs and
> reads **LOW** — it says there is room in the year when there isn't. Nothing about the number would
> show that twelve jobs were missing from it. Same failure as every other one in this codebase: it
> doesn't throw, it just matches nothing, and that reads as "no data".

---

## Stage 1 — one job (SHIPPED 2026-09-23)

**`jobs.expected_hours`** (`db/schema/079`), nullable, an **override**:

```
target = COALESCE(expected_hours, est_labor_hours_rollup)
worked = hours_rollup                    -- every time entry on the job
```

* Set in **Project Info → Edit → ⏱ Expected Hours**, so it saves through the tab's one save bar
  (`PI_SECTIONS`) with no new button. The edit hint says what a blank box falls back to.
* Read as the **⏱ Hours strip** on Project Info (admin/office): `1,416 h target · 1,323 h worked ·
  93 h left (93%)` with a bar that turns amber at 90% and red when over.
* **No target renders a PROMPT, never a dash** — *"no hours target set — ✏ Edit to set Expected
  Hours, or this job is missing from the year's workload"*. The dash is what let twelve jobs go
  missing.
* **Deliberately not backfilled** from the rollup: a backfill freezes today's estimate into a column
  that then stops tracking change orders, and erases the difference between "we estimated this" and
  "someone typed a number" — which the strip shows as provenance.

### Traps found while measuring (do not re-derive)

1. **Estimates are ADDITIVE, not versions.** Bethel School has seven (1300 + 16 + 30 + 20 + …):
   change orders land as new estimates. Summing is right — but one bad row inflates a job silently,
   which is half the reason a human override exists.
2. **⛔ Never filter the rollup to `Approved`.** Sullivan Pullet's **900 h — the single biggest piece
   of remaining work on the books — sits on a `Sent` estimate.** `v_job_rollups.est_labor_hours_rollup`
   already sums `Archived/Completed | Sent | Approved`. Narrowing it drops that job and the total
   just looks smaller.
3. **`hours_rollup` is every time entry on the job, unfiltered.** PTO and paid holidays are
   `source='Manual'` and hang off overhead buckets (Shop Work / Office Work), not real jobs — fine
   per job, and it keeps them out of job work in stage 2 too. Re-check if leave ever gets booked to
   a customer job.
4. **NULL is not 0.** `n()` would flatten "nobody has said" into "a target of zero hours", which
   reads as 100% complete the moment anyone books an hour. The emit keeps null null; there is a test.

---

## Stage 2 — the year’s workload (SHIPPED 2026-09-23)

Its own top-bar button (owner's call): **📊 Workload**, in the ☰ menu beside 📊 Hours by Job — that
is where someone already goes to ask *where did the time go*; this asks it forwards.

**Admin + office** (`_ADMIN_OFFICE_READS`) — owner's call. It was strict admin for a day on the
reasoning that the order book sits with payroll; it doesn't. Scheduling the work it describes is
office's job, and unlike 👥 People it carries **no wage figure** — hours committed, hours left, and
who is on the crew by name. The crew checkboxes (`setCapacityCrew`) sit at the **same** tier
deliberately: a control that is guaranteed to 403 reads as a broken feature, not a locked one.

```
backlog  = Σ max(target − worked, 0)   over AWARDED jobs
capacity = people × hours/day × working days remaining − booked PTO
```

* **Working days** come from `company_holidays`, which the PTO screens already own — not a hardcoded
  list. Two holidays remain in 2026.
* **Headline in two units**: hours over/short, *and* **weeks of work in hand vs weeks left**. The
  second is the one a contractor feels.
* **⚠⚠ COVERAGE ON THE FACE OF IT, always**: *"18 of 21 awarded jobs have hours set — 3 not counted"*.
  A backlog without its coverage is a confident wrong answer. If this line is ever dropped to make
  the screen tidier, the screen is lying.
* **Per-job table beneath**, biggest remaining first, with **last-worked date** — Bethel School shows
  93 h left but nobody has touched it since 28 Aug, and stalled work is not remaining work.
* **Hours/day and days/week are INPUTS** (remembered per browser), because the question is usually
  *what if*: overtime is how a 600-hour gap actually closes. A six-day week scales the Mon–Fri day
  count by `daysPerWeek/5` — an estimate of overtime capacity, not a promise about which Saturdays.
* **Today it reads:** 2,814 h of work vs 2,184 h of crew time — **630 h over**, 17.6 weeks in hand
  against 13.8 left, ≈157 h of overtime each. Plus the 12 uncounted jobs.

### Decisions taken (owner, 2026-09-23)

* **`Ready to Invoice` is NOT backlog** — *“if it’s ready to invoice then I’m fine working, just haven’t
  invoiced”*. Only `Awarded` counts. Those four jobs carry 628 worked hours against 45 estimated;
  counting them would queue finished work.
* **Work due next year still counts, and is also shown separately** — the *“only work due before the
  year ends”* tick. A job with **no** completion date counts as this year’s: unknown is not a reason
  to look less busy.
* **Approved PTO comes out of capacity** (24 h ahead today). Pending requests do not — they are not
  committed.
* **Crew is a per-person FLAG, not a number** (`db/schema/080`). Active payroll-eligible employees
  count unless flagged out, so *“if I hire somebody, an active employee, I want them included”*
  happens with no action. **Larry Unruh is flagged out**: active, able, simply not scheduled — a fact
  about one person, not his role (he is admin, as are Miles and Rick, who both count). Editable from
  the crew checkboxes on the screen, so the number is never a mystery.

---

## Files

* `db/schema/079_job_expected_hours.sql` — the column and the reasoning
* `netlify/functions/airtable.js` — `expected_hours` in the job read/emit, validated in
  `handleUpdateJobInfo`
* `index.html` — `refreshJobHoursView()` (the strip), `piEditExpectedHours` (the input),
  `piInfoCurrent()` (the string/number normalisation the dirty check needs)
* `db/schema/080_capacity_crew.sql` — `employees.counts_toward_capacity`, and why it is a flag
* `netlify/functions/airtable.js` — `handleWorkload` (facts only) and `handleSetCapacityCrew`
* `index.html` — `openWorkload` / `renderWorkload` (every what-if is computed client-side)
* `tests/handlers.test.mjs` — validation, NULL-vs-0, "no target" stays a prompt, Awarded-only,
  coverage rides with the total, and nobody named in the crew logic
