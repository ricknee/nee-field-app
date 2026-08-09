# Plan: PTO and paid holidays

**Status:** **FOUNDATION BUILT 2026-08-08 (schema `023` + `024`, applied to prod, inert).
App layer NOT built.** Nothing in the UI records PTO yet, so nothing has changed for anyone.

Owner's requirements, 2026-08-08:
> "i need PTO. i need to be able to track how much [is] taken. we pay 2 weeks pto every year plus
> 6 major holidays. also in the payroll pdf to the accountant we need to be able to tell them how
> many hours were pto or paid holiday."

---

## 1. Decisions already taken (don't re-litigate these)

| Question | Answer |
|---|---|
| Who is tracked? | **Employees only.** The salaried people are the owners — "they take time off without tracking." |
| The 6 holidays | **Auto-filled**, nobody records them. Applies to the same employees-only population, since holiday *hours* only mean something where hours drive pay. |
| Allowance | **Per person**, and unused hours **carry over**. |
| Salaried on the payroll PDF | **Unchanged.** Confirmed already true — the salaried branch prints "Bi-weekly Salary" and city-tax hours only, ignoring hours entirely. |

**Salaried jobsite time needs no work.** A salaried owner clocking a jobsite already flows into
`time_entries` → GP and T&M billing, while their payroll PDF stays salary-based. That falls out of
the existing design.

## 2. What is BUILT

- **`company_holidays`** — date, name, hours (default 8). Dates, not rules: six a year is less work
  than a rule engine that's wrong whenever a holiday lands on a weekend.
- **`pto_years`** — per employee per year: `allowance_hours`, `carried_in_hours`. Carry-in is
  *stored, not derived*, so closing a year is an explicit act with a visible number rather than a
  recursive calculation that silently changes when an old entry is edited.
- **`v_pto_balances`** — entitled / used / remaining. **Used is derived** from time entries, so
  correcting a mis-entered PTO day fixes the balance automatically.
- **`024` — the GP correctness fix.** ⚠⚠ The important one. See §3.

## 3. ⚠⚠ The trap this feature walks into

PTO and holidays are recorded as ordinary `time_entries` rows (`class = 'PTO'` / `'Paid Holiday'`,
no job). That is right — they are hours, and payroll already sums hours. But **they are not hours
worked**, and two places compute overtime from hours:

1. **The payroll PDF.** `wk1OT = max(0, total - 40)`. A week of 40 worked + 8 PTO must be
   40 regular + 8 PTO, **not** 40 regular + 8 overtime. Getting this wrong overpays every holiday
   week. **⬜ NOT YET FIXED — this is the first job in §4.**

2. **`v_job_labor_cost_true`'s OT denominator** — summed every hour of the week, every job plus
   unlinked time, and priced the excess into each job at 1.5×. ✅ **Fixed in `024`.** Verified: a
   40-worked + 8-holiday week gave an OT denominator of 48 h → 8 h of overtime charged to the job
   under the old rule, and 40 h → 0 h under the new one. Also verified the change is a **no-op on
   today's real data** (57 jobs, 10,461.75 h, $339,051.97 live cost — identical before and after),
   because no PTO rows exist yet.

   This is structurally the same failure as the manual `Reviewed` checkbox that produced a ~$179k
   hole in closeout: a denominator that quietly stopped meaning what the formula assumed.

## 4. What is LEFT to build

1. **Exclude PTO/Holiday from the payroll PDF's OT split** (§3 item 1). Money-critical; do first.
2. **Report PTO and Paid Holiday hours on the payroll PDF** — the accountant's actual ask. Per
   employee, per week, alongside Reg/OT.
3. **Add `PTO` and `Paid Holiday` to the class list** — and note they must NOT be offered as clock
   classes (you don't punch a holiday); they're payroll-entry classes.
4. **Holiday auto-fill** — an action that materialises 8 h `Paid Holiday` entries for eligible
   employees on each `company_holidays` date. Must be idempotent (re-running creates nothing new)
   and must skip anyone who actually worked that day.
5. **PTO balance UI** — remaining hours on ⏱ My Hours for the employee; allowance editing on the
   People screen for admin.
6. **Seed the data** — the 6 holiday dates for 2026 (⬜ owner has not yet said *which* six), and
   `pto_years` rows for Jeff and Patrick.
7. **Year-end rollover** — an admin action creating next year's `pto_years` rows with
   `carried_in_hours` = this year's remaining. Deliberately manual and explicit. Not needed until
   December.

## 5. Open question

**Which six dates?** The usual set for a contractor here would be New Year's Day, Memorial Day,
Independence Day, Labor Day, Thanksgiving, Christmas — but that is a guess and it decides what
people get paid, so it needs confirming rather than assuming.
