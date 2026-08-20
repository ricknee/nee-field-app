# Plan — give billing allocations a write path

**Status: ✅ BUILT AND CUT OVER 2026-08-11 — this plan is CLOSED.** `ALLOCATIONS_WRITE=on`, all
four Airtable automations are undeployed, and the app owns allocations. Roadmap §8; audit item 03,
closed (see `docs/ROADMAP.md` §8 and `docs/AUDIT-airtable-remaining.md` item 03).

> **Everything below this line is historical.** It is the record of how the flip was staged and
> what was found on the way — not outstanding work. Where the text says the switch is inert, the
> automations are live, or the cutover is pending, read it in the past tense.

Built: `netlify/functions/_allocations.js`, wired into `handleUpdateTimeEntryPayroll`,
`handleApproveExpense` and `handleSaveInvoice`. 148 tier-1 tests pass, 2 new — including one that
proves the switch is genuinely inert by failing if Airtable is touched at all while it is off.

**What remained at the time of writing was §3 — the cutover — plus the §6 diff, and one thing
found while building. All of it is now done; the item below was resolved by going Neon-native:**

> ### ⚠ 24 billable time entries (48.25 h) cannot be billed AT ALL, and it grows
> Found 2026-08-11 while writing the gate. An allocation's `Time Entry` field is an Airtable
> **link**, so a time entry with no Airtable twin has nothing to point at. As of today that is
> **30 entries, 24 of them billable, 48.25 h**, none reviewed yet — so nothing is lost *yet*.
> **The old automation had exactly the same blind spot and could not even report it**, because a
> record that is not in Airtable cannot trigger an Airtable automation.
>
> It grows: the QB puller and the time clock both write Neon-only rows. `createLaborAllocation`
> returns `skipped: "no-airtable-twin"` and logs a warning, so at least it is visible now.
>
> **The real fix is allocations going Neon-native**, which requires `_billing-sync.js` to stop
> treating Airtable as the authority on existence (§2). That is its own piece of work and should
> be scheduled before the untwinned population gets large enough to matter.
>
> **✅ RESOLVED 2026-08-11 — allocations are Neon-native (`db/schema/033_neon_native_allocations.sql`).**
> The untwinned population is no longer a problem because an allocation no longer needs an Airtable
> twin to exist. Two rules came out of it and are live constraints, not history:
> `_billing-sync.js` needs **both** the empty-array guard **and** `airtable_id IS NOT NULL`, or it
> deletes every native row within the hour; and the **native** insert must write `bill_rate` (the
> mirror must not), or those hours value at $0 in `v_invoices` while still printing on the PDF
> (`db/schema/036_native_allocation_bill_rate.sql`).

**One-line (as written 2026-08-11, now superseded):** The app reads billing allocations but cannot
create or link one — four Airtable automations do that, and they are the only reason anyone still
has to open Airtable in normal operation. *Since the cutover the app both creates and links them,
and those four automations are undeployed.*

---

## 1. What the four automations actually do

Fetched from the live base 2026-08-11 and decoded field-by-field, because "creates labor
allocation" is not a specification.

| id | fires on | conditions | does |
|---|---|---|---|
| `wflTwXb6dG32FFv9s` | Time Entries | `Billable` ✓ **and** `Unbilled Hours` > 0 **and** `Labor Billing Allocations` **empty** **and** `Labor Reviewed` ✓ | create allocation `{Time Entry: [id], Allocated Hours: <the entry's Hours>}` |
| `wflNmJsnIhWtSjUlL` | Expenses | `Billable?` ✓ **and** `Unbilled Material Amount $` > 0 **and** `Material Billing Allocations` **empty** **and** `Reviewed` ✓ | create allocation `{Expense: [id], Allocated Material Amount $: <the expense's Unbilled Material Amount $>}` |
| `wflOcxtmkzdxKMVQW` | Invoices | `Auto Allocate?` ✓ | find ≤1000 **labor** allocations where the allocation's Job lookup = the invoice's Job **and** its Invoice link is empty → set Invoice |
| `wfl7bzJpZY9kcJ27i` | Invoices | `Auto Allocate?` ✓ | same, for **material** allocations |

Two details that a paraphrase loses and the port must not:

- **The labor allocation is written with `Hours`, but gated on `Unbilled Hours` > 0.** Those are
  different fields. They are equal *at that moment* only because the third condition guarantees no
  allocation exists yet, so Billed Hours is 0 and Unbilled == Hours. Reproduce it as: gate on
  unbilled, **write the full `Hours`**.
- **The material allocation writes the same field it gates on** — `Unbilled Material Amount $` —
  so it allocates the unbilled remainder, not the full amount. The two halves are NOT symmetric.

**"Unallocated" is the idempotency guard**, and it is the third condition in both creators. That is
what makes review → un-review → re-review safe today, and it is what the port has to reproduce.

## 2. ⚠⚠ The constraint that decides the whole design

`_billing-sync.js` runs hourly and finishes with:

```sql
DELETE FROM labor_billing_allocations    WHERE NOT (airtable_id = ANY($1::text[]))
DELETE FROM material_billing_allocations WHERE NOT (airtable_id = ANY($1::text[]))
```

where `$1` is every id fetched **from Airtable**. It exists for a good reason — un-allocating in
Airtable deletes the row, and upsert-only sync would leave an orphan that keeps billing a customer.

**The consequence: any allocation that exists in Neon but not in Airtable is deleted within the
hour.** A Neon-native row with a synthetic `app:…` id — the pattern used for `labor_cost_rates` —
would vanish, and the invoice total would silently drop *after* looking correct. No error anywhere.

**So this table inverts the usual rule. Write AIRTABLE FIRST, take the returned rec id, then
insert into Neon with it.** That is the opposite of the Neon-first contract used for time
entries, expenses, estimates and invoices, and it is correct *here specifically* because the
hourly sync makes Airtable authoritative for **existence**.

Failure modes under that ordering:
- Airtable create fails → nothing written anywhere, error surfaces to the user. Clean.
- Airtable succeeds, Neon insert fails → row exists in Airtable only; **the hourly sync adopts it**
  within the hour. Self-healing, and the invoice total is briefly low rather than wrong forever.

> ### ⚠ THIS SECTION WAS WRONG, AND IT WAS FIXED THE SAME DAY — see `db/schema/033`
>
> It used to end: *"The alternative — teaching `_billing-sync.js` to spare app-created rows — is
> strictly worse: it would have to distinguish 'app created this' from 'Airtable deleted this',
> which is the exact ambiguity the delete pass exists to resolve."*
>
> **That reasoning was sound and the conclusion was still wrong**, because it assumed the
> ambiguity was real. It is not: a row with a **NULL `airtable_id`** can never have been deleted
> in Airtable, because it was never there. `airtable_id IS NOT NULL` separates the two cases
> exactly, with no heuristic.
>
> The cost of believing it: the first version refused to allocate any entry without an Airtable
> twin — which, from the week of 2026-08-10, is **100% of them**. Ten minutes after cutover the
> owner reviewed two real entries and got nothing. Allocations are Neon-native now, and the
> hourly delete pass carries the one-line guard.

## 3. ⚠⚠ The deployment sequence, which is riskier than the code

**The automations cannot stay on while the write path is live.** Both would fire: the handler
creates an allocation, the Airtable record-change then triggers the automation, whose
"unallocated" condition may already be stale. The result is **two allocations for one time entry**
— and since `v_invoices.invoice_total_calc` sums them, that is **double-billing a customer**.

The existence check cannot close this on its own: it is a race between our write and Airtable's
trigger, not a logic gap.

So the code ships **inert**, behind an env-var kill switch, exactly like `TIME_CLOCK_PAYROLL` and
`LOGIN_SOURCE`:

```
ALLOCATIONS_WRITE          unset | off   -> handlers do nothing, automations own it (today)
ALLOCATIONS_WRITE = on                   -> handlers own it, automations MUST be undeployed
```

**Cutover, in order, and it is a single sitting:**

1. Ship the code with the switch unset. Nothing changes. Soak a deploy.
2. Undeploy all four automations in the Airtable UI.
3. `netlify env:set ALLOCATIONS_WRITE on` and deploy.
4. Review one time entry and one expense; confirm exactly ONE allocation each, in both stores.
5. Save an invoice with Auto Allocate; confirm the right allocations attached and the total matches.

**Rollback is the reverse and takes a minute:** unset the var, re-deploy the four automations.
Because the automations' conditions are *state-based* rather than event-based, re-enabling them
picks up anything missed while they were off — they will create allocations for any reviewed,
unallocated, billable record. That is why this is safe to flip back.

> ⚠ Do NOT delete the automations. Undeploy them. Deleting throws away the only specification of
> the behaviour, and step 1 of this file is a transcription, not the original.

## 4. What gets built

`netlify/functions/_allocations.js` — shared, because the invoice half is called from a different
handler than the creators:

- `createLaborAllocation(timeEntry)` — gated on billable + unbilled hours > 0 + no existing
  allocation + labor reviewed. Airtable create, then Neon insert.
- `createMaterialAllocation(expense)` — same shape, material fields.
- `attachAllocationsToInvoice(invoiceId, jobId)` — finds unlinked allocations for the job in Neon,
  sets the Invoice link in Airtable, then mirrors to Neon.

Wired into the existing writes, in the same transaction as the thing that triggers them:

| handler | already writes | add |
|---|---|---|
| `handleUpdateTimeEntryPayroll` | `labor_reviewed` | create labor allocation when it flips ON |
| `handleApproveExpense` | `reviewed` | create material allocation when it flips ON |
| `handleSaveInvoice` | the invoice | attach unlinked allocations when `autoAllocate` |

## 5. What this kills, deliberately

`index.html` carries a documented fallback for the *"brief automation lag between Review and
allocation row creation"*. Writing the allocation synchronously removes the lag, which makes that
fallback dead code. **Delete it in the same commit, not later** — a fallback for a condition that
can no longer occur is a trap for whoever reads it next.

## 5a. Cutover baseline, captured 2026-08-11 before flipping anything

Snapshot table `_alloc_cutover_baseline` holds per-invoice totals. Headline figures:

| | |
|---|---|
| labor allocations | **2,606** (1,504 unlinked), 8,364.75 h |
| material allocations | **252** (88 unlinked), $684,176.89 |
| `sum(invoice_total_calc)` | **$1,188,617.34** |

**Three fidelity checks run against the port before cutover, all clear:**

- **The automation caps its find at 1,000 records; the port has no cap.** Moot in practice — the
  largest unlinked-labor pile on any single job is **277**. Worth re-checking if a job ever gets
  near it, since at that point the two would genuinely diverge (and the port would be the
  *correct* one).
- **The port resolves a job through the Neon FK; the automation used Airtable's Job lookup.**
  0 unlinked labor allocations have a time entry with no job, so the two agree on every row.
- **2 orphaned allocations** (one labor 0.50 h, one material $110.00) have **no parent link at
  all** — no Time Entry, no Expense. The port skips them because it cannot resolve a job; **the
  automation skipped them too**, because its Job lookup resolves *through* the missing parent and
  is therefore empty. No divergence. They are unlinked, so they contribute nothing to any invoice
  total. Junk worth deleting one day, not a blocker.

## 5b. ⚠ The undeploy is an OWNER action — there is no API for it

`update_automation` edits the **draft** only; its own documentation says live behaviour is
unchanged until applied in the Airtable UI. `delete_automation` exists but deleting is exactly
wrong here — these four are the only remaining specification of the behaviour, and §1 above is a
transcription of them, not the original.

**So step 2 of the cutover is four toggles in the Airtable UI**, same shape as the Jobs-mirror
sync freeze in `AUDIT-airtable-remaining.md`: Airtable exposes no API for it.

## 6. The gate before it counts

Allocations decide what a customer is billed. This gets the `013`/`015` treatment: after cutover,
`v_invoices.invoice_total_calc` must reproduce every invoice total it did before, and the labor and
material allocation counts must match Airtable exactly. **Diff before trusting, not after.**
