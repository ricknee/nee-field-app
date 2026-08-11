# Plan — give billing allocations a write path

**Status: ✅ CODE BUILT AND SHIPPED INERT 2026-08-11. ⬜ Cutover not done — `ALLOCATIONS_WRITE`
is unset, so the four Airtable automations are still doing the work.** Roadmap §8 "the one real
piece of work left"; audit item 03.

Built: `netlify/functions/_allocations.js`, wired into `handleUpdateTimeEntryPayroll`,
`handleApproveExpense` and `handleSaveInvoice`. 148 tier-1 tests pass, 2 new — including one that
proves the switch is genuinely inert by failing if Airtable is touched at all while it is off.

**What remains is §3 — the cutover — plus the §6 diff, and one thing found while building:**

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

**One-line:** The app reads billing allocations but cannot create or link one — four Airtable
automations do that, and they are the only reason anyone still has to open Airtable in normal
operation.

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

The alternative — teaching `_billing-sync.js` to spare app-created rows — is strictly worse: it
would have to distinguish "app created this" from "Airtable deleted this", which is the exact
ambiguity the delete pass exists to resolve.

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

## 6. The gate before it counts

Allocations decide what a customer is billed. This gets the `013`/`015` treatment: after cutover,
`v_invoices.invoice_total_calc` must reproduce every invoice total it did before, and the labor and
material allocation counts must match Airtable exactly. **Diff before trusting, not after.**
