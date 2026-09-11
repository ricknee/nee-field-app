# Vendor-invoice review — supplier invoices that can't find their job

**Status: LIVE 2026-09-09.** The Mini Bee is sending and `vendor_invoices` has real
rows. **Lowe's and Contractor Lighting & Supply were added the same day** — four
vendors are accepted now, not two.

⚠ The bot runs on a **separate box that is unreachable from this repo.** Its only
fingerprint here is the `cedautomation` employee; grepping this codebase finds nothing
of it. Changing what it sends is a change over there, not a deploy here — see §4 for
the contract, and `docs/minibee-invoice-change.md` for the copy handed to that project.

---

## 1. What was already happening

An external bot signs in as the **`cedautomation`** employee (role `office`, Neon id
`4456a2fa-b8eb-4595-9428-61493a61b90c`, created 2026-08-28) and calls
`addGeneralExpense` once per CED/Wolff invoice, having matched the invoice's PO to a
job itself. **215 expenses that way so far — 168 CED, 47 Wolff**, latest Wolff
2026-09-03, latest CED activity today.

Upstream of it, Make files the PDFs into pCloud on a daily timer:

| Scenario | Time | What it does |
|---|---|---|
| `CED 26` / `Wolff 26` | 05:00 / 06:50 | Gmail → pCloud `…/Incoming Invoices` |
| `CED Watch PCloud` / `Wolff Watch PCloud` | 05:30 / 07:00 | PDF.co AI parse → rename `CED - {PO} - $amt.pdf` → `…/Invoices` |
| `… Delete Filed Invoices` | 07:20 | clears the incoming folder |

**Nothing in Make creates an expense, and nothing anywhere said "needs review".**
That phrase described a gap, not a state.

### The hole

`handleAddGeneralExpense` requires a job — no `jobId`, or one that fails
`isJobHandle`, is a **400**. So an invoice whose PO matched nothing had nowhere to
go. It wasn't rejected loudly; it simply never became an expense, and the only trace
was a PDF in pCloud.

**And the app cannot read pCloud.** `PCLOUD_ACCESS_TOKEN` is unset and unobtainable —
pCloud's app-registration page has been down for months (see `_pcloud.js` and
`PLAN-job-photos.md`). So a review screen could never be built by listing that
folder. It has to be fed by the bot. That single fact drove the whole design.

---

## 2. What was built

**Every invoice now lands in `vendor_invoices` first and becomes an expense second.**
The bot stops matching; the app matches.

```
bot ──POST vendorInvoiceIntake──▶ vendor_invoices row (+ PDF in R2)
                                        │
                                  PO match on jobs
                                        │
              ┌─────────────────────────┼─────────────────────────┐
        exactly one                  zero                   two or more
              │                        │                         │
      expense created           status = needs_review     status = needs_review
      status = matched          reason = no-job-match      reason = ambiguous-po
      (unchanged behaviour)     / no-po-on-invoice        (both jobs named on screen)
                                        │
                                🧾 Invoice Review
                                 assign │ reviewed
```

### Why the app does the matching, not the bot

One copy of the rule instead of two that can drift, a permanent log of every invoice
that ever arrived, and — the real reason — the automatic path and the by-hand path
run through the **same** `settleVendorInvoice`. A screen that creates expenses
differently from the robot is a screen that will one day create a different expense.

### Why a table and not a job-less expense

The cheap version is to let the bot write an expense with `job_id NULL` and call the
review list a filter over `expenses`. **Don't.** Every job view, GP view and
allocation query joins expenses to a job, so a job-less expense would be a real money
row that appears on no screen and in no total — this codebase's characteristic
failure: *it does not throw, it matches nothing, and matching nothing reads as "there
is no data."* An invoice awaiting review is not an expense yet.

### ⚠⚠ Two matching jobs park — they are never guessed between

Not hypothetical. **`Harlin Smith (2 Barn)` and `Rebecca Smith (2 Barn)` both reduce
to `2BARN` in production today.** Taking the first row puts one customer's material on
another customer's job, where nothing surfaces it: the job still has a plausible cost,
the GP is merely wrong, and the invoice still looks filed. A person resolves it in ten
seconds from the screen; code cannot resolve it at all.

### ⚠ The two vendors disagree about spacing, and neither is wrong

PDF.co's CED template reads **`CAJ 436`**; its Wolff template reads **`CAJ436`** off
the same job's paperwork; `jobs.po_locked` spells it **`Joe Yoder (CAJ 436)`**.
Compare any two literally and nothing matches. Both sides are reduced to A–Z0–9,
uppercased (`normalizePoCode` / `JOB_PO_CODE_SQL`) before comparison.

`COALESCE(po_locked, po)` is load-bearing: **14 jobs have only `po`**, all New Lead or
Not Awarded — exactly the jobs young enough to still be buying material. Reading
`po_locked` alone would park every one of their invoices.

### ⚠ Wolff prints credit memos with a TRAILING minus

The live sample's total is `773.85-`. `Number("773.85-")` is `NaN`, which would store
NULL — a returned $773 of gear costing the job nothing back, with no error anywhere.
`parseSignedAmount` handles trailing, leading and parenthesised minus plus thousands
separators.

A credit goes to the expense's **`material_credit`**, a normal invoice to
**`manual_material_cost`**. They are two columns, not one signed one: `v_expenses` and
every GP rollup read them separately and subtract the credit themselves, so a credit
written as a negative cost is counted twice.

### ⚠ A zero balance is NOT a credit  *(added with Contractor Lighting, 2026-09-09)*

Contractor Lighting prints a **running balance**, so an ordinary charge can show
`0.00` — and a zero-balance *credit* looks identical on the paper. Nothing in the
number says which it is, so the endpoint does not guess: `0.00` lands in **neither**
expense column. The Mini Bee reads the real credit status off the document and says so
with `"isCredit": true`; a signed amount (Wolff's trailing minus) still works exactly
as before.

The flag is honoured **only when explicitly `true`**, and it can only ever turn an
amount *into* a credit, never out of one — so a bot that sends `false`, or omits it,
cannot accidentally convert a credit memo back into a charge.

Why this is worth a section: a charge filed as a credit **subtracts** from the job's
material cost, so the job's GP reads *better* than it is. That is the same shape as the
estimate-GP bug that ran five years unnoticed — the error was always in the direction
that flatters the number, and **nobody chases a number that looks good.**

### The five accepted vendors

`ACCEPTED_VENDORS` in `_vendor-invoices.js` is the single list; the 400 message and the
review screen's vendor chips are both built from it.

| Accepted spellings (case-insensitive) | `expense_vendors.name` |
|---|---|
| `CED`, `CED CONSOLIDATED ELECTRICAL DISTRIBUTORS, INC.` | CED |
| `Wolff`, `Wolf Bros Supply`, `WOLFF BROS. SUPPLY, INC.` | Wolff Brothers |
| `Lowe's`, `Lowes`, `Lowe’s`, `LOWE'S HOME CENTERS, LLC` | Lowe's |
| `Contractor Lighting & Supply` / `… and Supply` | Contractor Lighting & Supply |
| `Home Depot` / `THE HOME DEPOT` / `HOME DEPOT #4512` | Home Depot |

⚠⚠ **The right-hand column is load-bearing and is not a label.** `vendorHandleFor`
looks the vendor up with `lower(name) = lower($1)`; a name matching no `expense_vendors`
row resolves to NULL and the expense is created **with no vendor on it**. Nothing
throws — the cost lands on the job attributed to nobody, which is this system's
characteristic silent failure. Both new names were read out of Neon on 2026-09-09
before the aliases were written: `Lowe's` still carries an Airtable rec id
(`recNZLNmYciizye23`), Contractor Lighting is native and has only a uuid. Both forms
work because `createExpenseNative` resolves `airtable_id = $8 OR id::text = $8`.
Home Depot (`recwkYML0GVfOonxp`) was verified the same way on 2026-09-10.

⚠ **The allowlist is not "any vendor we know".** `expense_vendors` holds dozens of
names — Menards among them — and being in that table does not make a supplier
acceptable here. This list is the set whose invoices a bot is trusted to turn into money
unattended; everything else is a 400 a person has to look at.

⚠ **Adding a vendor is two places, not one:** the alias here *and* a matching
`expense_vendors` row. Checking only the first gives an endpoint that accepts the
invoice and files it against nobody.

### The invoice PDF becomes the expense's receipt  *(2026-09-10)*

When an invoice becomes an expense — automatically on a PO match, or by hand off
the review screen — its PDF is copied to `expenses/<expenseId>/invoice-<no>.pdf`,
so it shows up in the expense's normal Receipts tile. Someone questioning a cost
two years from now opens the expense; they should not have to know a review queue
exists.

⚠⚠ **COPY, NEVER MOVE.** `vendor_invoices.pdf_key` still points at the original
under `vendor-invoices/<id>/`, and the review screen's "Open the invoice" link is
built from it — moving the object would break that link for every settled invoice
and leave the row pointing at nothing. Both prefixes sit outside the recycle
bin's lifecycle rule, so both copies persist. The files are 15–26 KB.

⚠ **It fails SOFT, and this case is stricter than the usual R2 rule:** the expense
already exists by the time the copy runs. A throw would take the caller's 502
branch and mark the invoice `expense-create-failed` — a real cost on the job under
a row claiming it never landed. A missing receipt is re-attachable by hand; a
lying status is not.

⚠ **`pdf_key` must be SELECTed** in the assign path and carried through in the
intake path. `settleVendorInvoice` copies `inv.pdf_key`, and omitting it from
either caller produces no error at all — just an expense with no paperwork,
found months later by whoever needed the invoice. Both are pinned by a static
test.

### `linkExistingVendorInvoice` — filling the holes in the log  *(2026-09-10)*

`POST { vendor, invoiceNo, po, invoiceDate, amount, jobId, expenseId, pdfKey,
receivedAt? }` → writes ONE `vendor_invoices` row for an invoice that is already
an expense.

It exists because the bot's original path — match the PO itself, call
`addGeneralExpense` — never wrote a `vendor_invoices` row, so the log is missing
exactly the invoices where the system worked. 33 CED expenses in one week, none
of them in the table.

⛔ **IT CREATES NO EXPENSE AND CHANGES NONE.** Not created, not updated, not
reviewed, not billed, not deleted. The named expense must already exist and is
read only to prove the link is genuine. There is exactly one write in the handler
and it is the `vendor_invoices` INSERT — a static test asserts the absence of
`createExpenseNative`, `addGeneralExpense`, and any write to `expenses`.

Seven checks, all 400 on failure with nothing written: the expense exists; it is
on the supplied job; its vendor resolves to the supplied vendor; the normalised
invoice number appears in its description; the amount matches to the cent (a
credit must match `material_credit` **and arrive negative** — a credit is a
separate column, not a negative cost); and `pdfKey` is a receipt that actually
exists under that expense's prefix. R2 unconfigured is a **503**, not a pass —
unverified is not verified.

⚠ **Every check is repeated inside the INSERT's WHERE clause, and that is the
atomicity.** The reads above it exist only to name which check failed. The Neon
HTTP driver's `transaction()` cannot branch between statements, so a transaction
wrapping read-then-write would not close the gap between them; one guarded
statement does — the row cannot land against an expense that changed after it was
read.

Idempotent through the existing `(vendor, invoice_no)` unique index: a re-run
returns `{ duplicate: true, status: "matched", existingId }` and writes nothing.
Success returns `{ ok: true, status: "matched", vendorInvoiceId, expenseId,
createdExpense: false }`.

Rows land with `match_reason = 'historical-expense-link'`, `resolved_by =
'Historical reconciliation'` and a note saying no expense was created — so a
backfilled row never reads as a real-time match on the review screen.

### No Airtable, anywhere

`vendor_invoices` is Neon-native and was built after `AIRTABLE_WRITES=off`. There is
no mirror and there must not be one — a second copy nothing reads, that a re-enabled
sync could one day import back as duplicate expenses.
`tests/handlers.test.mjs` asserts the absence statically.

---

## 3. Files

| File | What |
|---|---|
| `db/schema/069_vendor_invoices.sql` | the table, the dedupe index, the reasoning |
| `netlify/functions/_vendor-invoices.js` | PO normalisation, the match rule, signed amounts, vendor aliases — pure, no network |
| `netlify/functions/airtable.js` | 4 actions + `settleVendorInvoice`; authz entries |
| `netlify/functions/_r2.js` | `vendorInvoicePrefix` — a new top-level `vendor-invoices/` prefix |
| `index.html` | 🧾 Invoice Review button + badge, modal, `viAssign` / `viMarkReviewed` + the job typeahead |
| `tests/handlers.test.mjs` | 5 cases, all offline |

### Actions

| Action | Method | Tier | Notes |
|---|---|---|---|
| `vendorInvoiceIntake` | POST | admin+office | the bot's; idempotent on (vendor, invoice no) |
| `vendorInvoices` | GET | admin+office | the queue; **fails closed (503)**, never an empty list |
| `linkExistingVendorInvoice` | POST | admin+office | **backfill only** — records an invoice whose expense already exists. Creates NO expense. |
| `vendorInvoiceAssign` | POST | admin+office | creates the expense, marks matched |
| `vendorInvoiceMarkReviewed` | POST | admin+office | not going on any job; keeps the row, records why |

`vendorInvoiceIntake` sits at `_ADMIN_OFFICE` rather than the `_NON_VIEWER` default a
write would otherwise get, because **it creates expenses with no human in the loop** —
an employee POSTing a fabricated invoice would be posting money onto a job. The tier
is also simply the bot's own role.

---

## 4. ⬜ WHAT IS LEFT: repoint the bot

Nothing writes to `vendor_invoices` yet. The bot still calls `addGeneralExpense`
directly, which keeps working exactly as it does today — this is additive, not a
cutover, so there is no moment where invoices stop flowing.

**The change:** for each invoice, instead of matching the PO itself and calling
`addGeneralExpense`, call:

```http
POST /.netlify/functions/airtable
Authorization: Bearer <the cedautomation session token, as today>
Content-Type: application/json

{
  "action":      "vendorInvoiceIntake",
  "vendor":      "CED",                  // one of the four above — anything else is a 400
  "invoiceNo":   "0171-1063885",         // required; a STRING, leading zeros and all
  "po":          "CAJ 436",              // as read off the paper; null/omitted is fine
  "invoiceDate": "2026-08-28",           // YYYY-MM-DD
  "amount":      "1063.08",              // string or number; "773.85-" is a credit
  "taxAmount":   "0.00",
  "isCredit":    false,                  // optional; ONLY from the document, never inferred
  "pdfBase64":   "JVBERi0xLjQK…"         // optional, ≤ 8 MB decoded
}
```

Replies, all HTTP 200 unless noted:

| Response | Meaning |
|---|---|
| `{ok, id, status:"matched", jobId, expenseId}` | PO matched one job; expense created |
| `{ok, id, status:"needs_review", reason, candidates[]}` | parked for a person |
| `{ok, duplicate:true, id, status, jobId, expenseId}` | already had this invoice; nothing written |
| `400 {ok:false, error}` | unknown vendor, or no `invoiceNo` |
| `502 {ok:false, id, status:"needs_review", error}` | recorded, but the lookup or the expense create failed — the row is on the review screen |

**Safe to retry anything.** Idempotency is on the normalised (vendor, invoice number)
pair, so `0171-1055250` and `0171 1055250` are the same invoice. The bot polls a
folder on a daily timer, so re-sending yesterday's invoice is the *normal* case — the
same reasoning as the inventory push's `push_id` guard, and without it a retry charges
the job twice and nothing complains.

### After it's repointed

- ⚠⚠ **Re-query production after the first real run.** Deploying is not evidence —
  this app's defects don't throw, they match nothing. Check that
  `SELECT status, count(*) FROM vendor_invoices GROUP BY status` shows `matched` rows
  climbing, not everything landing in `needs_review`. A queue that is suddenly *all*
  misses means the PO expression stopped matching, not that the crew stopped using POs.
- The 215 expenses the bot already wrote are untouched and stay as they are. There is
  no backfill: the invoices behind them are already costed, and re-ingesting them
  would double every one.
- The old direct `addGeneralExpense` path stays open. It is the field app's own
  expense form; nothing about this feature narrows it.

---

## 5. Not done, deliberately

- **No PDF for anything already filed.** R2 only ever holds what the bot sends from
  the repoint onwards. The historical PDFs are in pCloud and stay there.
- **No line-item import.** The invoice becomes one Materials expense at its total,
  which is what the bot already does and what GP reads. PDF.co returns line items;
  nothing here consumes them.
- **No email/alert on a parked invoice.** The badge on the top bar is the whole
  notification, matching 🌴 Time Off. Revisit if invoices sit for days.
- **No delete.** A reviewed invoice keeps its row and its note — "why is there no
  expense for this invoice" gets asked months later.
