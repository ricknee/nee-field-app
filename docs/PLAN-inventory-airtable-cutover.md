# Plan — retiring the Airtable inventory base as a write target

**Status: 2026-08-12 — five of six slices done. ~3-4 hours left.**
Companion to `docs/PLAN-inventory-to-neon.md`, which moved the **reads**. This moves the **writes**.

> **Six Airtable writes remain in the whole file**, and two of those were never in scope (they go
> to the MAIN base's `Expenses`, which belongs to `docs/AUDIT-airtable-remaining.md`). The other
> four are on `Inventory Items` and are slice 5.
> ✅ **Owner confirmed 2026-08-11: nobody uses Airtable for anything — the app is the only way this
> data is seen.** So this is a full retirement: no reverse mirror, and the base gets archived.
> ✅ **The "Adjustment subtracts instead of setting" defect is FIXED** (`da93e4e`) — see §Slice 1.
> The counting day is unblocked, and counting is the only thing that repairs the historical figures.

## ⚠⚠ The ordering changed mid-flight, and the new one is the right one

This plan opened ordered **most important first** — by write count and business impact. That was
wrong, and it broke when slice 3 (estimates) turned out to be unshippable on its own:
`handleOrderCreate` wrote the estimate into an Airtable **linked-record field**, and
`saveEstimateAsTemplate` fetched the estimate from Airtable **by rec id**. Flip estimates alone and
orders and templates point at records that do not exist.

**The correct ordering is LEAF-FIRST: an entity moves only after everything that references it
already speaks the new id, or moves with it in the same slice.** Write count is a bad proxy for
size; the real questions are *does Neon already know this entity* and *who points at it*.

That reshuffled the back half: stock settings (nothing references it) went early, the six
estimating tables became **one slice**, and items — the most-referenced entity in the base — went
last instead of sixth.

---

## What this actually buys

Reads are Neon-first today, but **every write still goes to Airtable first and is then mirrored to
Neon**. The migration made Airtable unnecessary for *answering* questions, not for *recording*
them. Ending that gets four things:

1. **It deletes an entire class of bug.** Every "flipped a read without its writes" failure — hit
   four times now, most recently the Step D 404 — exists only because there are two stores. One
   store, no mirror, no drift.
2. **A cart becomes one transaction.** `handleSubmitCart` writes **one Airtable record per line in
   a loop**. In Postgres that is a single multi-row insert, so a half-submitted cart stops being
   possible.
3. **Writes get roughly twice as fast** and stop being shaped by Airtable's rate limit — the
   batches-of-10 loops throughout the file exist only to satisfy it.
4. **The autonumber double-sync goes away** (the thing that shows an order as **#0** if it fails).

---

## The slices

Started at **36 writes across 12 tables in one file** (`netlify/functions/inventory.js`).
**Six left.**

| # | Slice | Writes | Status |
|---|---|---|---|
| 0 | **Formula/rollup sweep** + delete the dead form | — | ✅ **DONE 2026-08-11.** Came back clean (§3): no formula anywhere needs reproducing. Interface `pbdc6kzAEV7yzXHfD` deleted; base has **zero interfaces and zero forms** (revert handle `actXs5D93HDtPDCk8`). |
| 1 | **Ledger** — Inventory Transactions | 6 | ✅ **DONE + PROD-SMOKED 2026-08-11** (`322116e`, fix `40bc757`). Cart, receive, transfer, mark and push all confirmed live. §Slice 1. |
| 2 | **Expense pushes** — history header + lines | 2 | ✅ **DONE + PROD-VERIFIED 2026-08-12** (`d1c9d50`, `db/schema/038`). 34 headers, 415 lines, 0 orphaned, all reconciling to the cent. §Slice 2. |
| 3 | **Stock settings** — reorder points | 2 | ✅ **DONE 2026-08-12** (`8b26747`, `db/schema/039`). Smallest domain, nothing references it — first under leaf-first. §Slice 3. |
| 4 | **The estimating cluster** — estimates, lines, templates, template lines, orders, order lines | 15 | ✅ **DONE + PROD-SMOKED 2026-08-12** (`2932b03`, `db/schema/040`). Six tables, one slice. Order **#41** created live off a native estimate. §Slice 4. |
| **5** | **Reference tables** — Inventory Items | **4** | ⬜ **~2-3 h.** LAST because transactions, estimate lines, template lines, order lines and vendor pricing all reference items. Also drops the remaining read fallbacks on Items, Locations, Vendors and Vendor Pricing. |
| **6** | **Decommission** — retire the loader, archive the base | — | ⬜ **~1 h.** After slice 5 the loader has nothing left to load. |

**The two writes that will still remain afterwards go to the MAIN base** (`inventory.js` Expenses,
the expense push feeding GP). They were never in scope — see `docs/AUDIT-airtable-remaining.md`.

> ⚠ **Open question for slice 5, and it is not a cutover question.** Locations, Vendors and Vendor
> Pricing have **no write paths at all** in this app — they are read-only here. With nobody opening
> Airtable any more, that means they are currently uneditable *anywhere*. Whether the inventory app
> needs "add a vendor" / "add a location" is new work, not part of retiring the base.

## ⚠⚠ THE RULE THAT COST THE MOST: a domain LEAVES the loader on the day it goes native

Learned twice, the second time as a live defect. `loadInventoryReference` kept upserting tables Neon
already owned. Airtable is a frozen snapshot after a cutover, so re-running the loader does not
repair anything — it **overwrites**, and worse, **a deleted row has no conflict to hit, so the
upsert INSERTS it straight back.** Delete a transaction in the app, run the loader, and the stock
movement returns from the dead.

Slice 1 saw only half of it and excluded `expense_created`/`push_id` from the UPDATE list — which
stopped pushed material being re-offered but did nothing about resurrection. Fixed properly in
`7fd7f20`; every slice since removes its tables from the loader in the same commit, and the loader
response now lists what it deliberately does not touch.

---

## Slice 1 — what it cost, and what it taught

**Prod-smoked 2026-08-11.** Transactions are born in Postgres; Airtable stayed at 863 rows while
Neon moved on. Confirmed live: a cart (one insert, `submit_id` stamped), receive (+3,000 landed on
the to-leg), transfer (both legs moved: Shop #1 576→476, Box Trailer 55→155), the pushed-mark, and
a full expense push. The two natively-written transactions were charged, marked `expense_created`,
stamped with the push id, and left the pending list — 62 → 60, with **0 native rows still pending**.

### ⚠⚠ The id currency has THREE readers, not two

Changing the ledger's handle from the Airtable rec id to the uuid broke the push in production,
because a **third** read still spoke the old currency: the chargeable-set guard
(`SELECT airtable_id …` feeding `stillPending`). Every id in the request failed to match, every job
was classed a stale snapshot, and **no job could be pushed at all**.

The lesson is the same one Step D taught, one layer down: it is not enough to change the paths you
are editing. **Grep for every query that touches the entity's id**, not just the ones in the
handlers you have open. The three were: the pending read, the mark, and the chargeable-set guard.

That it was an outage rather than a double charge is the guard failing safe, and is the only reason
this was cheap.

### The tests passed while it shipped, and why

The push suite modelled **one id space** — the uuid and the rec id were the same string, so asking
for the wrong column still matched. It now answers with whichever column the SQL actually asked
for, with the rec id a genuinely different value. Reverting the fix fails **six of seven** cases;
before, it failed none. Any future slice that changes an id currency should do the same: **give the
mock two id spaces, or it cannot catch the only bug that matters.**

### A contract that got stricter on purpose

A push that cannot verify what is chargeable now **refuses before creating anything** (503). Step E
let a Neon outage through — create the expense, fail at the mirror, heal on retry — because
Airtable could still answer what was pending. It cannot any more: its copy is missing every native
row and still reports unpushed for material already charged. Guard #2 is exactly what stops a stale
client re-charging under a fresh push id, so with no way to run it, the push does not start. The
heal path is kept for the narrower failure it was really about: the expense reaching Airtable while
its Neon mirror does not.

### ✅ FIXED 2026-08-11 (`da93e4e`) — "Adjustment" subtracted instead of setting

Found during this smoke, **pre-existing — not caused by slice 1**, and dating from Step C rather
than the cutover. The UI asks *"Set 1/2" EMT PIPE at Shop #1 to 2000 units?"* and toasts *"Stock
adjusted to 2000"*, but the handler writes the quantity on the **from** leg, which `v_stock_on_hand`
subtracts. So counting 400 pipes and typing 400 **removes** 400.

**30 adjustment rows exist, every one on the subtracting leg, 10,219 units removed** — a large part
of why on-hand reads so negative (26,332 used against 15,039 received). The old Airtable automation
almost certainly treated an Adjustment as *set the cache to this value*; deriving on-hand from the
raw ledger silently turned it into *subtract*.

**The fix makes "set" real:** read current on-hand, post the DIFFERENCE — to-leg if positive,
from-leg if negative with the sign dropped. The ledger stays a pure record of movements.

**It self-heals, so the 30 bad rows need no repair.** The delta is measured from current on-hand, so
the first correct count lands an item on the right number however wrong its history was. That is why
the counting day is now the cure rather than the thing that would have made every figure worse.

Three smaller decisions worth keeping: a count that **matches** writes nothing (a zero-quantity
movement is noise in the history of an item somebody checked and found correct); an unknown
item/location pair is a **404** rather than a phantom movement, while a pair that has simply never
held stock still returns **0** — the `CROSS JOIN` keeps those two cases distinguishable; and it
**fails closed**, because with no current figure there is no delta and guessing would write a
movement nobody counted.

The toast now reports the delta — *"Set to 400 (was -2424, +2824)"*. On a counting day that number
**is** the finding, and it is also how a count entered against the wrong location shows itself.

⬜ **The counting day is now unblocked.** Nothing repairs the historical figures except counting:
each item lands on truth the first time it is counted.

### Smaller notes

- The loader's `transactions: 4330` count does not match the 863-row table it wrote. Data is
  correct (`upsertChunked` dedupes on `airtable_id`, and Airtable 863 = Neon 863), but the reported
  figure is wrong and the loader counts have been trusted for reconciliation. Worth a look.
- The **4,330** figure quoted in ROADMAP §4 and earlier in this file as a ledger row count is not
  one — the code comment is explicit that it is the number of rollup-value comparisons Step C
  reconciled. The ledger is **863 rows**.
- Sales tax is deliberately its own expense record rather than folded into the materials line, so
  the materials line stays a clean sum of what left stock. Both carry the same push id.
- Owner wants the push to **attach its PDF automatically**. Not built: it needs the function to
  hand the PDF to the Make webhook the way invoices and estimates already do, because pCloud's app
  registration is dead and only Make holds a working token. A real slice, not a toggle.

## Slice 2 — the push history

**Prod-verified 2026-08-12.** `db/schema/038`: `expense_pushes` + `expense_push_lines`, the audit
trail behind every dollar the inventory app has charged to a job. 34 headers and 415 line snapshots
carried across, **0 orphaned**, and all 34 pushes reconcile to the cent — $52,252.25 of material
charged between 2026-04-22 and 2026-08-11.

**It was scoped as "two writes, an hour" and was a whole domain.** Neon had no push tables at all,
so it needed schema, a loader pass, an FK resolve, **both** reads flipped and the write.
**Counting writes is a bad way to size a slice** — the question is whether Neon already knows the
entity.

- The **id currency** lesson applied up front for the first time: `pushHistory` hands ids to the
  client which hands them back to `pushHistoryDetail`, so both moved to the uuid. Left alone, the
  detail view would have 404'd on any push created after the cutover — the exact failure the ledger
  shipped the night before.
- The header write is now **idempotent on `push_id`**. A retried push used to mint a *second*
  history row for a charge that happened once.
- ⚠ **Deliberate deviation: `recordPushHistory` stays best-effort rather than failing closed.** It
  runs *after* the money has moved — expenses created, transactions marked — so throwing there would
  show an error over a push that actually succeeded and send the user to retry a charge guard #1
  would then have to catch. A lost log line is the cheaper failure. Recorded so it is not mistaken
  for an oversight.

## Slice 3 — reorder points

**Done 2026-08-12.** `db/schema/039`. Smallest domain in the base and the only one nothing
references, which is why it went first once the ordering became leaf-first.

**The write got simpler by moving.** The Airtable version created a Stock Levels record carrying a
`Quantity On Hand` of 0 and an "Item | Location" display string, then trusted an automation to
maintain that cache without clobbering the reorder point. None of it exists now: on-hand is derived
from the ledger, so the only thing worth storing is the number a human chose. Create became an
**upsert on the (item_id, location_id) partial unique** — that pair *is* the identity of a setting,
which also makes the UI's create-vs-update split harmless.

- ⚠ **The view had to move with the id currency.** `v_stock_levels` exposed only
  `ss.airtable_id`, so a setting created after the cutover would have had no handle and the screen
  would have offered "create" forever. It now also exposes `ss.id`, appended at the end of the
  select list because that is what `CREATE OR REPLACE` allows — and **rebuilt from
  `pg_get_viewdef()`, not from 032's copy**, because the `.sql` files here drift.
- Both writes distinguish *saved* from *matched nothing*: an update hitting no row 404s, because a
  reorder point that silently did not save means an item stops warning it is running out.

## Slice 4 — the estimating cluster

**Prod-smoked 2026-08-12.** `db/schema/040`. **Six tables in one slice**, for the reason at the top
of this file: orders and templates referenced estimates by Airtable rec id, so the currency had to
change across all of them at once. For a few commits in between, the file was genuinely
half-broken — which is why it is one commit and not three.

**Things that got smaller by moving:**

- **The `#0` order bug is gone by construction.** `Order ID` was an autonumber absent from the
  create response, so an order had to be written, re-fetched and re-synced just to learn its own
  number — showing as **#0** if that second trip failed. A sequence hands it over in the same
  INSERT. Live proof: the smoke created **#41**.
- Replacing an estimate's lines is **one DELETE by FK**, where before it asked Airtable which lines
  existed and removed them ten at a time.
- `refreshTemplatePrices` is one `UPDATE…FROM`; `createEstimateFromTemplate` is one
  `INSERT…SELECT` that clones quantities and prices items **live** — the split that is the whole
  point of a template.
- Deletes cascade instead of being walked by hand. 12 mirror helpers and 38 field-id constants
  deleted.

> ⚠⚠ **The sequence starts at 40, NOT `max()+1`.** Surviving order numbers are 13, 17, 23-25 and
> 27-31 — every gap is an order someone saw and later deleted, and **#32 was minted and deleted
> during the Step D smoke**. Airtable autonumbers never reclaim; `max()` does. Seeding from
> `max()` would have reissued a number already printed on someone's order.

**Line numbers deliberately did NOT become a sequence.** `Line ID` and `Line Item ID` were global
autonumbers, but nothing outside the row ever read them — they only order lines within one parent.
Native lines number **1..N per estimate or order**, so the column finally means what its name says,
and there is no global counter to seed or collide.

## What still is not smoked on production

- A real **Adjustment** — the fix that unblocks the counting day (`da93e4e`).
- **Deleting a line from history** — the lowest-stakes of the ledger's six paths.

## Out of scope

**The 3 writes to the MAIN base** (`inventory.js:1780, 1881, 1922`) — the expense push creating
`Expenses` rows. Different base, feeds **GP and Make**, already mirrored to Neon by Step E. That
belongs to `docs/AUDIT-airtable-remaining.md`. Retiring the inventory base does not touch it.

---

## Why this is cheaper than the main-base exit — the evidence

All verified, not assumed:

1. **One file writes to this base.** No `airtable.js` reference, no `db/etl/` script, no second
   consumer.
2. **Make does not touch this base at all** — zero references across all 22 Airtable/HTTP scenarios
   (re-verified 2026-08-10). ⚠⚠ In the main base, **mirror writes are the Make trigger bus**, so
   every hook must be replumbed before a mirror write can be dropped. Here there is no bus.
3. **One Interface exists and it is dead.** A "Use Material" form on `Inventory Transactions` that
   the API refuses (`422: one of the form's required columns does not exist`) — the `Job` link it
   required was deleted in Step C3. It cannot submit. Delete the shell.
4. **Neon already holds the data, reconciled.** Every table has a `uuid` PK with
   `gen_random_uuid()`; `airtable_id` is only a UNIQUE side-key. Both sides agree: estimates 16=16,
   templates 3=3, template lines 149=149, orders 10=10, ledger 4,330=4,330, on-hand 264/264.

**Precedent:** billing allocations already made this exact move (`db/schema/033`). Same shape, and
its scars are reusable.

---

## The three technical blockers

### 1. Record identity — Airtable mints the key

Every Neon inventory row hangs off `airtable_id` because Airtable creates the record and returns
the `rec…` id. Native rows are born in Postgres with their uuid and **`airtable_id` stays NULL**.

The uuid PKs already exist, so this is mechanically small — but it changes every FK resolution in
the file. The sync helpers currently resolve parents with
`(SELECT id FROM material_estimates WHERE airtable_id = $n)`; native rows have no `airtable_id`, so
those take the uuid directly.

> ⚠⚠ **The guard allocations learned the hard way.** Anything that reconciles Airtable → Neon must
> skip native rows or it deletes them within the hour. `_billing-sync.js` needed **both** the
> empty-array guard **and** `airtable_id IS NOT NULL`. `loadInventoryReference` upserts and never
> deletes, so it is safe as written — it must stay that way.

### 2. Two autonumbers

`Order ID` → `material_orders.order_number`, and `Line ID` → `line_number` on estimate and order
lines. Postgres sequences replace them.

> ⚠⚠ **Do NOT seed a sequence from `max()` of surviving rows.** Airtable autonumbers never reclaim
> a deleted number; `max()` does. Proof from today's smoke: order **#32** was created then deleted,
> so `max(order_number)` reads **31** while Airtable's next value is **33**. Seeding at `max+1`
> re-mints **32** and collides with a number a human has already seen on a printed order. **Seed
> from Airtable's next autonumber, plus headroom.**

Current: `order_number` max **31** (Airtable next: 33) · estimate `line_number` max **4,238** ·
order `line_number` max **664**.

### 3. Formulas read back after a write — ✅ **SWEPT 2026-08-11, and it came back clean**

The rule from Steps 4d/4e: **audit the Airtable formula and rollup columns BEFORE flipping a
write**, because a Neon-native row has nobody to compute them. All 12 write-target tables were
audited for every computed type (formula, rollup, lookup, count, autoNumber, createdTime, aiText).

**Result: there is no formula anywhere that has to be reproduced.** The entire computed surface
that matters is three autonumbers and two created-times.

**A — must be handled (and all were already in this plan):**

| Field | Table | Replacement |
|---|---|---|
| `Order ID` (autoNumber) | Material Orders | sequence — mind the seeding trap in §2 |
| `Line ID` (autoNumber) | Estimate Line Items | sequence |
| `Line Item ID` (autoNumber) | Material Order Lines | sequence |
| `Date Created` (createdTime) | Estimates, Material Orders | `created_at DEFAULT now()` |

**B — read ONLY inside Airtable fallback branches, so they vanish with the fallback:**
`Wire (Ft.)`, `Unit Cost (from Item)`, `Total Value`, `Quantity On Hand` (Stock Levels);
`Date Created`, `Line Total`, `Total Items` (Estimates / Order reads). The Neon path already
derives every one of these from a view.

**C — never read by any code at all.** The surprise, and it is a big one:

- **All 20 per-location rollups and formulas on `Inventory Items`** — `Qty In (Shop #1)`,
  `Qty Out (Shop #2)`, `On Hand - #4 Transit`, `Qty In (Global)` and the rest: **zero reads.**
  This is the field-name-encodes-the-location design that Step C replaced with one view and one
  row. It is already dead weight, it just has not been deleted.
- `Price Variance ($)`, `Price Variance (%`, `Suggested Default Unit Cost`, `Attachment Summary`
  (Inventory Items) · `Pricing ID` (Vendor Pricing, a formula **primary field**) · `Calculation`
  and `Default Unit Cost (from Inventory Item)` (Transactions) · `Current Price`,
  `$ Current Line Total`, `Barcode`, `Category` (Template Lines) · `Job Name (from Material Order)`,
  `Category` (Order Lines).

> One that looked dangerous and is not: the `Name` **formula** is the primary field on
> `Inventory Transactions` and *is* mirrored into `inventory_transactions.txn_name` — but **no
> handler ever reads `txn_name` back.** It is a stored label with no consumer. Native rows can
> leave it NULL or compose an equivalent string in the app; nothing depends on it either way.

**Consequence for the estimates: no slice is bigger than scoped, and several are smaller.** There
was no hidden formula to port. `Total at Save` and `Line Total at Save` are stored currency fields
the app already computes, and `recomputeTemplateTotal` sums them itself rather than reading a
rollup.

---

## Traps carried forward

- ⚠⚠ **The same-commit rule is per-ROW, not per-HANDLER.** Step D shipped a live 404 because
  `createEstimateFromTemplate` was a *second* handler creating an estimate. **Enumerate write paths
  by grepping the POST to the table**, not by listing the handlers you have open.
- ⚠⚠ **Fail-soft hides everything.** A mirror that fails soft is correct while Airtable is the
  authority and catastrophic once it is not. **Every native write fails closed.** This is also why
  the total is larger than the read migration's: a bad read flip degraded to *slow*, because
  Airtable was still there to answer. A failed native write means the record does not exist.
- ⚠ **`submitCart` has no idempotency key** — a native retry could double-log material. Slice 1
  adds one.
- ⚠ **Push ID idempotency is a UNIQUE constraint** — keep it; it is what makes a retry free.
- ⚠ **Deletes are repaired by nothing** — the loader upserts and never removes.
- ⚠ **PREPARE every new parameterised statement against real Postgres.** Offline tests cannot reach
  the Neon path; they only prove the Airtable fallback.
- ⚠ **Booleans cross the Neon wire as `"t"`/`"f"`**, and an array bind arrives as the literal
  `{a,b}`, not a JS array. Both have already cost a debugging session.
- ⚠ Nothing local can read this base (both PATs 403), so every reconciliation runs through a
  deployed admin action and the browser console.

---

## ✅ The decision — CLOSED 2026-08-11

**Owner: "we dont use airtable at all for anything. just the app."**

So this is a **full retirement**. No reverse mirror, no Neon→Airtable copy, and the base is
archived at slice 8. That also retires the outstanding "re-group the Job Usage view" request — the
view is not used, so there is nothing to re-group and nothing to rebuild in the app.

Two consequences worth keeping in view:

- **Every slice can delete as it goes.** With no human reader, the ~30 never-read computed fields
  in §3 group C do not need preserving through the cutover — they can go with their table.
- **The loader becomes the last thing standing.** `loadInventoryReference` is the only remaining
  reason the base has to stay coherent. Slice 8 stops it, and only then is the base inert.
