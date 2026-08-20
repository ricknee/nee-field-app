# Plan — retiring the Airtable inventory base as a write target

**Status: 2026-08-12 — 36 writes → ZERO. All six slices done. Only the Airtable-side
decommission is left (~1 h, owner actions, no code).**
Companion to `docs/PLAN-inventory-to-neon.md`, which moved the **reads**. This moves the **writes**.

> **The inventory base is written by nothing, and read by nothing.** Slice 6 deleted the loader
> (`79b1b56`), which was the last reader. Re-measured from code 2026-08-19
> (`docs/AUDIT-inventory-app.md`): `inventory.js` makes **zero reads and zero writes** to the
> inventory base and no longer names `INVENTORY_BASE_ID`. The two writes still in the file go to
> the **MAIN** base's `Expenses` — the expense push feeding GP — and were never in scope.
>
> ⬜ **What is left is in Airtable, not in the repo:** undeploy the one still-deployed automation
> (`wflTe6pr2oCtig6qp` "Stock Levels" — a `recordCreated` trigger on a table nothing writes),
> archive the base, and delete `INVENTORY_BASE_ID` from the Netlify dashboard and `.env.example`.
> The conduit-assembly prototype (1 assembly, 1 labor unit) is exported to
> `nee-backups/conduit-assembly-prototype_appfsLJwfow4CepCw_2026-08-20.md` — it is a design sketch
> for the future native cost engine, not data to migrate.
>
> ⚠⚠ **The slices were not the end of it.** Items (slice 5) and locations/vendors/pricing made
> both entities natively creatable, but **three READERS were still keyed on the Airtable rec id
> alone** — found by the 2026-08-19 audit, fixed 2026-08-20 in `f9c908e` / `db/schema/043`. The
> worst silently lost every stock movement into an app-created location. See §Slice 5 addendum.
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
| 5 | **Items** — the last domain | 4 | ✅ **DONE + PROD-SMOKED 2026-08-12** (`39e6e86`, `db/schema/041`, fixes `cf43966` / `e7a8e8c` / `2449a0c`). **Zero Airtable writes left to the inventory base.** ⚠ Items keep a **dual handle** — see §Slice 5. ⚠⚠ And the readers did not all learn it — see the addendum below. |
| 5b | **Reference data becomes writable** — locations, vendors, vendor pricing | — | ✅ **DONE 2026-08-12** (`c0087ba` backend, `621fbe0` UI, `db/schema/042`). Closed the open question at the top of this plan: those three tables had **no write path at all**, so with nobody opening Airtable they were uneditable anywhere. ⬜ Still has **no test coverage**. |
| **6** | **Decommission** — retire the loader, archive the base | — | ⚠ **Code half DONE 2026-08-12** (`79b1b56`) — the loader is deleted, so nothing reads the base. ⬜ **Airtable half outstanding (~1 h, owner):** undeploy `wflTe6pr2oCtig6qp`, archive the base, drop `INVENTORY_BASE_ID`. |

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

## Slice 5 — items, and the one deliberate inconsistency in the whole cutover

**Done 2026-08-12.** `db/schema/041`. The last four writes. **The inventory base is now written by
nothing** — the only writes `inventory.js` still makes go to the MAIN base's `Expenses` (the expense
push feeding GP), which was never in scope.

### ⚠⚠ ITEMS KEEP A DUAL HANDLE. Do not "tidy" this away without reading why.

Slices 1-4 each moved their public id wholesale to the uuid. **Items deliberately did not.**

The item id is the app's widest currency: roughly **40 backend sites and 100 in the frontend**, and
it flows *item picker → cart → transaction → expense push*. Rewriting all of that in one slice would
have been the largest and riskiest change of the entire cutover, for no functional gain.

So an item's public handle is **its Airtable rec id if it has one, else its uuid**:

- reads emit `COALESCE(airtable_id, id::text)`
- the five child writes resolve with `airtable_id = $n OR id::text = $n`
  (ledger, estimate lines, order lines, template lines, stock settings)

Symmetric, opaque to everything downstream, and the **866 historical items keep the ids already
sitting in transactions, estimate lines, template lines, order lines and vendor pricing**. The two
forms cannot collide: a rec id always starts `rec` and is never a valid uuid.

**This is a transition shape, not a destination.** Once the base is archived, normalising to the
uuid is one data migration plus one read change — cleanup, not cutover, and not urgent.

### Two things it surfaced

- `syncItemCostToVendor` was still reading **`Unit Cost Rollup (Live)` from Airtable** — the rollup
  `v_item_live_cost` replaced back in Step B. That handler had gone on ignoring the view ever since.
- Making `itemIndex()` throw (there is no second item table to fall back to) exposed a real
  regression in `handlePendingExpenses`: a rejected `Promise.all` skipped its 503 and returned a
  bare **500**, losing the message that says nothing was charged. The chargeable rows are now
  fetched and checked *before* the indexes.

### ✅ The hole it left, and closing it — `2449a0c`

Found by trying to delete a test part: **the app could only ever CREATE items.** Editing, retiring
and deleting all happened in Airtable, and the cutover removed that without replacing it.

`itemUpdate` + `itemDelete` (admin-only) close it, and the Edit button lives on the stock lookup —
where you already are when you notice a cost is wrong. The UI reuses the new-item form rather than
adding a second one.

> ⚠ **Delete REFUSES when anything references the item, and that refusal is the feature.** An item
> on an old estimate or a pushed transaction cannot be removed without blanking the line it appears
> on — history would silently change. The error counts what blocks it (*"4 stock movement(s), 2
> estimate line(s)"*) and names the alternative: **untick Active**, which hides it from the pickers
> and leaves every record intact. Delete then only exists for what it is genuinely safe for —
> something created by mistake that nothing has ever used.
> Stock settings are deliberately **not** in that guard: a reorder point is a setting on the item,
> not a record of something that happened, and the FK cascades.

Prod-smoked 2026-08-12: cost edited, TEST PART deleted, an in-use item correctly refused, Active
toggled. Items, transactions and settings all returned to their pre-test counts.

One asymmetry worth knowing: **create sends only the fields you filled in; edit sends every field,
including the blank ones.** On create an absent key means "not given"; on edit it would mean "keep
the old value", so clearing a barcode has to be explicit.

### ✅ CLOSED — Locations, Vendors and Vendor Pricing are writable too (`c0087ba`, `621fbe0`)

The rest of the same hole. All three were read-only here because you maintained them in Airtable;
the cutover removed that without replacing it, and the vendor-pricing panel still ended with *"Add
a Vendor Pricing record in Airtable"* — an instruction that had become impossible to follow.

Five actions, admin-only: `locationSave`, `vendorSave`, `vendorPricingSave`, `vendorPricingDelete`,
and a **vendors list that never existed** — vendors were only ever reachable through an item's
pricing rows. Pricing is edited inside the item's panel on the stock lookup; locations get their own
screen; a vendor can be added from inside the pricing form so a missing supplier does not derail the
task.

> ⚠⚠ **Two constraints Airtable never enforced** (`db/schema/042`):
> **One preferred vendor per item.** `v_item_live_cost` filters `preferred AND active` and wraps the
> result in `MIN()` — and that `MIN` exists *because* Airtable allowed two preferred rows. It broke
> the tie silently, by price, which is not a decision anyone made. A partial unique index makes the
> state impossible, and the save clears the flag from the item's other rows in the same request.
> **One price per item per vendor**, via `ON CONFLICT`. Two rows for one pair is what makes "which
> price is current?" unanswerable.

Also: `last_price_update` moves only when the cost actually changes; locations and vendors both
refuse duplicate **names**, because both are picked and read by name and two "Shop #2"s cannot be
told apart in a dropdown; and locations can be **retired but never deleted** — a location appears on
every movement ever logged against it.

### ⚠⚠ ADDENDUM 2026-08-20 — the dual handle needs READERS taught too (`f9c908e`, `db/schema/043`)

Slice 5 got the dual handle right everywhere it looked, and the sweep it ran was the right sweep.
It just did not run wide enough, twice over:

1. **It was never re-run for `locations`.** Those became natively creatable eight commits later in
   `c0087ba` (slice 5b above). Nobody went back.
2. **It only swept handlers.** Three of the misses were not in a handler's own SQL — they were in
   `v_stock_levels`, and the handlers merely selected the wrong column out of it.

The writers were all correct. `handleLocationSave` and `handleItemUpdate` resolve on
`airtable_id = $1 OR id::text = $1`, and `handleItems` / `handleLocations` serve
`COALESCE(airtable_id, id::text)`. **The readers were the gap** — so a native row saved perfectly,
appeared in the picker, and evaporated on the next read.

| | Site | What it did |
|---|---|---|
| F-01 | `insertTxns` — the two **location** subselects | Resolved to NULL for an app-created location. The row still inserted, but `v_stock_on_hand` skips legs with a NULL location and `v_stock_levels` INNER JOINs `locations` — so the movement was **logged, chargeable, in History, and absent from every stock figure, silently.** Shared by cart / receive / transfer / adjustment. |
| F-02 | `handleStockLevels`, `handleStockLevelsAll`, `handleReorderAlerts` | Filtered on / returned `item_airtable_id`. A native item matched nothing, the query **succeeded with zero rows**, and Check Stock rendered a clean working-looking screen saying the item was nowhere. |
| F-03 | `handleCreateStockLevel` | Same miss, but guarded → 404. The INSERT had already run, and a NULL `location_id` sits **outside** the partial unique index, so the row was saved, invisible, and reported as failed. A retry appended another. |

**Fixed in the view, not at the call sites.** `db/schema/043` appends `item_handle` and
`location_handle` to `v_stock_levels`, so any future reader is correct by construction and the id
currency is decided in exactly one place. F-03's insert became an `INSERT…SELECT`, so an
unresolvable handle has nothing to insert rather than saving an orphan.

> **The rule, stated so it generalises:** an id arriving at a handler came out of a picker, and the
> pickers serve `COALESCE(airtable_id, id::text)`. **Match what the picker serves, on every
> entity.** A `WHERE` that names only `airtable_id` does not error on a native row — it resolves to
> NULL or returns zero rows, and both of those look like a working screen.

> **And the sweep is not done when the handlers are done.** Grep the **views** too. Three of these
> four sites were invisible to a handler-level sweep because the wrong column was inside
> `pg_get_viewdef`, not inside the file being edited.

**Tests:** 7 new cases, 6 of which fail on the code as it shipped. The mock now models **two id
spaces** — a genuinely different uuid and rec id, plus a fully native item/location pair — which is
the slice-1 lesson applied one layer up. The F-01 case asserts the rule over *every* FK subselect
in the ledger insert rather than the two that were broken, so the next entity to go native is
covered before it exists. It also caught an existing assertion that compared a value against itself
and had stayed green throughout.

### ⚠ Not in scope, now or later: the per-ft cost engine

The three conduit-assembly tables (Labor Units, Conduit Assemblies, Assembly Components) hold **one
labour code, one assembly and four components** — a prototype nothing reads. **Owner, 2026-08-12:
this is a NEW BUILD in Neon after the migration is completely finished, not migration work.** Do not
port the tables, and do not let them hold up archiving the base. The thinking is recorded in memory
(`project_conduit_assemblies_estimating`); build from that, not from the rows.

## ✅ Everything is smoked on production (2026-08-12)

All six slices, plus the two fixes and the item-edit screen. The adjustment was the last one: TEST
PART 0 → 15 posted a **+15 delta on the adding leg** and landed on exactly 15, which is the bug that
made a counting day dangerous. **The counting day is unblocked.**

Three bugs were found by smoking rather than by reading, all the same shape — *it saves fine, then
something cannot find it again*:

1. A **new item vanished on refresh.** Three reads carried `WHERE COALESCE(airtable_id,'') <> ''`,
   a Step-B guard meaning "skip malformed rows" that came to mean "skip everything created since
   the cutover". The list was the visible half; `neonItemIndex` was the worse one, so the item would
   not have priced in a cart either.
2. **Adjusting a native item 404'd.** Its on-hand lookup still read `i.airtable_id = $1`. The same
   grep turned up **seven latent ones** in the loader's FK-resolve passes, which would have set
   `item_id` to NULL on native rows — invisible to `v_stock_on_hand`, so the next loader run would
   have quietly removed native material from stock.
3. **No way to edit or retire an item at all** — see §Slice 5.

⚠ The lesson, having now paid for it five times: **when a domain goes native, grep for
`airtable_id` in every WHERE, not just in the handlers you are editing.** And match on the clause,
not the table name — the adjustment's was missed because its `WHERE` sat on a different line.

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
