# Plan — migrating the inventory app to Neon

**Status:** planned 2026-08-07, nothing built. This is the detail behind `docs/ROADMAP.md` §4,
which until now was five bullet lines.

**Letters match ROADMAP §4 exactly** (A = drop the Jobs mirror, B = reference data, C = the
ledger, D = estimating, E = the expense push). Do not re-letter — the field app's Step 4
re-lettering on 2026-08-05 is still causing confusion in older notes.

---

## 1. The gate — do not start before this

Two conditions, from ROADMAP §6 and §4:

1. **Field-app Step 3 is done** (Make off the time path). Until then, starting here means two
   half-migrated systems at once — the exact thing §6 exists to prevent.
2. **Step A (C3) has landed** — `docs/BET-drop-jobs-mirror-C3.md`. It removes the only hard
   coupling between the two Airtable bases, so inventory can then move on its own clock.

**One exception, and it's free:** the five main-base `Jobs` reads (§4 below) can flip to Neon the
day `handleJobs` flips in the field app, because they read data that is *already* in Neon and
touch no inventory table. That is Step B0 and it is genuinely independent.

---

## 2. What is actually there

`inventory.js` is 2,997 lines / 49 handlers. `inventory.html` is 8,426 lines. The base
`appfsLJwfow4CepCw` has **17 tables**. Row counts below are **unconfirmed** — count them before
sizing anything (a script over the API with `fields[]=` empty is the cheap way).

| Table | Role | Slice |
|---|---|---|
| `Jobs` (`tblBWsMk3Gmv7bdCu`) | the mirror — **deleted by C3**, never migrates | A |
| `Vendors` | reference | B |
| `Vendor Pricing` | reference (per-item, per-vendor cost) | B |
| `Locations` | reference (shops, trucks, trailer) | B |
| `Inventory Items` | the catalog — **48 fields, ~20 of them derived** | B |
| `Inventory Transactions` | the ledger. Everything else is derived from this | C |
| `Stock Levels` | **not a table — a cache** (see §5) | C |
| `Estimates` | material estimates (⚠ *not* the main base's estimates) | D |
| `Estimate Line Items` | | D |
| `Estimate Templates` | | D |
| `Estimate Template Lines` | | D |
| `Material Orders` | | D |
| `Material Order Lines` | | D |
| `Labor Units` | conduit assemblies engine — **built, not wired into the app** | D |
| `Conduit Assemblies` | same | D |
| `Assembly Components` | same | D |
| `Expense Pushes` | push history/audit | E |
| `Expense Push Lines` | frozen per-line snapshot | E |

**The three conduit-assembly tables are not in use yet** (`project_conduit_assemblies_estimating`
— two rollup fields still need adding by hand in Airtable, and there's no app integration). That
makes them the one place where the right answer might be *build it in Neon and never migrate it*,
like panel schedules and job checklists were. Decide that at Step D, not now.

---

## 3. The single best reason to do this at all

It isn't speed, and it isn't tidiness. **`Inventory Items` encodes the location list in its field
names.**

Quantity-on-hand exists as fourteen fields — `Qty In (Shop #1)`, `Qty Out (Shop #1)`,
`Quantity On Hand (Shop #1)`, then the same trio again for Shop #2, `#4 Transit`, `#5 Express`,
`#6 Trailer`, plus a Global pair. Every one is a rollup or formula over the same transaction
table, filtered by a location the field name hard-codes.

**Adding a location today means hand-adding three Airtable fields and editing the frontend.** In
Postgres the whole block is one view:

```sql
CREATE VIEW v_stock_on_hand AS
SELECT item_id, location_id,
       SUM(CASE WHEN direction = 'in' THEN qty ELSE -qty END) AS qty_on_hand
FROM inventory_transactions GROUP BY 1, 2;
```

Fourteen fields become zero. A new truck becomes a row. That is the argument to make to the owner
— not "Postgres is better", but *"you can add a truck without me editing anything."*

The same shape applies to `Price Variance ($)`, `Price Variance (%`, `Suggested Default Unit Cost`
and `Unit Cost Rollup (Live)` — all derived from `Vendor Pricing` and all free in SQL.

---

## 4. The couplings that set the order

`inventory.js` reaches into the **main** base in exactly 11 places. They are the whole reason this
app can't just be lifted:

| Line | Main-base table | What it does | Where it goes |
|---|---|---|---|
| 112 | `Employees` | `handleLogin` — identifier + PIN compare | **shared with the field app — see §7** |
| 143 | `Employees` | `handleEmployees` — active list | Neon `employees` (already exists) |
| 166, 198, 231, 247, 544 | `Jobs` | job pickers, estimating jobs, awarded jobs, template contractors, pending-expense grouping | Neon `jobs` (already exists, 112 rows, hourly) — **Step B0** |
| 1045 | `Expenses` | idempotency read (`Push ID`) | Step E |
| 1127, 1167 | `Expenses` | **creates** the materials + tax expense records | Step E |

**Two of those five Jobs reads and both Employees reads can flip today-ish** — Neon already holds
that data. The two `Expenses` *writes* at 1127/1167 are the last thing to move and are gated on
field-app Step 4d.

---

## 5. Two things Airtable is doing that Postgres has to take over

### 5a. `Stock Levels` is a cache maintained by an Airtable automation

Its own description says so: *"Updated automatically by Airtable automation when a transaction is
created."* So `Quantity On Hand` is a **stored copy** of something `Inventory Transactions` already
implies — and `Inventory Items` derives the *same number a second, independent way* via its
rollups.

> ⚠ **Run this check before migrating anything:** do the two derivations agree today? A stored
> cache updated by an automation drifts the moment the automation misfires or a transaction is
> edited rather than created. If they disagree, that is a **live data problem to report to the
> owner**, not a migration detail — and it is far easier to find now than after the move.

**`Stock Levels` does not become a pure view, despite what ROADMAP §4 says.** It carries two
fields that are real user data, not derived:

- `Reorder Point` — written by `handleUpdateReorderPoint`, read by `handleReorderAlerts`
- `Notes`

So it splits: a small real table (`stock_settings`: item, location, reorder_point, notes) plus
`v_stock_on_hand` above. Update ROADMAP §4 Step C when this lands.

> Also note **there are two reorder points** — one on `Stock Levels` (per item *per location*) and
> one on `Inventory Items` (`Reorder Point`, per item). Establish which one the app actually acts
> on before collapsing them.

### 5b. The `Stock ID` formula is being string-parsed for a foreign key

`handleStockLevels`, `handleStockLevelsAll` and `handleReorderAlerts` all recover the location name
by doing `stockId.split(" | ")` and taking the last segment. The location is a real linked record;
the code reads it out of a display string because that was easier.

That is three sites that silently break on any item name containing `" | "`. In Postgres it is a
join, and the bug class disappears. **Don't port the parse — port the intent.**

---

## 6. The steps

Same pattern as every field-app slice: **mirror → read-flip → write-flip → retire**, one table
family at a time, each independently shippable and revertible.

### Step A — drop the Jobs mirror (C3) · ~1 h hands-on + soak

Already planned and verified: `docs/BET-drop-jobs-mirror-C3.md`. Irreversible, needs an explicit
go-ahead. Nothing else here starts until it's done.

### Step B0 — the cross-base reads · ~1-2 h · **independent, can go early**

Point the five main-base `Jobs` reads and the `handleEmployees` read at Neon, Neon-first with an
Airtable fallback and a `_source` marker, exactly like the payroll reads. No inventory table moves.
No schema. Proves `_neon.js` works from `inventory.js` at all, which nothing has yet.

**NOT gated on the field-app `handleJobs` flip** — an earlier draft of this plan said it was, and
that was wrong. Verified 2026-08-07: the four Jobs handlers need only `Job Name`, `Job PO`,
`Job Status`, `Tax Status` and `Contractor Name (Text)`, and Neon `jobs` already carries every one
(`name`, `po`, `po_locked`, `status`, `tax_status`, `contractor_name`), refreshed hourly. The field
app's flip is blocked on ~35 *other* `mapJob` keys that inventory never reads. **This step is
genuinely independent of the field app.**

> ⚠⚠ **The one hard constraint: keep returning the Airtable rec id as `id`.**
> `handleJobs` → cart → `submitCart` stamps it as `Job ID (Main)` → `handlePushExpenses` writes it
> into a **linked-record field on main-base Expenses** (`"fldPNFIzq1grsdxYi": [String(jobId)]`,
> `inventory.js:1116`). Return the Neon uuid instead and the push silently writes uuids into an
> Airtable link field. Select `airtable_id AS id`, not `id`. Same for `handleEmployees`.
>
> This is precisely the `startsWith("rec")` bug class that already shipped once during 4c. It
> resolves itself at Step E, not before.

### Step B — reference data (Vendors, Vendor Pricing, Locations, Inventory Items) · ~5-7 h

The catalog. Big table, but **static-shaped** — no money moves when you read it wrong, and the
derived fields (§3) all collapse into views.

1. Schema `012_inventory_reference.sql`: `vendors`, `vendor_pricing`, `locations`,
   `inventory_items`, each carrying `airtable_id` for the soak.
2. ETL `db/etl/inventory-reference.mjs`, full-reload, same shape as `fleet.mjs`.
3. Read-flip: `handleItems`, `handleLocations`, `handleItemVendorPricing`.
4. Write-flip: `handleCreateItem`, `handleUpdateItemCost`, `handleSyncItemCostToVendor`,
   `handleUpdateReorderPoint` — **fail closed**, Airtable as fail-soft mirror.
5. Barcodes are the natural key users actually type. `Barcode Value` plus `Alternate Barcodes`
   (a multiline text field holding several) — model that as a `item_barcodes` child table with a
   UNIQUE constraint, and expect the constraint to reject real duplicate data on first load. That
   rejection is the point.

### Step C — the ledger (Inventory Transactions + Stock Levels) · ~6-8 h

The heart of it, and the biggest single win: it kills ~20 rollup/formula fields, the Stock Levels
automation, and the `" | "` string-parse in one go.

1. Schema `013_inventory_ledger.sql`: `inventory_transactions` (+ `push_id`, `main_job_id`,
   `job_name` — the field descriptions in Airtable already name these target columns),
   `stock_settings`, and the `v_stock_on_hand` view.
2. **`push_id` becomes `UNIQUE` with `INSERT … ON CONFLICT`** — the comment at `inventory.js:994`
   already anticipates this. The idempotency the expense push currently gets from re-reading
   Airtable becomes a database constraint.
3. Read-flip: `handleHistory`, `handleStockLevels`, `handleStockLevelsAll`, `handleReorderAlerts`.
4. Write-flip: `handleSubmitCart`, `handleReceive`, `handleTransfer`, `handleAdjustment`,
   `handleDelete`. **`handleSubmitCart` writes one Airtable record per line in a loop** — in Neon
   that becomes one multi-row insert in one transaction, so a half-submitted cart stops being
   possible.
5. Reconciler: on-hand per item per location, Neon vs Airtable, zero mismatches, for several days.

### Step D — estimating · ~6-8 h

`Estimates` / `Estimate Line Items` / `Estimate Templates` / `Estimate Template Lines` /
`Material Orders` / `Material Order Lines`.

> ⚠ **Name these carefully.** Neon already has `job_estimates` — the **main base's** estimates,
> which feed the GP views. The inventory base's `Estimates` is a *different thing* (a material
> take-off). Call them `material_estimates` / `material_estimate_lines`. Getting this wrong
> corrupts the GP layer, which is the one thing in this system that must not be corrupted.
>
> This is also exactly the seam `docs/BET-unify-estimates.md` is about. The bet's decision was a
> **loose link via shared job_id, not a merge**. Once both live in Postgres that link is a foreign
> key rather than a hope, so **Step D is when the unify bet becomes cheap** — but it is still a
> separate decision, not a freebie to smuggle in here.

Decide at the start of this step whether the three conduit-assembly tables migrate or get rebuilt
Neon-native. They have no app integration, so rebuilding is probably cheaper.

### Step E — the expense push · ~4-6 h · **last, and gated**

`Expense Pushes`, `Expense Push Lines`, and the two cross-base `Expenses` writes.

**Gated on field-app Step 4d** (expenses to Neon). Until then the push's destination is still an
Airtable table, so this step would move the source and leave the target behind — the worst
possible half-state for the one path that spends money.

> ⚠⚠ **The `expenses` mirror trap — the same one that bit pCloud folder ids.** Neon *already has*
> an `expenses` table (381 rows). It is a **full-reload one-way mirror from Airtable** — every row
> carries a single identical `synced_at` (2026-08-07 09:02:04), so the loader truncates and
> reloads. **An expense written into Neon before Airtable stops being the source will be silently
> erased by the next ETL run.** Same shape as `project_pcloud_job_folders_replumb`: flip *inside*
> the field-app expenses flip, never before it.

R2 already archives the materials PDF (`handleJobDocUploadUrl`), so that part needs nothing.

---

## 7. Login — a shared dependency, not an inventory slice

`handleLogin` exists in **both** functions and both read the main base's `Employees` for the PIN.
Neon's `employees` table has `name`, `username`, `role`, `active`, `qb_user_id` — **no PIN and no
email**. So login cannot flip here, and shouldn't: it moves once, for both apps, or the two drift.

It is also the last thread. When every other slice is done, login is the only thing still reading
Airtable — and moving it is the moment Airtable actually goes dark. Two things go with it:

- The PIN is stored and compared **in plaintext**. CLAUDE.md already flags this as a deliberate
  known gap from PR #19. Moving the compare into Postgres is the natural moment to hash it, and
  doing that *later* means doing it twice.
- `AUTH_SECRET` is shared, so a token from either app validates in both. That stays true and
  doesn't need touching.

**Treat this as its own final step, owned by neither app.** Not in scope for A–E.

---

## 8. Traps carried in from the field-app migration

Every one of these has already cost time once. They apply verbatim here.

1. **`grep "startsWith(\"rec\")"` is not enough.** It shipped a live regression during 4c
   (`project_step_4c_in_flight`) and would have rejected every truck at 4b. **Every handler that
   takes an id originating from a flipped read needs checking — even the ones that never
   validate.** `inventory.js` passes record ids around constantly (`itemId`, `locationId`,
   `jobId`, `txIds`), so this is the single highest-risk mechanical task in the whole plan.
2. **Writes fail closed, reads fail soft.** Settled at field-app Step 2 and not up for
   rediscussion. A write that lands in Airtable but not Neon is invisible once reads come from
   Neon.
3. **Don't key on a Neon FK that a lagging sync populates.** Panel schedules key on
   `job_airtable_id`, not a `jobs` FK, because `jobs` refreshes hourly and a brand-new job has no
   row for up to an hour (`project_neon_job_link_lag`). Inventory transactions already carry
   `Job ID (Main)` as text — **keep it that way**, don't "improve" it into an FK.
4. **Linked-record write shape** — bare `["rec…"]`, never `[{id:"rec…"}]`, for as long as the
   Airtable mirror is still being written.
5. **A comment-correction pass on every multi-diff build** — it caught two real bugs on the vendor
   build (`project_comment_rewrites_surface_bugs`). `inventory.js` is dense with comments that
   describe Airtable-shaped behaviour; most will be wrong after each flip.

---

## 9. Rough total

| Step | Size |
|---|---|
| A — C3 | ~1 h + soak |
| B0 — cross-base reads | ~1-2 h |
| B — reference data | ~5-7 h |
| C — the ledger | ~6-8 h |
| D — estimating | ~6-8 h |
| E — the expense push | ~4-6 h |
| *(login, shared)* | *~2-3 h, separate* |
| **Total** | **~23-32 h**, plus soaks |

Comparable to the field app's Steps 4a-4d put together, which is about right — it is a smaller
domain but a less-toured one, and the frontend is 8,400 lines nobody has flipped before.

---

## 10. Decisions needed before Step B starts

1. **Row counts** — unconfirmed for every table above. Count them; the sizing depends on it.
2. **Do the two on-hand derivations agree today?** (§5a) Possibly a live data bug, worth knowing
   either way.
3. **Two reorder points** — which one does the business actually use? (§5a)
4. **Conduit assemblies** — migrate or rebuild Neon-native at Step D? (§6)
5. **Does the unify-estimates bet get pulled into Step D**, or stay a separate later bet? (§6)
