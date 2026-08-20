# Audit — what is left on the inventory app

*Run 2026-08-19 from the code, the live Neon database, the live Airtable base and its automations,
and by executing the test suites. Not written from `ROADMAP.md` or from
`PLAN-inventory-airtable-cutover.md` — both of those are now stale, and saying how is part of the
finding.*

Published copy (same content, easier to read):
https://claude.ai/code/artifact/34267f7d-94a6-4b1e-8838-6a37ae07d1f0

---

> ## ✅ PROGRESS — 2026-08-20
>
> | | Item | State |
> |---|---|---|
> | 1 | **The three id-currency bugs** | ✅ **FIXED + DEPLOYED** — `f9c908e`, `db/schema/043`. 7 new tests, 6 fail on the old code. **247 pass.** ⏳ Owner browser smoke outstanding. |
> | 2 | **Decommission** | ⬜ **Owner-only.** Prototype exported ✅. ⚠ The Airtable MCP **cannot undeploy** an automation — only delete — so the remaining three steps are UI actions. |
> | 3 | **Documentation drift** | ✅ **DONE** — `4c3e6c8`. All five files plus the stale `inventory.html` comments. |
> | 4 | **Test gaps** | ⬜ Not started. Still 12 uncovered write paths. |
> | 5 | **Operational decisions** | ⬜ Owner. |
>
> 🔴 **And one thing this audit did not look for, found while fixing item 3.** `netlify.toml` has
> `publish = "."`, so **every tracked file was a public static asset** — `netlify/functions/_auth.js`,
> `airtable.js`, `inventory.js`, all of `db/schema/`, and every doc in this folder, all answering
> **200 with no authentication**. Fixed in `4c3e6c8` + `f30ba66` with forced 404 routes. No secret
> leaked: `.env` is gitignored and Netlify does not serve dotfiles, and `AUTH_SECRET` is env-only,
> so tokens were never forgeable. What was readable was the auth *scheme*, the `authzFor` policy,
> every Airtable field id, and the database design. Full detail in `netlify.toml`.
>
> ⚠⚠ **`force = true` is the whole rule.** Netlify skips a redirect when the path matches a real
> file — which every blocked path did. The first attempt shipped without it, **the deploy went
> green, and `_auth.js` still answered 200.** A green deploy is not a working rule; re-request the
> URL.

---

## The verdict

**The migration is finished. The app is not.**

`inventory.js` makes **zero reads and zero writes to the Airtable inventory base** — re-confirmed
by call graph, not grep. Every domain is native in Postgres, every read fails closed, the loader is
gone, and all 239 automated tests pass. That half is genuinely done.

What is left splits into four things, and only one of them was on any plan:

| | What | Cost | On a plan? |
|---|---|---|---|
| 🔴 | **Three latent id-currency bugs** from the last two slices — the sweep that catches them was never run on `locations` or on native `inventory_items` | ~2 h + smoke | **No** |
| ⬜ | **Decommission** — undeploy one dead Airtable automation, archive the base, drop `INVENTORY_BASE_ID` | ~1 h, owner action | Yes (slice 6, half-done) |
| 📄 | **Documentation drift** — `CLAUDE.md`, `ROADMAP.md`, `SYSTEM-MAP` and the cutover plan all still describe a two-base app | ~1 h | No |
| 🧪 | **Test gaps** on 12 write paths, including `receive`, `transfer` and the entire reference-data slice shipped on the last day | ~3-4 h | No |

**Nothing here is blocking, and nothing here is Airtable-migration work.** The honest remaining
figure is **~6-8 h**, of which ~2 h is the only part that can bite.

---

## 1. Measured state — 2026-08-19

Everything in this section was counted, not recalled.

### The function

| | |
|---|---|
| `inventory.js` | 3,775 lines · **58 dispatched actions** (24 GET, 34 POST) |
| `inventory.html` | 9,125 lines · calls **57** of the 58 |
| Dead endpoint | **1** — `getExpenseFields`, a debug action that prints Airtable field ids from the metadata API. Never called by the UI, and `authzFor` lets **any signed-in role, viewer included**, hit it. |
| Reads/writes to Airtable **inventory** base | **0** |
| Airtable **main**-base calls | 9 sites, in two groups — see below |

### The nine remaining Airtable calls, and which ones are real

Eight of the nine are **fallbacks that never run while Neon is up**:

- `handleLogin` (gated by `LOGIN_SOURCE`, currently `neon`)
- `handleEmployees`, `handleJobs`, `handleEstimatingJobs`, `handleAwardedJobs`,
  `handleTemplateContractors`, and the job index inside `handlePendingExpenses` — all Neon-first
  with an Airtable fallback that returns the identical shape

The ninth is real, and it is the only Airtable dependency this app still has at runtime:

> **`handlePushExpenses` writes main-base `Expenses` first, then mirrors to Neon and fails closed.**
> Two `POST`s (materials + sales tax), one `filterByFormula` read for the `Push ID` idempotency
> guard, one `GET` re-read per created expense so the Airtable formulas are populated before the
> sync, one metadata-API call for the receipt field id, and one multipart attachment upload.

**This is not inventory-app work.** Airtable is still the *identity* authority for expenses in both
apps — R2 receipt keys are built from the expense rec id — so the push leaves Airtable when
**expenses** leave Airtable, which is a field-app decision. See `_expenses.js` for why this caller
fails closed and the field app's doesn't.

### Neon, live

| Table | Rows | Native (`airtable_id IS NULL`) | Latest |
|---|---:|---:|---|
| `inventory_transactions` | 871 | 8 | **2026-08-18** |
| `inventory_items` | 866 | **0** | — |
| `stock_settings` | 281 | 0 | — |
| `expense_pushes` / `_lines` | 34 / 415 | 0 | 2026-08-11 |
| `material_estimates` / `_lines` | 18 / 595 | 2 | 2026-08-12 |
| `material_orders` / `_lines` | 11 / 248 | 1 | 2026-08-12 |
| `material_estimate_templates` / `_lines` | 3 / 149 | 0 | 2026-07-17 |
| `locations` | 6 | **1** (`Test Shop`) | 2026-08-12 |
| `vendors` | 4 | 0 | — |
| `vendor_pricing` | **2** | 0 | — |

**The app is in daily use** — the newest ledger row is from yesterday. The native counts are what
matter for §2: the domains that went native last are the ones with almost no native rows yet, which
is precisely why the bugs below have not fired.

### Airtable, live

- Base `appfsLJwfow4CepCw` **still exists** — 17 tables, **not archived**.
- `Inventory Transactions` frozen at **863 rows, last written 2026-08-11**. Neon moved on to 871.
  Nothing has touched the base since the cutover. ✅
- **0 interfaces, 0 forms** (slice 0 deleted them). ✅
- ⚠ **1 automation still deployed:** `wflTe6pr2oCtig6qp` "Stock Levels" — a `recordCreated` trigger
  on `Inventory Transactions` running a script that maintains the old on-hand cache. It cannot fire
  (nothing creates transactions there any more) so it is dead weight rather than a hazard, but it
  should be undeployed before the base is archived.
- The three **conduit-assembly** tables hold **1 assembly and 1 labor unit**, created 2026-06-17.
  A prototype, exactly as recorded. Nothing of value is lost by archiving.

### Tests

**239 pass, 0 fail** across all four suites (`handlers` 161, `inventory-reference` 58, `inventory-jobs` 12,
`inventory-push` 8). Run with `AUTH_SECRET` set and no `DATABASE_URL`.

---

## 2. 🔴 The real finding — three sites still speak the old id currency

This is the trap the cutover plan names twice and the memory file names in capitals:

> **When a domain goes native, grep `airtable_id` in every WHERE clause — match on the CLAUSE, not
> the table name.**

It was run for `inventory_items` when slice 5 landed, and it caught the handlers that were open at
the time. It was **never run for `locations`**, which became writable eight commits later in
`c0087ba`, and it **missed the three item sites that live inside a view** rather than inside a
handler's own SQL.

The writers are all correct. `handleLocationSave` and `handleItemUpdate` both resolve on
`airtable_id = $1 OR id::text = $1`, and `handleLocations` / `handleItems` both hand the frontend
`COALESCE(airtable_id, id::text)`. **The readers are what was missed** — so a natively-created row
saves perfectly, the picker shows it, and then it evaporates on the next read.

**None of these has caused damage yet.** Verified across all seven FK pairs — 0 unresolved rows in
`inventory_transactions`, `stock_settings` and `vendor_pricing`. They have not fired because there
are **0 native items** and the only native location is the throwaway `Test Shop`. Every one of them
fires on the next real one.

---

### F-01 · A stock movement into a natively-created location vanishes — silently

**`netlify/functions/inventory.js:560-561`** · severity: **high, silent**

```js
`(${p(r.txnDate)}, ${item}, (SELECT id FROM inventory_items WHERE airtable_id=${item} OR id::text=${item}),` +
`${p(Number(r.qty) || 0)}, ${p(r.type ?? null)},` +
`${from}, (SELECT id FROM locations WHERE airtable_id=${from}),` +   // ← no uuid arm
`${to},   (SELECT id FROM locations WHERE airtable_id=${to}),`  +    // ← no uuid arm
```

The item subselect takes **both** handle forms. The two location subselects take **only the rec
id**. `handleLocations` serves a native location as its uuid, so the uuid arrives here, matches
nothing, and the row inserts with `from_location_id` / `to_location_id` **NULL** while the
`*_airtable_id` columns hold the uuid.

`v_stock_levels` builds its pairs from `v_stock_on_hand` and then does
`JOIN locations l ON l.id = p.location_id`. **A NULL `location_id` drops the row entirely.**

**Failure scenario.** An admin adds "Shop #3" from Manage Locations. It appears in every picker.
A crew member logs 40 sticks of 3/4" EMT out of Shop #3 to a job. `submitCart` returns success, the
transaction is in the ledger, it appears in History, and it will push to the job as an expense.
But Check Stock shows Shop #3 with **nothing in it**, forever, and the reorder alert for that item
never counts the stock that left. On-hand is wrong and no error is ever raised.

This path is shared by `submitCart`, `receive`, `transfer` and `adjustment` — all four ledger
writers go through `insertTxns`.

**Fix:** `WHERE airtable_id = ${from} OR id::text = ${from}` on both arms. One line each.

---

### F-02 · A natively-created item has no stock, and no category

**`netlify/functions/inventory.js:2466`** (plus `:2489` and `:2540` region) · severity: **high, silent**

```js
FROM v_stock_levels WHERE item_airtable_id = $1     // handleStockLevels
```

Same shape, other entity. `handleItems` serves a native item as its uuid; this filters on
`item_airtable_id`, which is NULL for that item. The query succeeds and returns **zero rows**, so
`q?.rows` is truthy, the handler answers `_source:"neon"` with `levels: []`, and Check Stock renders
a clean, working-looking screen saying the item is nowhere.

Two sibling handlers leak the same NULL outward rather than filtering on it:

- **`handleStockLevelsAll`** returns `itemId: r.item_airtable_id || ""`
- **`handleReorderAlerts`** returns `itemId: … || ""` and `locationId: … || ""`

The frontend joins on that id — `itemCatMap[lv.itemId]`, `itemMap[lv.itemId]` at
`inventory.html:3312`, `:3387`, `:3531`. An empty string joins to nothing, so on the Inventory Value
screens a native item's stock lands under **"Uncategorized"** with a blank name and its value is
attributed to no category. (`handleReorderAlerts`' ids are display-only today, so that one is
latent-harmless — but it is the same defect and should go with the others.)

**Failure scenario.** Anyone creates a new item — the first one since 2026-08-12. They receive 500 ft
of it into Shop #1. Receive succeeds. Check Stock on that item shows **no locations at all**. The
Inventory Value tab counts its dollars under "Uncategorized" against a blank row.

**Fix:** add `item_handle` / `location_handle` columns to `v_stock_levels`
(`COALESCE(i.airtable_id, i.id::text)`, same for `l`), filter and return those. One migration,
three call sites — cleaner than patching each handler, and it closes the class rather than the
instances.

---

### F-03 · Setting a reorder point on a native location fails *and* leaves an orphan row

**`netlify/functions/inventory.js:2613`** · severity: **medium, loud**

```sql
VALUES ($1, (SELECT id FROM inventory_items WHERE airtable_id = $1 OR id::text = $1),
        $2, (SELECT id FROM locations WHERE airtable_id = $2), $3, now())   -- ← no uuid arm
ON CONFLICT (item_id, location_id) WHERE item_id IS NOT NULL AND location_id IS NOT NULL
```

Third instance of the same miss. This one is **caught** — the handler checks the returned
`location_id` and answers 404 "Item or location not found", which is the guard working as designed
and the reason this is medium rather than high.

But the INSERT has already run by then. With `location_id` NULL the **partial unique index does not
apply**, so the row lands, sits outside `v_stock_levels`, and is reported to the user as a failure.
Retrying appends another one. The guard's own comment predicts this exact state — "saved, and
invisible" — it just doesn't undo it.

**Fix:** the uuid arm (which makes the guard unreachable for this cause), and either wrap it in a
transaction or make the resolution a precondition rather than a subselect.

---

## 3. ⬜ Decommission — slice 6, half done

The **code** half shipped in `79b1b56`: `loadInventoryReference` is deleted, and with it the last
reason the Airtable base had to stay coherent.

The **Airtable** half has not been done, and the base is still sitting there:

1. **Undeploy `wflTe6pr2oCtig6qp` "Stock Levels"** — the last deployed automation on the base.
   Zero risk; it has had nothing to trigger on since 2026-08-11.
2. **Export the conduit-assembly prototype** if it is worth keeping — 3 tables, 1 assembly, 1 labor
   unit. The per-ft cost engine is a *future native build*, explicitly not migration work, and its
   Airtable prototype is a reference sketch, not data. Screenshot or CSV is enough.
3. **Archive the base.** Nothing reads it, nothing writes it, it has no interfaces and no forms.
4. **Delete `INVENTORY_BASE_ID` from the Netlify dashboard** and from `.env.example`. It is dead in
   code; the three test suites set it only as leftover scaffolding.

> ⚠ Do steps 1-3 in that order. Archiving with a deployed automation attached is the kind of thing
> that is fine until it isn't, and it costs ten seconds to avoid.

---

## 4. 📄 Documentation drift — four files describe an app that no longer exists

The code is a week ahead of everything that documents it. Worth an hour, because the next session
will read these first and the memory file already warns that stale docs cost a real debugging
detour once before.

| File | What it still says | Truth |
|---|---|---|
| `CLAUDE.md:56` | `INVENTORY_BASE_ID` is a required env var for `inventory.js` | Not read anywhere |
| `CLAUDE.md:144` | `inventory.js` is "unique in that it spans **two Airtable bases**" | It spans one, for the expense push only |
| `docs/ROADMAP.md` §1 | "⬜ Items (~2-3 h) then decommission (~1 h)"; writes "**36 → 6**" | Items shipped; writes are **0** |
| `docs/PLAN-inventory-airtable-cutover.md` | Header: "Only decommissioning is left"; slice 5 "⬜ Not yet prod-smoked"; slice 6 "⬜ ~1 h. The loader now only reads…" | Slice 5 smoked, loader deleted |
| `docs/SYSTEM-MAP.html:611` | An "Inventory base — 14 tables" card in the live architecture | Should move to a retired/archive treatment |

Also worth a pass: **`inventory.html` carries seven stale Airtable comments** describing behaviour
that has moved (`:1492` "Save to Airtable in background", `:1652` "Combine same-item rows into one
Airtable record", `:5066` "sequential because Airtable rate-limits at 5 req/sec" — which is now an
unnecessary throttle on a Postgres write, `:6651` "fresh totals from the Airtable rollup").
Per the project's own habit, a comment-correction pass tends to surface real bugs; this audit found
three by doing exactly that.

---

## 5. 🧪 Test gaps

78 inventory-specific cases cover the migration well — every read fails closed under test, every
native write is proven native, and the push has both idempotency guards. The gaps are all in write
paths, and they cluster in the two slices that shipped last:

**No coverage at all:**

- `receive`, `transfer` — core ledger writers, native and fail-closed, and both go through the
  `insertTxns` path that carries **F-01**
- `locationSave`, `vendorSave`, `vendorPricingSave`, `vendorPricingDelete` — the entire
  reference-data-writable slice (`c0087ba` / `621fbe0`), shipped the same day as the loader
  retirement with zero tests
- `orderCreate`, `orderUpdate`, `orderDelete`, `refreshTemplatePrices`, `saveEstimateAsTemplate`,
  `estimateTemplateLineUpsert` / `LineDelete`
- `syncItemCostToVendor`, `jobDocUploadUrl`

> ⚠ The lesson from slice 1 applies directly to F-01 and F-02: **give the mock two id spaces, or it
> cannot catch the only bug that matters.** The push suite passed while shipping an outage because
> its uuid and rec id were the same string. Any test written for these fixes must use a genuinely
> different value for the uuid and the rec id — and a location row whose `airtable_id` is NULL.

---

## 6. Operational observations — not code

Found while measuring. None of it is a defect; all of it is the owner's call.

- **62 transactions are pending an expense push, worth ~$4,455 gross.** The last push was
  **2026-08-11**. 53 of them sit on **Awarded** jobs and are pushable today (**$2,113**, oldest
  2026-07-08) — that is real material cost not yet on a job or in GP.
- **9 of the 62 can never be pushed.** All from 2026-04-28, all with no `job_airtable_id` at all, so
  the push has no job to charge. They **net to exactly zero** (122.7 out, 122.7 returned), so no
  money is missing — but they will sit in the pending list forever. Worth deleting or marking so the
  list means something.
- **`vendor_pricing` holds 2 rows, `vendors` holds 4.** A full vendor-pricing UI exists — per-item
  vendor comparison, preferred-vendor flags, `syncItemCostToVendor` — against essentially no data.
  Either it needs loading or the feature is speculative; both are fine, but it is worth knowing
  which before building further on it.
- **`getExpenseFields` is reachable by a viewer.** A debug endpoint that lists Airtable field ids.
  Deleting it removes the app's last metadata-API call and one line from `authzFor`'s blast radius.

---

## 7. Explicitly *not* on this list

Recorded so a future session doesn't re-scope them onto the inventory app:

- **The expense push leaving Airtable.** Gated on expenses leaving Airtable, which is a field-app
  decision driven by R2 receipt keys. §1.
- **The conduit-assembly per-ft cost engine.** Owner, 2026-08-12: a **future new build in Neon,
  after the migration is fully complete**. Not migration work. Do not port the Airtable tables.
- **Contacts** (`listContactsByCompany`, `createContact`, and the power-company pair). Field app,
  item 06's last slice, and the only domain with no Neon table at all.
- **Stock figures reading lower than they used to.** On-hand is derived from the ledger now; the old
  Stock Levels cache disagreed on 237 of 269 pairs and was deliberately not ported. More reorder
  alerts is **the correction**, not a regression. `db/schema/032`.

---

## Recommended order

1. **F-01 + F-02 + F-03 in one commit**, with the two-id-space tests, then smoke: add a location,
   move stock into it, check stock; create an item, receive it, check stock and the value tab.
   *~2 h. This is the only item with a live failure mode.*
2. **Decommission** — undeploy, export the prototype, archive, drop the env var. *~1 h, mostly owner.*
3. **Documentation pass** — the four files above plus the stale `inventory.html` comments. *~1 h.*
4. **Backfill tests** for `receive`, `transfer` and the reference-data slice. *~3-4 h.*
5. **Decide** on the pending-push backlog and on vendor pricing. *Owner.*
