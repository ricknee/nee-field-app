# Drop the Jobs mirror — Step C3 (the irreversible part)

**Status:** Planned 2026-08-03, not executed. Needs an explicit go-ahead.
**Steps A, B, C1, C2 are done, pushed and smoke-verified.** The bet's goal is already met: the
app reads the mirror nowhere. C3 is cleanup, not function.

---

## 1. What the mirror actually is

`Jobs` (`tblBWsMk3Gmv7bdCu`) in the **inventory** base `appfsLJwfow4CepCw` — a copy of the main
base's jobs, so `Inventory Transactions` could link to a job with a cross-base record link.

**Verified 2026-08-03:** that table carries a field `Sync Source` of type **`externalSyncSource`**.
That is Airtable's own **native synced-table** feature. **Make.com does not populate this table
and never did.** "Turning off the sync" means turning off an Airtable sync, not a Make scenario.

It was replaced in Step B by two plain text fields on `Inventory Transactions`:

| Field | ID | Purpose |
|---|---|---|
| `Job ID (Main)` | `fldePDNz1zc2bmNkk` | main-base job record id, written by `submitCart` |
| `Job Name` | `fldZlC25ou4d6CzCl` | human-readable label for debugging |

## 2. Does this break QB Time, Trello or pCloud? **No.**

This was the main worry and it is misplaced — those run off the **main** base, not the mirror:

| Make scenario | State | Does |
|---|---|---|
| `4509804` **Airtable – Job Awarded** | ACTIVE | `tsheets` → creates the QB Time job, then `trello` ×2 → the card |
| `4509211` **New Lead - Estimating (pCloud Folders)** | ACTIVE | ~20 `pcloud` modules → builds the job folder tree |
| `4723276` Upload to pCloud (Estimates/Invoices/Generator) | ACTIVE | PDF filing |

All three are triggered by main-base Airtable webhooks and touch main-base Jobs records. **None
reads the inventory base.** Deleting the mirror cannot affect new-job creation, QB Time customers
or jobs, Trello cards, or pCloud folders.

**pCloud folder creation stays on Make permanently, and that is fine.** pCloud's app-registration
page has been down for months (see `docs/PLAN-job-photos.md` §2), so there is no API route for us
to take it over. Make holds a working pCloud OAuth token from before that broke. C3 does not
remove Make and is not meant to.

## 3. What C3 actually deletes

1. `Inventory Transactions` → field **`Job`** (`fld7OG04Sgkp88JsU`) — the cross-base link
2. The **`Jobs` mirror table** itself (`tblBWsMk3Gmv7bdCu`)
3. The **Airtable sync** feeding it

Collateral that goes with the table (expected, not a surprise):
- `Jobs.Inventory Transactions` link (`fldE8NSDIiOotu1H3`)
- `Jobs.Inventory Item Rollup (from Inventory Transactions)` (`fldVOJoMlniTTXRh8`)
- `Jobs.Quantity Rollup (from Inventory Transactions)` (`fldrnHFAFMoEY3WEK`)

Those rollups only ever described the mirror's own view of transactions. Nothing in the app reads
them — verified by grep below.

## 4. Verification done 2026-08-03

- `fld7OG04Sgkp88JsU` appears in the codebase **once**, in a comment in `inventory.js:366`
  explaining why the link is deliberately no longer written. No live reference.
- `tblBWsMk3Gmv7bdCu` appears in the codebase **zero** times.
- `inventory.js` writes `Job ID (Main)` + `Job Name` at `submitCart`; `handlePendingExpenses`
  resolves the main-base job straight from `Job ID (Main)`.
- The mirror's `Sync Source` field type confirms an Airtable-native sync, not Make.

**Still to verify before executing** (the one gap): the 2026-06-06 audit that found no Make
scenario references the inventory base is two months old. Re-confirm by checking the blueprints of
the active scenarios for base id `appfsLJwfow4CepCw` before deleting anything. The scenarios
*list* endpoint does not include base ids — it must be a blueprint check.

## 5. Order of operations

Deliberately reversible for as long as possible. Nothing before step 4 destroys data.

1. **Re-run the Make audit** (§4 gap). Abort if anything references the inventory base.
2. **Snapshot the mirror.** Export `Jobs` (`tblBWsMk3Gmv7bdCu`) to CSV and keep it with the
   other backups. It is a derived copy, but the export costs a minute and removes the "what did
   it hold?" question forever.
3. **Turn OFF the Airtable sync** on the mirror, leaving the table in place. Stop the flow before
   removing the vessel — if anything unexpected depended on fresh data, it surfaces now, while
   everything is still recoverable.
4. **Soak 48 hours.** Do a real inventory push and a real submitCart in that window. Confirm the
   expense push still groups correctly and `Job ID (Main)` still resolves.
5. **Delete the `Job` link field** on `Inventory Transactions`. Irreversible.
6. **Soak again**, at least one more real push.
7. **Delete the `Jobs` mirror table.** Irreversible.
8. Update `docs/SYSTEM-MAP.html` (inventory base drops from 14 tables to 13) and close the bet.

## 6. What could go wrong

| Risk | Reality |
|---|---|
| New jobs stop reaching QB Time / Trello / pCloud | **No** — main-base scenarios, untouched (§2) |
| Expense push breaks | Already runs off `Job ID (Main)`; step 4's soak proves it before anything is deleted |
| A stray Airtable view/automation in the inventory base uses the link | The single native automation (Stock Levels) ignores it — re-check at step 1 |
| Historical transactions lose their job association | They keep `Job ID (Main)` + `Job Name`. Rows created **before** Step B may have only the old link — **check for those at step 2** and backfill if any exist, or accept losing the association on old rows |
| Someone wants the mirror back | Re-creating a synced table is a few clicks; the CSV from step 2 covers the data |

> ⚠ The last-but-one row is the real one to check. Step C1 backfilled 19 transactions across 3
> jobs, but confirm the count of rows with a `Job` link and **no** `Job ID (Main)` is zero before
> deleting the field.

## 7. Time

Steps 1-3: ~30 min. Then two soak windows. Steps 5-8: ~15 min. Total hands-on well under an hour,
spread over 3-4 days by the soaks.

## 8. Honest assessment of value

The bet's actual goal — the app no longer depends on the mirror — was achieved at Step C2. C3
removes a stale duplicate of production data that no longer updates anything and nothing reads.

That is worth doing (a synced copy of jobs sitting in a second base is exactly the thing someone
wires something new into by accident a year from now), but it is **not urgent**, and it is the
only genuinely irreversible item on the whole roadmap. There is no cost to leaving it a while
longer, and no decay if it never happens.
