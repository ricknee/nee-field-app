# Drop the Jobs mirror — Step C3 (the irreversible part)

**Status: ✅ CLOSED 2026-08-10. The Jobs mirror no longer exists.** Every step of §5 executed and
verified — execution log in §9. The `Job` link field and the mirror table are both deleted, and
the two Airtable bases are structurally decoupled: nothing in the inventory base reaches the main
base except the expense push, which runs on `Job ID (Main)` text.

**Verified after the fact, not assumed:** the API now returns `422 Could not find a field` for
`fld7OG04Sgkp88JsU` and `422 Could not find a table` for `tblBWsMk3Gmv7bdCu`; all 501 transactions
still carry `Job ID (Main)`; and a real push run *after* both deletes created main-base expense
`recgkGpRDCONTGjbQ` on the right job with the right markup.

Table deletion revert handle: `actEuOPZfW1yT1YQ0` (Airtable best-effort — treat as permanent; the
CSV in `nee-backups` is the real backup).

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
3. **Turn OFF the sync** on the mirror, leaving the table in place. Stop the flow before removing the
   vessel — if anything unexpected depended on fresh data, it surfaces now, while everything is
   still recoverable. ⚠ There are **three** sync sources, not one, and the right move is the
   Update-method toggle rather than removing sources — see §9 for the exact option to pick.
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

## 8. Why this matters for the Neon migration

Confirmed with the owner 2026-08-03: **the inventory app is in scope for the migration too**, not
just the field app. That reframes C3 from tidying to sequencing.

The mirror is **the only hard coupling between the two Airtable bases**. While it exists,
`Inventory Transactions` reaches a job through a *cross-base Airtable record link* — a construct
with no equivalent in Postgres. You cannot port it; you can only replace it, which Step B already
did with `Job ID (Main)`, a plain record-id string. That is exactly the shape Neon wants: a
foreign key by value.

So C3 is what makes the inventory base **structurally independent** of the main base. After it,
the two can migrate on their own timelines instead of having to move together — which matters,
because the field app's Neon work (time entries, payroll, GP) is mid-flight and the inventory app
shouldn't have to wait on it.

The inventory schema is already annotated for this. Field descriptions in the base itself carry
the target mapping:

- `Inventory Transactions.Job ID (Main)` — *"Future Neon: inventory_transactions.main_job_id"*
- `Expense Pushes.Push ID` — *"Future Neon: expense_pushes.push_id UNIQUE"*
- `Labor Units` — *"Future Neon: labor_units table, Labor Code as UNIQUE business key"*
- `Conduit Assemblies` — *"per-ft money fields become a SQL view over components"*

Rough order once C3 lands (not planned in detail yet): reference data first (Vendors, Locations,
Inventory Items), then the ledger (Inventory Transactions, Stock Levels as a view rather than a
maintained table), then estimating (Estimates, Templates, Assemblies). The expense push is the
seam to the main base and should move last, because it is the only path that writes across.

## 9. Execution log

### ✅ Step 1 — Make audit re-run — **PASSED 2026-08-08**

The §4 gap ("the 2026-06-06 audit is two months old") is closed. Checked **blueprints**, not the
scenarios list, because the list endpoint carries no base ids.

- Team 6575 has **70 scenarios; 18 are active.** ⚠ **`isPaused` is NOT the activation flag** — it
  is `false` on all 70. The real flag is **`isActive`**. Anything auditing Make scenarios must use
  `isActive` or it will read every retired scenario as live.
- **22 scenarios use an `airtable` or `http` module** (the only two ways to reach an Airtable base);
  the other 48 use neither and were excluded on that basis. **All 22 blueprints were fetched and
  grepped.**
- **Result: 0 references to `appfsLJwfow4CepCw` and 0 to `tblBWsMk3Gmv7bdCu`.** Every Airtable
  scenario points at the main base `appiqWg6SvKcGfMAu`. (One inactive 2022 scenario, `351560`,
  points at a long-dead base `apptaMAETfGqbbT7N` — neither ours nor relevant.)
- Confirmed in passing: **`4546051` has `isActive: false`**, so Step 3 of the roadmap is still
  holding, and the mirror's `Sync Source` field is type **`externalSyncSource`** — re-verifying that
  this is an Airtable-native sync, not a Make scenario.

Audit script: `scratchpad/c3-make-audit.mjs` (throwaway; re-derivable from the notes above).

### ✅ Step 2 — orphan check + snapshot — **PASSED 2026-08-08**

**2a — the risk row in §6, cleared.** Transactions with a `Job` link (`fld7OG04Sgkp88JsU`) but
**no** `Job ID (Main)` (`fldePDNz1zc2bmNkk`): **0**. The filter was proved to discriminate rather
than silently return nothing — **116** transactions carry the legacy `Job` link, and all 116 also
carry a `Job ID (Main)`, every value a well-formed `rec…` id. **Deleting the link field loses no
job association.**

**2b — snapshot taken.** 26 rows × 19 columns, written **outside the repo** to
`C:\Users\irick\projects\nee-backups\inventory-jobs-mirror_tblBWsMk3Gmv7bdCu_2026-08-08.csv`
(+ a `.json` alongside).

> ⚠ **Deliberately not in the repo.** `netlify.toml` publishes the repo **root**, so a CSV committed
> there would be served publicly on the live site.

> ⚠ **Neither PAT in `.env` can read the inventory base** — `AIRTABLE_PROD_READ_PAT` and
> `AIRTABLE_API_KEY` both return **403** on `appfsLJwfow4CepCw` (they are scoped to the main and
> sandbox bases). The snapshot was taken through the Airtable MCP connection instead. **Anyone
> scripting against the inventory base needs a different credential** — worth knowing before Steps
> B/C/D of `PLAN-inventory-to-neon.md`, which will all need one.

The snapshot was verified rather than trusted: the Jobs-side link count reconciles **exactly** with
the Transactions side (4 jobs holding 116 links total, vs the 116 found independently), and all four
quantity rollups match the live read (Lance Koehn 8.3 · Ryan Yoder 19 · Kenny Barkan 11.5 ·
Bethel School 5670.39).

**Only 4 of the 26 mirror rows have ever held a transaction link** — Bethel School (99), Kenny
Barkan (7), Ryan Yoder (6), Lance Koehn (4). The other 22 are inert copies.

### ✅ Steps 3-7 — DONE 2026-08-08 → 2026-08-10

- **Step 3 — sync frozen 2026-08-08.** Update method set to *"Only sync changes when requested"*.
- **Step 4 — first soak.** A real `submitCart` (`TX-20260809-155855`) and a real expense push
  ($0.26, Bethel School) on 2026-08-09, with the sync frozen. The push grouped correctly off
  `Job ID (Main)`.
- **Step 5 — `Job` link field DELETED 2026-08-10.** ⚠ The delete dialog reported **5 dependencies**,
  and two of them were **interface elements the audit had never looked at** — §4 covered code, Make
  and base fields, but not Airtable **Interfaces**. They turned out to be two forgotten "Use Material"
  forms, both on `Inventory Transactions`. **They were already dead**: a form can set the `Job` link
  but cannot set `Job ID (Main)`, and `handlePendingExpenses` has resolved only by that text since C2,
  so anything logged through them never reached a push. Owner confirmed nobody uses them.
  > **Lesson for the next irreversible Airtable delete: check Interfaces.** `list_pages_for_base`
  > answers it in one call. A field/rollup/formula sweep is not a dependency sweep.
- **Step 6 — second soak.** A real push *after* the field delete: `TX-20260810-105119` → main-base
  expense `recgkGpRDCONTGjbQ`, Bethel School, $7.50 → $8.25 billable (10% markup), tax-exempt
  inherited, push id matching. End to end, with the link gone.
- **Step 7 — mirror table DELETED 2026-08-10** via the Airtable MCP (`actEuOPZfW1yT1YQ0`).

### Historical — what step 3 involved

Turn the sync **OFF** on the mirror, leaving the table in place. This is a **synced-table setting in
the Airtable UI** — Airtable exposes **no API for sync configuration**, so neither the MCP nor a
script can do it. Then soak 48 h (§5 step 4) with a real inventory push and a real `submitCart`
before anything is deleted.

> ⚠ **CORRECTION — the mirror has THREE sync sources, not one.** This file said "the Airtable sync"
> throughout; the Synced table settings dialog shows **Sources: Active 3**, all from
> *Northeastern Electric, Inc.*:
> **Project is Awarded** · **Service Calls** · **Project is Complete (Ready to Invoice)**.
>
> Those are exactly the three choices of the mirror's `Sync Source` field (`fldK8ZPItP6sXCp5N`,
> type `externalSyncSource`), so each row records which source view it arrived through. **Anything
> written assuming a single sync is wrong** — including step 3 of §5 as originally worded.

**How to actually stop it — Settings → "Automatically sync changes at regular intervals" → Change,
then pick the FIRST option:**

| Update method | Use? |
|---|---|
| **Only sync changes when requested** | ✅ **This one.** Freezes all three sources at once. Table stays a synced table, data and schema intact, reversible in one click. |
| Automatically sync changes at regular intervals | the current setting — what we're leaving |
| Stop syncing changes and convert to unsynced table | ⛔ **Not now.** A structural conversion, not a pause: the table stops being synced and `Sync Source` stops being an `externalSyncSource`. Rebuilding means re-creating all three source syncs. Doing this *before* the soak throws away the recoverability the soak exists to provide. |

> **Do NOT "remove source" on the three sources either** — removing a source can take its rows with
> it. Four of the 26 rows carry the 116 transaction links. Freeze, don't remove.

Residual risk of the chosen option: someone can still click **Sync now** and restart the flow by
hand. During a soak that is a feature, not a risk. Confirmation it took: the *"last synced N minutes
ago"* line stops advancing.

> 💡 **Park this for step 7.** "Convert to unsynced table" is a genuine **alternative to deleting the
> mirror** — it keeps the 26 rows as a plain historical table instead. Not today's decision, but
> don't let it get made by accident now.

**⬜ Unconfirmed at the time of writing:** whether the Update-method change was actually saved. Check
the dialog before starting the 48 h soak clock — the soak only counts from when the sync is genuinely
frozen.

---

## 10. Honest assessment of value

The bet's original goal — the app no longer depends on the mirror — was achieved at Step C2. On
its own, C3 removes a stale duplicate that no longer updates anything and nothing reads.

Read against §8 it earns more than that: it is the step that lets the inventory base move to Neon
independently. Still **not urgent** — it is the only genuinely irreversible item on the roadmap,
there is no decay if it waits, and it should happen when there is appetite for a 48-hour soak
rather than because it is next on a list.
