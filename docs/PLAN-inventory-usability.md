# PLAN — Inventory app usability (small changes, no redesign)

Owner asked 2026-11-07: make the inventory app friendlier for crew and admin
**without a major change**. Plan first, build after approval.

**Status:** round 1 (items 1–3) shipped in `2a17630` and confirmed by the owner on 2026-11-07. Round 2 (4, 5 as subtitles only, 9, 10) shipped right after, as a reorder and rewording only. Next: 8, then 6 (which needs its own plan).

**Round 1: items 1, 2, 3.** Items 4–11 are listed at the end, planned
but not scheduled.

Ground rules for every item:
- **`inventory.html` only.** No new backend actions, no schema changes. Each item reuses
  a screen and function that already exists.
- **Nothing changes how stock or money is recorded.** These are shortcuts into the
  existing flows. Submit, the ledger and push stay exactly as they are.
- Any new modal or sheet pushes a back entry, so the phone's back button closes it
  (the same pattern as every other screen here).
- Roles stay as they are: a shortcut never offers an action the home screen hides
  for that role (for example, Adjust Stock stays admin-only).

---

## 1. Show where you're working from

**Today.** Your location sits in a small "📍 Set location" pill in the top bar, and it's
easy to miss. It's already remembered on the device (`nee_inv_loc`), and it's the
**default** for the From/To picker on Use on Job, Return, Receive, Transfer and Adjust.
Add Wire Scrap is the only screen that refuses to run without one.

**Change.** Add one line at the top of the home screen, above "Log materials":

```
📍 Working from: Truck 3                                    Change
```

When no location is set, it reads as a prompt in the accent colour:

```
📍 Pick where you're working from — it fills in your screens   Pick
```

Tapping the line opens the existing location picker (`goPickLocation`). It updates
everywhere the pill already updates (`updateLocPill`). The top-bar pill stays, because it
still works from inside any screen.

**Size:** small. One block of HTML/CSS plus a few lines in `updateLocPill`.

---

## 2. "Continue on Job ___"

**Today.** Use on Job already fills in your **last job** (`nee_last_job`, shared with the
field app), but you only find that out after opening the screen.

**Change.** When a last job is set and still active, the Use on Job tile says so:

```
📦  Use on Job
    Continue on: Bethel — 1185 Old Rt 6          (tap to log)
```

With no last job, or a job that's no longer in the active list, the tile shows today's
subtitle: "Log items pulled for a job". The job can still be changed on the screen
itself, exactly as now.

Because the key is shared, opening a job in the field app and then switching to inventory
shows that job here. That's the point of the shared key.

**Size:** small. One function that sets the subtitle after jobs load and after any job pick.

---

## 3. Scan from the home screen

**Today.** Every screen has its own ⬛ scan button, so you pick the action first and then
scan. The bins already carry QR labels, and the label's QR holds the item's barcode.

**Change.** Add a wide **Scan a bin** button at the top of the home screen, beside or under
the location line. It uses the existing scanner (`startScan`), which covers both Android
and the iPhone ZXing fallback, with a new target, `"home"`.

- **The item is found:** a small action sheet opens:

  ```
  1/2" EMT Conduit 10'
  On hand at Truck 3: 14    ·    All locations: 212

  [ 📦 Use on Job ]  [ ↩️ Return ]  [ 🔄 Transfer ]
  [ 🔍 Check Stock ]  [ ✏️ Adjust ]  ← admin only
  ```

  Each button opens the existing screen and then selects the item, the same way that
  screen's own scan button does today (`pickItem`, `pickItemFor('xfr' | 'adj', …)`,
  `lookupStock`). After that, everything is the normal flow.
- **The barcode isn't recognised:** you get today's message ("Barcode not found: …") plus
  a **Search instead** button that opens Check Stock.

Return to Shop and Receive are left out of the sheet on purpose. Return to Shop is about
material bought for a job, not shop stock, and Receive starts from a vendor delivery
rather than a bin.

**Size:** medium. A new sheet, one branch in `handleScanResult`, and a check that each
target screen can take an item right after it opens (they wait on `dataReady`, so the
pick has to run after render).

**Risk to check while building:** the on-hand counts in the sheet come from data the app
already loads. If the per-location count isn't loaded yet at that moment, the sheet shows
the item and the buttons without the counts rather than waiting.

---

## Testing round 1

There's no automated UI test for `inventory.html`, so testing is a manual browser pass.
Add these lines to `tests/SMOKE.md`:

1. **Location line.** No location set shows the prompt. Pick one: the line, the pill and
   the From picker on Use on Job all show it. Reload: it's still there.
2. **Continue on.** Open a job in the field app, switch to inventory: the tile names it.
   Log an item, and it lands on that job. With an archived or unknown last job, the tile
   shows the plain subtitle.
3. **Scan.** On an **Android** phone and an **iPhone** (different scanner paths), scan a
   bin. Each sheet button opens the right screen with the item already selected. An
   employee doesn't see Adjust. Back closes the sheet. A bad barcode shows Search instead.

Deploy is a push to `main` after that pass.

---

## Round 3 — #8 Admin "needs attention" strip (built 2026-11-07)

**What it looks like.** It shows for **admins only**, at the top of home, under the location line:

```
┌ Needs attention ─────────────────────────────────────────────┐
│ 💰 $2,481.30 to push · 5 jobs     🔗 37 new part numbers     │
│ 📦 16 open orders                 🔴 14 items low            │
│ ⚠ 2 jobs can't be pushed — check Push Expenses               │
└───────────────────────────────────────────────────────────────┘
```

Each chip opens its screen: Push Expenses, New Part Numbers, Active Orders, Reorder Alerts.
The ⚠ line opens Push Expenses too, because that's where the unmatched-job warning already lives.

**No backend change.** All four numbers already have an action, and the data is small
(checked 2026-11-07: 133 unpushed entries, 1,064 invoice lines, 878 items):

| Chip | Source (existing) | Already loaded on home today? |
|------|-------------------|-------------------------------|
| $ to push / jobs / ⚠ unmatched | `pendingExpenses` → `pending[].jobTotal`, `taxable`; `unmatched[]` | no |
| New part numbers | `partNumbersQueue` → `waiting.length` | no |
| Open orders | `ordersCount` | **yes**, for the Active Orders badge |
| Items low | `reorderAlerts` | **yes**, for the Reorder Alerts label |

The two that already load are **reused**: one request feeds both the strip and the existing
badge, never two. The two new ones load only for admins, after the home screen has drawn,
so they never slow it down.

**⚠ The dollar figure must equal the Push Expenses grand total, to the cent.** That screen
adds **7.5% tax on taxable jobs** (`g.jobTotal * 0.075`). A strip that summed `jobTotal`
alone would show a smaller number than the screen it opens. So the grand-total math moves
into one shared function, `pushGrandTotal(pending)`, and the Push screen and the strip both
call it. That's the only refactor in this round.

**Rules:**
- **A request that fails hides its chip; it never shows 0.** "0 to push" when the read
  failed is exactly the silent wrong answer this system keeps getting bitten by.
- With everything at zero the strip shrinks to one line: **✓ Nothing waiting**.
- It refreshes on login and every time you come back home through `goHome()`, so after a
  push, going home shows the new figure. No timers and no polling.
- It's admin-only, so employees see nothing new. Office already doesn't get the inventory app.

**Testing (manual, on main):** the strip's $ matches the Push Expenses grand total (try a
taxable job). Each chip opens the right screen. Push something, go home, and the figure
drops. An employee sees no strip. Kill the network for one request (devtools) and that chip
disappears instead of reading 0.

**Size:** medium, about one evening. It's `inventory.html` only.

---

## Round 4 — #7 faster entry (built 2026-11-07)

**Correction to the original plan:** the +/− quantity buttons **already existed** on Use on
Job, Receive and Transfer. Only the second half was new:

- **Recent items.** Under the item search on Use on Job / Return / Return to Shop there's a
  **Recent** row: the last 8 items this person added to a cart **on this device**, newest
  first. Tapping one is exactly a search pick (`pickItem`).
  - It lives in device storage (`nee_inv_recent_items_<user id>`), keyed per person so a
    shared shop tablet doesn't mix two lists. That's a convenience, not a record, which is
    why it isn't in the database.
  - An item that's been retired drops off the row by itself.

---

## Next plans (written 2026-11-07, not built)

### #11 — Edit the price where you see the item — ✅ BUILT 2026-11-08

Shipped as planned: `stockPriceHtml` / `saveStockCost` / `goWireCostCalcFor` in `inventory.html`.
Save lights only for a real change, and a change of more than 3× either way asks first
(slipped decimal). Back from the Wire Calculator redraws the price (popstate → `refreshStockPrice`).

**Today.** A price can be changed in four places, and **every one already saves through
the same action, `updateItemCost`**: inline on Use on Job (admin), inline on Recent Logs
(admin), the **Update Item Price** screen, and the **Wire Cost Calculator** (for weighed
wire, $/ft invoice → $/lb).

**Change.** In the Check Stock result, admins get the same inline **Unit cost** box that Use
on Job already has. It saves through `updateItemCost` and shows the same toast. For a
weighed-wire item (`wireFtPerLb > 0`), a link instead reads **"Price from an invoice $/ft →"**
and opens the Wire Cost Calculator with that item already picked.

- **Keep the Update Item Price and Wire Cost Calculator buttons** in Admin · Setup for now.
  Remove them only if, after a few weeks, nobody uses them.
- No backend change. It's admin-only, the same tier `updateItemCost` already enforces.

**Size:** small–medium.

### #5b — One Return button (optional — recommend NOT yet)

Merge Return Items and Return to Shop into one **Return** button whose first question is
*"Was this from shop stock, or bought from a vendor for this job?"*. It then continues into
today's two flows unchanged.

**Recommendation: wait.** Round 2 already rewrote both subtitles to answer exactly that
question. Merge only if the crew still picks the wrong one.

**Size:** medium (it's a chooser in front of two existing flows).

### #6 — Undo after logging (backend; its own build) — ✅ BUILT 2026-11-08

Shipped as planned:
- `submitCart` stamps `entered_by` from the session (`neonEmployeeById`). If the lookup can't
  answer, it falls back to the sent name rather than refuse to log material.
- `undoSubmit { submitId }`, non-viewer tier, one statement: `FOR UPDATE` lock, then the
  all-or-nothing checks (pushed / older than 10 min / not yours unless admin), then DELETE.
  It answers 409 `pushed`/`too-old`, 403 not yours, 404 unknown, and 503 if WHO can't be confirmed.
  The exact SQL was proved through an untyped `PREPARE` on a Neon branch (every case).
- App: the post-submit toast became a bar, "✓ N items logged to <job> · Undo", for 2 minutes.
  A refusal shows its reason in the bar. Logout clears it.
- Tests: 11 "Undo" cases in `tests/inventory-reference.test.mjs`.
- Not done (possible follow-up): putting the undone items back into the cart for a quick fix.

**What makes it possible** (verified 2026-11-07):
- **Stock is calculated from the logged entries** (`v_stock_levels` → `v_stock_on_hand`),
  so removing the entries puts the stock back. Nothing else has to be reversed.
- **Every cart submit already carries a `submit_id`** (the idempotency key, `ON CONFLICT
  (submit_id, submit_line_no)`), so "everything from that submit" is one indexed lookup.

**Change.**
1. **Backend: a new action, `undoSubmit { submitId }`**, at the non-viewer tier. One
   statement, all or nothing:
   ```sql
   DELETE FROM inventory_transactions
    WHERE submit_id = $1
      AND expense_created = false            -- never un-log what was already charged
      AND txn_date > now() - interval '10 minutes'
   RETURNING id
   ```
   Before deleting, it refuses with a clear message if **any** line of that submit is
   already pushed or past the window. A partial undo would leave half a cart, which is
   worse than none.
2. **Ownership:** only the person who submitted (or an admin) can undo it.
   ⚠ `entered_by` is a **name the browser sends**, not taken from the session token. So
   this build must either stamp `entered_by` server-side from the token on `submitCart`
   (the better fix, and worth doing anyway), or compare against the token's name.
3. **Frontend:** after Submit, the success message becomes
   **"✓ 3 items logged to Bethel · Undo"**, with the Undo button live for 2 minutes
   (shorter than the server's 10, so a slow phone never offers an undo the server refuses).
4. **Tests:** handler cases for undoing within the window, refusing a pushed line, refusing
   after the window, refusing someone else's submit, and all-or-nothing behaviour.

**Size:** large-ish (backend + frontend + tests). It's money-adjacent because it removes
ledger rows that would otherwise be pushed, so it should get the full test treatment.

### Found while planning #6 — the existing Recent Logs **Del** button — ✅ FIXED 2026-11-07

Shipped: Del shows only for admins and only on unpushed rows. Pushed rows show "✓ pushed".
The server refuses a pushed row with a 409; the guard is inside the DELETE statement
(`expense_created IS NOT TRUE`), so a push landing mid-request can't slip through.
Tests are in `tests/inventory-reference.test.mjs` ("Del: …").

1. **Everyone sees "Del", but only admins are allowed** (`delete` is in `_ADMIN_WRITES`).
   An employee tapping it gets an error. **Fix:** show it to admins only (a one-line UI fix).
2. ⚠ **An admin can delete an entry that's already been pushed to an expense**, with no
   warning. `deleteTxn` deletes by id with no `expense_created` check. Stock goes back up,
   but the **expense stays on the job**, so the job is charged for material the ledger says
   never left. **Fix:** refuse (409) when `expense_created = true`, and say *"Already pushed
   to <job> — fix the expense in the field app first."* That's small, and **worth doing
   before #6**, because Undo uses the same rule.

**Suggested order:** the Del fixes (small, and they close a money gap) → #11 → #6.
Hold #5b.

---

## Round 5 (2026-11-08): the owner chose Undo refills cart, Count a truck, Smarter search, Work offline

- ✅ **Undo refills the cart** (`113114c`). After a confirmed undo, the same screen reopens
  with the job, location and items.
- ✅ **Smarter search** (`113114c`). One `searchItems()` for all 8 boxes: words in any order,
  size and category, typo fallback, ranked.
- ✅ Found on the way: Adjust Stock 404'd on app-made locations (`7759b7e`).

### R5-A — Count a truck (PLAN, needs the owner's OK)

**Goal:** stand at a truck (or shop), go down a list, type what's really there, and fix stock
in one go.

**Screen:** home → Stock & transfers → **📋 Count Stock**.
1. Pick the location (defaults to "Working from").
2. List every item that location has stock of, plus any item with a reorder minimum there,
   grouped by category, each with a blank **Counted** box. "Show all items" adds the rest.
   Search box at the top (the new search).
3. Only rows you type into count. Blank means "didn't count it", **never "zero"**. A real zero
   is typed as 0.
4. **Review:** a list of only the differences ("12 RED THHN: system 40 lb → counted 32 lb, −8"),
   then **Apply**.

**Server: no new stock logic.** Apply calls the existing `bulkApply {type:"counts"}`. It
re-reads on-hand at apply time and posts the difference as an Adjustment, so material logged
mid-count isn't undone. Notes say "Truck count", and entered_by is stamped from the session.

**Owner decisions:**
- **Who can apply?** Today Adjust and Bulk Upload are admin-only. Options: (a) admin only;
  (b) anyone counts, an admin reviews and applies (needs a saved "pending count", so more work);
  (c) anyone applies. Recommendation: **(a) first**, add (b) if the crew should count their own trucks.
- **Wire by weight:** count in lb, shown with ft, the same as the stock screens.

**Size:** medium (one screen; it reuses the apply path). Tests: apply posts only typed rows,
blank ≠ 0, and the reviewed delta is recomputed at apply.

### R5-B — Work without signal (PLAN, needs the owner's OK)

**Goal:** in a basement with no signal, Use on Job and Returns still work. The cart is held
on the phone and sends itself when signal returns.

**What makes it safe already:** every cart carries a `submit_id` and the server ignores a
repeat (`ON CONFLICT`), so retrying the same cart any number of times can't double-log.

**Change:**
1. **Start without signal.** Today the app needs the network to load items, jobs and
   locations. It will keep the last loaded lists on the phone (items ~878, jobs, locations)
   and use them when offline, with a banner: "Offline — showing lists from 8:14 AM".
2. **Submit offline.** If Submit can't reach the server (no response at all, not an error
   answer), the cart and its `submit_id` go into a queue on the phone. The bar reads
   "📶 Saved on this phone — will send when you have signal (1 waiting)".
3. **Send later.** The app retries when signal returns (`online` event), on app open, and
   every 30 s while anything waits. Each send is the same `submit_id`, so it's safe.
   On success: "✓ 1 saved cart sent to <job>".
4. **A queued cart that the server refuses** (e.g. a retired item) stays in the queue,
   shown in red with the reason and Edit / Discard. It is never silently dropped.
5. **Scope:** cart submits only (Use, Return, Return to Shop). Receive, Transfer, Adjust and
   Orders stay online-only, with a clear "needs signal" message.

**Owner decisions:**
- **Date on a late-sent cart:** the server stamps the time it ARRIVES. Should the ledger
  carry the time it was LOGGED on the phone instead? Recommendation: send the phone's time
  and use it if it's within the last 7 days (otherwise the server time). It matters only for
  reports by date.
- **Undo for a queued cart:** not offered until it's sent. Before that, Discard removes it
  from the queue.
- **Stock shown offline** is as of the last load. It's labelled, not hidden.

**Risks:** a phone that's never reopened keeps its queue, so the home screen and the Undo bar
show "N carts waiting to send" until they go. Two phones can't conflict (each cart has its
own id). Logging out with carts waiting asks first.

**Size:** large-ish (app only, no server change except the optional phone timestamp).
Tests: queue/retry with the same id, a refusal stays visible, and the offline lists load.

## Later — planned, not scheduled

| # | Idea | What it touches | Size |
|---|------|-----------------|------|
| 4 | **Most-used actions first.** Move Check Stock and Order Materials up; put Wire Scrap, Print Labels and Adjust under a "More" section. | home HTML order only | small |
| 5 | **Clearer returns.** Plain-word subtitles, e.g. "Return Items: put shop stock back on the shelf" and "Return to Shop: leftovers bought for a job → shop stock + job credit". Merging them into one button is the bigger version. | home text; merge = log screen | small / medium |
| 6 | **Confirm + Undo.** "✓ 3 items logged to Bethel" with Undo for ~2 minutes. ⚠ Undo needs a **backend reversal** of the ledger rows and any pending push, so it's not a UI-only change and needs its own plan. | `inventory.js` + ledger | large |
| 7 | **Faster entry.** +/− steppers on quantity and a "your recent items" list on the item search. | log screen | small / medium |
| 8 | **Admin "needs attention" strip.** *$X waiting to push · N new part numbers · N open orders · N low.* Open orders and reorder counts already load. Push total and new-part-number count may need a cheap count action each. | home + maybe 2 small reads | medium |
| 9 | **Split Admin** into *Daily* (Push Expenses, New Part Numbers) and *Setup* (Add Item, Bulk Upload, Shops & Trucks, Update Price, Wire Calculator). | home HTML | small |
| 10 | **Estimates as a top-level button**, not an admin setting. | home HTML | small |
| 11 | **Edit price on the item itself** (fold Update Item Price and the Wire Calculator into the Check Stock result for admins). | Check Stock screen | medium |

Suggested next round after 1–3: **4, 5 (subtitles), 9, 10**, all of which are rearranging
and wording. Then **8**. **6** gets its own plan.
