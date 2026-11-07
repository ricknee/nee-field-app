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
