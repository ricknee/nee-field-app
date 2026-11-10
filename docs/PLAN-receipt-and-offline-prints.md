# Plan — 2-tap receipt + prints offline (field app, employee AND admin)

Owner picked these on 2026-11-09 from the phone-ideas list (the rest are saved
for later). **Plan only — nothing built until the owner says go.**

---

## #3 — Receipt in 2 taps

### Today
Job list → open job → My Expenses tab → ➕ New Expense → date, type, vendor,
amount, credit, description, billable → 📎 Attach receipt → pick photo → Save.
About 10 steps, and the job has to be found first.

### Proposed
A **🧾 Receipt** button that's always on screen (job list header and inside a job).

Owner 2026-11-09: **receipts first**, and **the photo must be optional** ("what
if I don't want to take a picture"). So the screen comes first, not the camera:

1. Tap → one short screen (no camera yet):
   - **📷 Add photo** — big button at the top, optional. Tapping it opens the camera
     (it has to start from a tap anyway). Skip it and just save.
   - (no photo → the expense saves with no receipt, same as today's form)
   - **Job** — already filled in: the job you're scheduled on today → else the job
     you last had open → else pick (search box).
   - **Amount** — number keypad opens by itself.
   - **Store** — big chips: last used, Lowe's, Home Depot, CED, Wolff, Other.
   - **Save**
2. Done: "✓ $42.18 on <job>". Type = Materials, billable = on, date = today.

A small "Pick a file instead" link keeps the PC / ScanSnap path. The full form
stays for anything unusual (credits, non-billable, other types).

- **Employee:** lands in their My Expenses, waiting for approval, exactly like today.
- **Admin:** same button; it lands on the job like an admin-entered expense. The
  approval list already shows the receipt thumbnail.

### Built on what exists
`addGeneralExpense` + the receipt upload-after-save (`flushPendingReceipts`,
compressed at 2560/q0.85). No new table, no new endpoint for the online case.
Today's schedule comes from `scheduleEntries`.

### Fix along the way
The current expense form sets today's date with `toISOString().slice(0,10)` —
that's **UTC**, so after ~7 pm an expense is dated **tomorrow**. Use the local date.

### ⚠ Double-charge risk — needs the owner's call
Lowe's, Home Depot, CED, Wolff and Contractor Lighting receipts already arrive
by email → MiniBee → app. If a crew member also snaps the paper receipt, **the
job is charged twice**. Faster entry makes this more likely. Options:
- (a) When one of those stores is picked, warn: "This store's receipts come in by
  email on their own — only add it if it wasn't on the company account."
- (b) Leave those stores off the chips.
- (c) Nothing — admin catches it in approval.

### Offline (optional phase 2)
No signal → keep the photo + details on the phone and send it later, like the
inventory offline carts. Needs a send-once id on the expense (a small schema
change: a unique client id) so a resend can never make a second expense. Photos
are kept in IndexedDB (too big for localStorage).

### Steps
1. Receipt button + camera + quick screen (online only). Local-date fix.
2. Owner smoke test on Android + iPhone.
3. (if wanted) offline queue with the send-once id, proved on a Neon branch.

---

## #4 — Prints offline

### Today
Prints open from a signed link that needs signal; the ⬇ download puts a copy in
the phone's PDF app, one file at a time (iOS blocks the rest). With no signal the
field app itself can't load the job list, so you can't even reach the Prints button.

### Proposed
- In a job's 📐 Prints: **"📥 Keep on this phone"** — saves every print (and spec,
  if wanted) for that job onto the phone in one go, with a progress bar.
- A **"📥 Saved prints"** button that works **with no signal**: the app opens on
  the phone's copy of the job list, says "Offline", and lists the jobs whose
  prints are saved. Tap one → tap a print → it opens.
- **Staying current (the important part):** when there's signal, the app checks the
  saved prints against the real ones. A changed or new sheet ("E-1 Rev C") is
  downloaded by itself, and a removed one is dropped. Each saved job shows
  "Saved Nov 9 · up to date" or "⚠ 2 newer prints — tap to update". **Working off
  an old revision is the risk to design against.**
- **Remove** per job, plus a "space used" line.
- **Employee and admin:** the same — prints are readable by every role already.
  Specs too. ⛔ `_docs/` (material costs) is NEVER saved offline.

### How
- Files go in the browser's Cache storage keyed by the print's **storage key**
  (not the signed URL — that expires in 6 h). R2 already allows GET from the app.
- `sw.js` keeps the app page available offline (it already falls back to cache);
  the job list gets the same "last good copy" treatment as the inventory app.
- No server change expected.

### Risks to check while building
- **iPhone may clear saved data** for a site not used in ~7 days unless the app is
  added to the Home Screen. The screen will say when a saved job is missing.
- Big print sets (50–100 MB) — show the size before saving.
- Opening a saved PDF on Android hands it to the PDF app; on iPhone it opens in
  the viewer. To be confirmed on both phones.

### Steps
1. Save/remove per job + open offline + "Saved prints" list.
2. Auto-update when signal returns + the out-of-date warning.
3. Owner smoke test in airplane mode on Android + iPhone.

---

## Owner answers (2026-11-09)
- Order: **receipts first**.
- Photo: **optional**.
- Double charge: **warn** (amber note when one of the emailed stores is picked).
- Receipt offline: **signal-only first**; save-and-send-later maybe later.

## Status
- ✅ #3 step 1 BUILT 2026-11-09: 🧾 Receipt in the top bar (job guessed: open job →
  today's schedule → last receipt's job) and in a job's button row (that job).
  Stores: last used, Lowe's, Home Depot, CED, Wolff, Other…. Local-date fix on
  every expense form. Also fixed: the schedule sent an EMPTY job id for app-made
  jobs (2 entries). ⬜ Owner smoke test (tests/SMOKE.md).
- ⬜ #4 prints offline — not started. Open question: prints only, or specs too?
