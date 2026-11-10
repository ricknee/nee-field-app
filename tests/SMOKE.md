# Smoke & regression checklist — NEE field app

Two layers. **Smoke** (Tier 0) is the fast must-pass-before-every-push set.
**Regression** is the broader matrix below it. Every smoke check is a regression
check; not every regression check is a smoke check.

Run the app locally first (no Netlify credits): `netlify dev` + a **test Airtable
base** (point `AIRTABLE_BASE_ID` / `INVENTORY_BASE_ID` at a sandbox copy so write
paths never touch live data). See `.env.example`.

---

## Tier 0 — Smoke (~5 min, before every push)

- [ ] **Login** with a known PIN → lands on the job list; wrong PIN is rejected.
- [ ] **Job list** loads and is grouped by status; search + contractor filter work.
- [ ] **Open a job** → Project Info renders; tabs switch without errors (console clean).
- [ ] **Create a General Expense** on a job → it appears in the Expenses tab.
- [ ] **Inventory:** log a Use, then **Push Expenses** → a Materials expense lands on the job in the main base.
- [ ] **Save an invoice** (Invoice Builder) → gets a display # and a snapshot; PDF builds.
- [ ] **Save a payroll run** → archives with PDF; prior run marked superseded.
- [ ] **PDF lazy-load (cold):** after a hard reload, the *first* "Generate PDF" click (invoice, estimate, reprint, generator report, or payroll) fetches jsPDF and produces the file — all PDF paths share one `ensureJsPDF()` loader, so a break here breaks every PDF. Blocking the CDN must alert "PDF library failed to load," not hang silently.

## Tier 1 — Backend handlers (automated)

- [ ] `node tests/handlers.test.mjs` → **all pass** (offline, mocked Airtable). Add a case per bug fixed.

## Tier 1.5 — Live SQL, against a Neon **branch** (opt-in, writes)

Tier 1 has no database, so it can pin the *shape* of a statement but never its
arithmetic. That gap is not theoretical: the estimate create shipped broken on
2026-08-22 and failed on the first click in production, because the pre-flight
check used `PREPARE name(text, numeric, …)` — declaring the parameter types and
so resolving the very ambiguity that broke. **The driver sends parameters
untyped.** Only a real call through the real driver proves a statement.

- [ ] `node tests/estimate-gp.live.mjs "<neon-branch-connection-string>"` → **all pass**

⚠ It **creates and deletes estimates on real jobs**. Make a branch first
(`create_branch` in the Neon MCP, or the console) and pass *that* connection
string — never the production one. It cleans up everything it makes.

Run it when touching the estimate money model, `estDerived`, `v_job_rollups`, or
anything that upserts `job_estimates`.

## Tier 2 — Money-path E2E (Playwright, against `netlify dev` + test base)

- [ ] Invoice builder: Contract %-progress and T&M modes both produce correct Snapshot Total.
- [ ] Estimate builder: save → correct next number; PDF + pCloud upload toast succeeds.
- [ ] Payroll: hours rollups match the entries; bonuses attach; supersede chain correct.
- [ ] Expense push: pushing twice does **not** create duplicate expenses (idempotency).

## Tier 3 — Manual domain matrix (before larger releases)

- [ ] **Roles:** admin / office / viewer / employee each see only their intended tabs/buttons.
- [ ] Estimates, Inspections (+ inspector picker), Generator commissioning + warranties.
- [ ] Scheduling (add/edit/delete, conflict banner), Fleet (mileage + service), Scissor lifts.
- [ ] **Job screen streamline** (2026-11-09), as ADMIN on a phone: no Change Status row until you tap
  the status pill ▾; picking a status saves it (toast), and the job moves group in the list. Project Info
  is one compact list (contractor, phone, email, address + miles); no Job Name/PO/Status/Type/Generator
  boxes. "⚙ Job settings · …" is one line; open it and change City tax, Bills as and Sales tax → the ONE
  Unsaved bar saves them and the fold stays open. Prevailing wage still has its own button. Edit → Cancel
  still swaps the panel. A PW job shows "🏛 Prevailing wage" in the header. As an EMPLOYEE/VIEWER: the pill
  does nothing, a viewer still sees Tax Status.
- [ ] **Job page, lower half** (2026-11-09): a job with no note shows no Job Notes card; admin sees
  "📝 + Add job note", which opens Edit on the notes box; save a note and the card appears (an employee
  sees it too). Hours + Completion Date are one joined card; a past date on an unfinished job reads
  "N days OVERDUE" in red. Scissor lifts: one full-width row each (thumbnail, name · who, Hooks/Box ✓✗,
  notes); tap the thumbnail for the big photo; Edit opens the old controls — change Hooks, Save, and the
  row updates. Return to Shop and Move to another job… still work.
  "✓ Due date met" (schema 088): on an OVERDUE job (Trail Cabinet), tap it → "· ✓ Met (date)" in green,
  no OVERDUE; reload and it stays; "undo" brings the OVERDUE back. Status is NOT changed by it.
  Customer name (2026-11-10): a "Customer" line sits above Phone. ✏ Edit → change First/Last Name → the
  one save bar says "project info" → Save → the line updates; reload and it stays. Clearing both shows "—".
  Contractor contact (schema 089): ✏ Edit → "Contractor contact" lists the contractor's people → pick one →
  Save → a "Contractor contact" block with Call/Text shows under Contractor; reload and it stays. "+ New
  contact…" → type a name → Save → it is added under the contractor AND linked. "— none —" removes the
  block. A job with no customer name/phone/email shows no Customer/Phone/Email lines.
- [ ] **Power Co. + Inspections tabs** (2026-11-09): info is one compact list. As admin, "✏ Edit power co.
  info" opens the same form — pick a company, then a contact, fill AIC/Temp/Perm/Meter, Save → the list
  updates. "✏ Edit inspection info" (now BELOW the list) — agency, inspector, permit, and "does not
  require inspections" all save as before. + Add Inspection and the record status buttons unchanged.
  Employee/viewer: no Edit bars.
- [ ] **Quick 🧾 Receipt** (`docs/PLAN-receipt-and-offline-prints.md`), on a phone, as an EMPLOYEE:
  top-bar 🧾 Receipt with no job open and scheduled today. The job is today's job. Enter an amount, no
  photo, Save: "✓ $X on <job>", and My Expenses on that job shows it waiting for approval, dated TODAY
  (try after 7 pm too). Again with 📷 Add photo: the camera opens, the thumbnail shows, and the expense's 🧾
  has the photo. Pick Lowe's: the amber double-charge warning shows. From inside a job, the 🧾 Receipt
  button uses that job. As a viewer: no Receipt buttons.
- [ ] Inventory: receive, transfer, adjust, reorder alerts, estimate templates, orders.
- [ ] **Inventory home** (`docs/PLAN-inventory-usability.md` #1–3):
  - Location line: with no location set it reads "Pick where you're working from". Pick one, and the
    line, the top pill and the From picker on Use on Job all show it. It survives a reload.
  - Continue on: open a job in the field app, switch to inventory, and the Use on Job tile names it.
    Log an item and it lands on that job. With no last job, or an archived one, it shows the plain subtitle.
  - Scan Item, on **Android and iPhone** (different scanner paths): each menu button opens the right
    screen with the item already selected. An employee sees no Adjust. Phone back and Cancel both close
    the menu, and after choosing an action one back press returns home. An unknown barcode offers
    Search instead / Scan again.
  - Needs attention (#8, admin only): the box's $ equals the Push Expenses grand total (include a taxable job). Each item opens its screen. Push something, go home, and the figure drops. An employee sees no box. A failed request hides its item; it never reads 0.
  - Recent items (#7): on Use on Job, add two items to the cart, then go back to the search. A Recent row shows them, newest first, and tapping one selects it. Signed in as someone else on the same device: their own list, not yours.
  - Recent Logs Del: as an employee, there's no Del button anywhere. As an admin, a pushed row shows "✓ pushed" and no Del; an unpushed row still deletes and its stock comes back.
  - Price on Check Stock (#11, admin): look up a normal item, change Unit cost, and Save. The big price updates, the toast shows, and a reload keeps it. A 10× change asks first. On a weighed-wire item, "Price from an invoice $/ft →" opens the Wire Calculator with that wire picked; save, press Back, and the new price shows. An employee sees no price or box.
  - Undo (#6): submit a Use cart. Home shows "✓ N items logged to <job> · Undo". Tap Undo: the toast says it's undone, Recent Logs no longer has those lines, and Check Stock is back where it was. The bar disappears by itself after 2 minutes. As admin, push that job's material inside 2 minutes, then tap Undo: the bar says it's already pushed and nothing changes. Recent Logs still shows your own entries under your name.
  - Count Stock (R5-A), as an EMPLOYEE: Stock & transfers → Count Stock. It lists what's on your truck. Type 3 counts (one matching, one higher, one lower), then Review: only the 2 differences show. Apply, then Check Stock shows the new numbers, and Recent Logs shows "Stock count" by you. Leave mid-count and come back: your counts are still there.
  - Work without signal (R5-B), on a phone: open inventory once with signal. Turn on airplane mode, close the tab, reopen the app: home shows the blue "Offline" notice and Use on Job works. Submit a cart: "No signal — cart saved", and home shows "1 cart saved on this phone". Turn airplane mode off: within ~30 s (or tap Send now) it sends ("✓ 1 saved cart sent"), and Recent Logs shows it once, dated when you logged it.
  - Home order (round 2): Job materials, Look up, Estimates (admin only), Stock & transfers, More, Admin · Daily, Admin · Setup. Signed in as an employee: no Estimates, Adjust or Admin sections, and every other button is still there and opens its screen.

---

## Known traps a regression suite MUST guard (see docs/SYSTEM-MAP.html → "Where it hurts")

- [ ] **Cross-job substring filter:** a job named e.g. `Jenny Ln 1` must NOT pull records from `Jenny Ln 10/11/12`. Verify the in-memory record-ID check, not just the `FIND` prefilter.
- [ ] **Expense-push idempotency:** a re-push of the same materials must NOT create a second expense. Now guarded by a stable per-group **Push ID** (stamped on the Expense, the source transactions, and the Expense Pushes header) plus a server-side re-read of `Expense Created?` and per-group marking. Automated coverage: `node tests/inventory-push.test.mjs` (guard #1 same-key retry, guard #2 stale-snapshot). Smoke: push a job, then push the same job again from a second tab → second push reports "already pushed"/"out of date", no duplicate expense lands on the job.
- [ ] **FILTERED vs UNFILTERED rollups:** `mapJob` GP/revenue must read the *filtered* Airtable rollups (Sent/Approved/Archived), not the unfiltered twins.
- [ ] **Two-base Job-Name join:** renaming a job must not silently drop it from `pendingExpenses` — unmatched jobs should surface, not vanish.
- [ ] **Linked-record write shape:** writes use `["rec…"]` string arrays, not `[{id:"rec…"}]` (the object shape has silently dropped writes).
- [ ] **`Active=TRUE` sync fields:** never write `Google Contact ID`, `Sync Status`, `Last Synced At`, `Needs Sync to Google` — Make owns them.
