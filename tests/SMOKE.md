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

## Tier 2 — Money-path E2E (Playwright, against `netlify dev` + test base)

- [ ] Invoice builder: Contract %-progress and T&M modes both produce correct Snapshot Total.
- [ ] Estimate builder: save → correct next number; PDF + pCloud upload toast succeeds.
- [ ] Payroll: hours rollups match the entries; bonuses attach; supersede chain correct.
- [ ] Expense push: pushing twice does **not** create duplicate expenses (idempotency).

## Tier 3 — Manual domain matrix (before larger releases)

- [ ] **Roles:** admin / office / viewer / employee each see only their intended tabs/buttons.
- [ ] Estimates, Inspections (+ inspector picker), Generator commissioning + warranties.
- [ ] Scheduling (add/edit/delete, conflict banner), Fleet (mileage + service), Scissor lifts.
- [ ] Inventory: receive, transfer, adjust, reorder alerts, estimate templates, orders.

---

## Known traps a regression suite MUST guard (see docs/SYSTEM-MAP.html → "Where it hurts")

- [ ] **Cross-job substring filter:** a job named e.g. `Jenny Ln 1` must NOT pull records from `Jenny Ln 10/11/12`. Verify the in-memory record-ID check, not just the `FIND` prefilter.
- [ ] **Expense-push idempotency:** a re-push of the same materials must NOT create a second expense. Now guarded by a stable per-group **Push ID** (stamped on the Expense, the source transactions, and the Expense Pushes header) plus a server-side re-read of `Expense Created?` and per-group marking. Automated coverage: `node tests/inventory-push.test.mjs` (guard #1 same-key retry, guard #2 stale-snapshot). Smoke: push a job, then push the same job again from a second tab → second push reports "already pushed"/"out of date", no duplicate expense lands on the job.
- [ ] **FILTERED vs UNFILTERED rollups:** `mapJob` GP/revenue must read the *filtered* Airtable rollups (Sent/Approved/Archived), not the unfiltered twins.
- [ ] **Two-base Job-Name join:** renaming a job must not silently drop it from `pendingExpenses` — unmatched jobs should surface, not vanish.
- [ ] **Linked-record write shape:** writes use `["rec…"]` string arrays, not `[{id:"rec…"}]` (the object shape has silently dropped writes).
- [ ] **`Active=TRUE` sync fields:** never write `Google Contact ID`, `Sync Status`, `Last Synced At`, `Needs Sync to Google` — Make owns them.
