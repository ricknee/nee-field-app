# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Northeastern Electric (NEE) field-operations app for an electrical contractor: jobs,
estimates, invoices, time/payroll, fleet, generators/warranties, inspections, scheduling,
and materials/inventory. It is **not** a built project — the tracked repo is a handful of files:
two large static HTML single-page apps served as-is, two Netlify Functions acting as an
Airtable proxy, a service worker, and `netlify.toml`. Airtable is the database; there is no
other server or ORM.

`docs/SYSTEM-MAP.html` is a standalone, openable whole-system architecture doc (architecture diagram,
ERD, integration map, "Where it hurts" trap list). Keep it in sync with the `/system-map`
skill after structural changes.

## Commands, build, and tests

There is **no build step, no linter, and no root `package.json`.** Do not look for one.
`netlify/functions/package.json` exists only to set `"type": "module"` for the functions.

- **Deploy:** push to `main`. Netlify auto-deploys `main` to production. There is no staging.
  Before committing anything described as "push to live", confirm `git branch --show-current`
  is `main` and surface a mismatch rather than silently landing on a feature branch.
- **Local preview:** `netlify dev` (serves the static root and runs the functions locally).
  Functions need the env vars below set in the local environment / `.env` or they throw at
  `ensureEnv()`. Copy `.env.example` → `.env`; for local writes point the base IDs at a
  **duplicated sandbox base** so expense-push/payroll/invoice paths never touch production.
- **Tests** (`tests/`, see `tests/SMOKE.md` for the full matrix):
  - Tier 0 **smoke** — manual ~5-min browser pass before every push (login, job list, expense,
    inventory push, invoice, payroll).
  - Tier 1 **backend handlers** — `node tests/handlers.test.mjs` (offline, mocked Airtable;
    add a case per bug fixed). This is the only automated check; run it before pushing.
  - Tiers 2–3 — Playwright money-path E2E and a manual domain matrix, run before larger releases.
- **Verification is otherwise manual:** load the app in a browser and smoke-test the affected
  flow. Don't go hunting through install locations for `node`/lint binaries if they aren't on
  `PATH` — rely on the browser smoke test.

### Required environment variables (set in Netlify dashboard; `.env.example` is the template)

- `AIRTABLE_API_KEY` — Airtable PAT (used by both live functions)
- `AIRTABLE_BASE_ID` — main NEE base (`appiqWg6SvKcGfMAu`)
- `INVENTORY_BASE_ID` — separate inventory base (`appfsLJwfow4CepCw`; `inventory.js` only)
- `GOOGLE_MAPS_API_KEY` — `handleCalculateMileage` distance lookups
- `ADMIN_BACKFILL_TOKEN` — gates the one-off `backfillTimeEntryEmployeeLinks` admin action

## Architecture

### Two frontends (static, vanilla JS — no framework, no bundler)

- **`index.html`** (~900 KB) — the main field app SPA. Talks to `/.netlify/functions/airtable`.
- **`inventory.html`** (~600 KB) — materials/inventory + estimating SPA. Talks to
  `/.netlify/functions/inventory`.

Both are hand-written vanilla JS with a single `state` object and `render*()` functions that
rebuild DOM from state. They share job context through common `localStorage` keys
(`nee_last_job`, `nee_last_job_id`), so a user can hand off from one app to the other.

Frontend conventions worth knowing before editing `index.html`:
- `apiGet(action, params)` / `apiPost(action, body)` are the only ways the client talks to the
  backend; `const API = "/.netlify/functions/airtable"`.
- Session is client-side only: the logged-in user object is persisted in `localStorage`
  (`STORAGE_KEY` in index, `SESSION_KEY`/expiry in inventory). There is no server session
  cookie — see auth note below.
- A **synthetic history back-stack** (`pushBackEntry`/`popBackEntry`, `window._neeBack`) makes
  the mobile back button close modals/job views instead of leaving the page. When you add a
  modal or full-screen view, push/pop a back entry so back-button behavior stays consistent.
- **jsPDF is lazy-loaded from a CDN** (`cdnjs … jspdf.umd.min.js`) only when a PDF is built;
  guard on `window.jspdf` before use.
- **pCloud uploads bypass the functions entirely.** Invoices, estimates, and service-report
  PDFs are sent as base64 directly from the browser to a **Make.com webhook**
  (`MAKE_PCLOUD_UPLOAD_WEBHOOK` in `index.html`). The functions are not in this path.

### Three Netlify Functions (`netlify/functions/`)

- **`airtable.js`** (~200 KB) — the main proxy for the field app. One `handler` dispatches on
  an `action` string: GET reads `event.queryStringParameters.action`, POST reads
  `JSON.parse(event.body).action`. The dispatcher is a flat `if (action === …)` chain at the
  bottom of the file (~line 3831). **To add an endpoint: write a `handleX` function, then
  register it in that chain.** Unknown actions return 400.
- **`inventory.js`** (~110 KB) — same dispatch shape, for the inventory/estimating app. Unique
  in that it spans **two Airtable bases** (`AIRTABLE_BASE_ID` for jobs/employees,
  `INVENTORY_BASE_ID` for stock/items/orders).

(A third function, `auth.js`, was deleted in `304b86c` — it was dead duplicate handlers using
legacy env-var PINs `EMPLOYEE_PIN`/`ADMIN_PIN`. That Phase-1 PIN model is **not** how the
live app authenticates — see Authentication & roles below.)

### Routing & PWA

- `netlify.toml`: `/api/*` → `/.netlify/functions/:splat` (200 rewrite); `/materials*` →
  `/inventory` (301). Publish dir is repo root; functions dir is `netlify/functions`.
- `sw.js` is the service worker: network-first for the HTML document (so deploys land cleanly),
  **network-only for `/.netlify/` function calls** (never cached), cache-first for other assets.

### Authentication & roles

Login (`handleLogin`) matches the submitted identifier (name/username/email) + PIN against the
**Employees table**, requiring `Active` and a non-empty PIN. It returns
`{ id, name, role }` and **sets no cookie/token** — the functions are stateless per request and
authorization is effectively client-enforced. Roles are normalized to four values: `admin`,
`office`, `viewer`, `employee`. `office` behaves like admin in the field app but is filtered out
of inventory and crew pickers; "strict admin" in the UI means `role === "admin"` only.

## Airtable integration conventions (read before touching `airtable.js`)

- **`TABLES`** maps logical names to either a table name or a `tbl…` ID. **`F`** maps logical
  field keys to **human-readable Airtable field names**, because Airtable returns records keyed
  by field name. `F.*` is for **reads only** — never put a field ID in `F.job`. **Write sites
  use field/record IDs inline** at the call site, not via `F`.
- Core helpers: `atFetch(path, opts)` (auth + error unwrap), `fetchAll(table, opts)` (handles
  `offset` pagination — always use it for full-table reads), `resp(code, body)` (JSON + CORS +
  `no-store`), and the field coercers `g`/`gNum`/`gBool`/`gFormulaBool`.
- **`filterByFormula` safety:** `escapeFormulaString` (~airtable.js:301) is the canonical escaper
  for interpolating user values into a formula string — use it for any new filter. The
  previously-noted ad-hoc strip/escape sites were converged onto it in `df8e6c4`.
- **Cross-job filter trap (recurring bug, STILL OPEN):** filtering linked records with
  `FIND(jobName, ARRAYJOIN({Job}))` matches by substring, so one job name that is a prefix of
  another (e.g. "Jenny Ln 1" vs "Jenny Ln 10/11/12"), or duplicate names, leak across jobs. The
  correct pattern (see `handleGetJobInvoices`) is: use the `FIND` as a loose **prefilter**, then
  **verify the linked record ID in memory**. `docs/TODO.md` lists the four sites still on the unsafe
  pattern.
- **Linked-record write shape:** writes use a bare `["rec…"]` array of record IDs, **not**
  `[{ id: "rec…" }]` — the object shape has silently dropped writes. The two legacy object-shape
  writes were converted in `8907a60`; keep new writes on the string-array shape.
- **Make-owned sync fields — never write them:** `Google Contact ID`, `Sync Status`,
  `Last Synced At`, `Needs Sync to Google` are owned by the Make.com automation layer. App writes
  to them cause sync loops/conflicts.
- **FILTERED vs UNFILTERED rollups:** `mapJob` GP/revenue must read the *filtered* Airtable
  rollups (Sent/Approved/Archived), not their unfiltered twins — the field names are deliberately
  counterintuitive (see the inline notes in `F.job`).
- **Single-select whitelists:** several writes validate against explicit option arrays
  (e.g. `SERVICE_TYPE_OPTS`, `WARRANTY_TYPE_OPTS`) with a safe fallback, so a stray client value
  can't trip Airtable typecast into silently creating a new option. Keep these arrays in sync
  with the table's configured choices.

## Working with the large files

`index.html` and `airtable.js` are very large single files. Prefer targeted `Grep` and ranged
`Read` (offset/limit) over reading them whole. Handlers in `airtable.js` are named `handleX` and
easy to grep; frontend logic clusters under `render*`/`apiGet`/`apiPost`. Field-name and
business-rule nuances are documented in dense inline comments near the relevant code — trust
those comments (especially the Airtable rollup naming notes in `F.job`, which are deliberately
counterintuitive).
