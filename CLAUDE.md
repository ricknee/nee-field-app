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

There is **no linter and no root `package.json`.** Do not look for one.

There is **one build command**, and only one: `netlify.toml` runs
`npm install --prefix netlify/functions --omit=dev`. It compiles nothing — it exists solely
so the functions' declared dependencies are actually installed on the Netlify build machine.
`netlify/functions/package.json` sets `"type": "module"` **and** declares
`@neondatabase/serverless`, which the Neon read path imports at runtime.

**Adding a dependency to `netlify/functions/package.json` is not enough on its own** if that
command is ever removed. Without it Netlify runs no install at all, `netlify/functions/
node_modules` is gitignored so it never ships, and the function fails at runtime with
`Cannot find package … imported from /var/task/netlify/functions/airtable.js`. That failure
is easy to miss because `_neon.js` fails soft: reads fall back to Airtable and return correct
answers, just slowly. It went unnoticed for three days.

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
- `INVENTORY_BASE_ID` — **dead; delete it.** Was the separate inventory base
  (`appfsLJwfow4CepCw`). Nothing has read it since the write cutover (`79b1b56`, 2026-08-12):
  `inventory.js` makes **zero reads and zero writes** to that base and no longer names the
  variable. It is still set in the Netlify dashboard and in `.env.example` only because the base
  itself has not been archived yet. The three inventory test suites also set it, as leftover
  scaffolding. Do not wire anything new to it.
- `GOOGLE_MAPS_API_KEY` — `handleCalculateMileage` distance lookups
- `ADMIN_BACKFILL_TOKEN` — gates the one-off `backfillTimeEntryEmployeeLinks` admin action
- `AUTH_SECRET` — HMAC key for signing/verifying session tokens (`_auth.js`, shared by both
  functions). **Required — auth fails closed:** `ensureEnv()` throws and every request 401s if
  it's unset. Local `netlify dev` and `node tests/handlers.test.mjs` need it too.
- `R2_ACCOUNT_ID` / `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` / `R2_BUCKET` — **optional as a
  group.** Cloudflare R2 store for jobsite photos (`_r2.js`, `docs/PLAN-job-photos.md`).
  **Fails soft like `_neon.js`, not closed** — unset just disables the Photos buttons. The browser
  uploads and downloads via **presigned URLs**, so photo bytes never pass through the function
  (no 4.5 MB payload ceiling, no per-image invocation). `aws4fetch` is lazy-imported, so the test
  suite stays offline. The bucket also needs a **CORS policy** allowing `PUT` from
  `https://hub.northeasternelec.com`, or uploads are refused by the browser before any credential
  is checked — that is bucket config, not env. Diagnose wiring with the admin action
  `GET ?action=r2Status`, which names the specific misconfiguration.
- `PCLOUD_ACCESS_TOKEN` / `PCLOUD_AUTH_TOKEN` / `PCLOUD_API_HOST` — **unused.** `_pcloud.js` and
  `tools/pcloud-*.mjs` are kept on disk but nothing imports them: pCloud's app-registration page
  has been down for months, so no API token can be issued (its native login demands a second factor
  under an undocumented parameter). Make.com still reaches pCloud only because Make registered its
  own app years ago. Don't wire these back up without re-reading `docs/PLAN-job-photos.md`.
- `LOGIN_SOURCE` — **optional; the employees/login migration kill switch.** Unset or
  `airtable` (the default) = Airtable decides a login, with Neon running as a shadow that only
  logs disagreements (`login-shadow` in the function logs). `neon` = Neon decides, falling back
  to Airtable whenever Neon is unreachable, so a database blip can't stop the crew logging in.
  It is an env var rather than a code path because it moves login for **both** apps at once —
  the riskiest switch in the migration — and flipping it back takes seconds with no rebuild.
  Only turn it on once the shadow logs are clean. `_source` on the login response says which
  store actually answered. See `netlify/functions/_employees.js`.
- `TIME_CLOCK` / `TIME_CLOCK_PAYROLL` — **the in-app time clock's two kill switches.**
  **Production today: `TIME_CLOCK=on`, `TIME_CLOCK_PAYROLL` UNSET.** So the crew punches, and
  **nothing they punch counts** — QuickBooks Time is still the book of record and the crew is
  double-entering until cutover. Feature work closed 2026-08-10; the clock has been in daily use
  since. Before flipping `TIME_CLOCK_PAYROLL`, work the checklist at the top of
  `docs/PLAN-time-clock.md` — it includes a `promoteClockPunches` dry run that does not exist yet
  and the hardcoded `SALARIED` name list, which is a live payroll hazard on its own.
  Both default to off, and the clock ships **inert**: unset, `clockStatus` answers
  `enabled:false`, the UI renders nothing and `clockIn`/`clockOut` 403.
  `TIME_CLOCK` = `admin` | `on` controls **who can punch** (admin only, for shaking it out on
  prod; then all payroll-eligible roles). `TIME_CLOCK_PAYROLL` = `on` controls whether a punch
  **becomes payroll hours** — off, punches land only in Neon's `clock_punches` ledger, which
  nothing else reads, so the clock cannot touch payroll by construction.
  They are two vars, not one, because QuickBooks Time keeps running until the cutover: the
  dangerous half isn't letting people punch, it's letting a punch turn into money while QB is
  also being paid from. Rollback is exact — `DELETE FROM time_entries WHERE source = 'Clock'`.
  See `docs/PLAN-time-clock.md` §11 and `db/schema/018_time_clock.sql`.
- `GENERATOR_SERVICE_CALLS` — **the generator service-call kill switch. Ships unset = inert.**
  `on` lets the hourly `qb-time-pull` open a service-call **job** for every generator whose
  service plan has come due; `dry` reports what it would create and writes nothing. Unset, the
  check returns `enabled:false` and touches nothing. It is a switch because **the first run is
  not a normal run**: six generators are overdue with the old Airtable latch already set, so they
  all become eligible at once, and **every job created burns a PO number that cannot be handed
  back**. Preview with the admin action `POST { action:"generatorServiceCheck", dryRun:true }`
  before flipping it. Replaces Airtable automation `wfledvx1A8oVscWla`; see
  `netlify/functions/_generator-service.js` and `db/schema/051_generator_service_calls.sql`.
- `DATABASE_URL` — **optional.** Neon Postgres connection string for the time-entries
  migration. When set, `_neon.js` lets read handlers run a **shadow read** against Neon and
  attach a `_shadow` diff to the response. **Fails soft by contract** (the opposite of
  `_auth.js`): unset/slow/broken Neon must never alter a response, so it is deliberately
  **not** in `ensureEnv()`. The driver is lazy-imported, keeping the test suite offline and
  install-free. See `docs/PLAN-time-entries-neon.md`.

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
- **`inventory.js`** (~3,800 lines) — same dispatch shape, for the inventory/estimating app.
  **59 actions, and Postgres is the only database it has.** Stock, items, locations, vendors,
  pricing, the ledger, push history, reorder points and the whole estimating cluster are all
  Neon-native, and every one of those reads **fails closed** rather than falling back to the
  frozen Airtable copy.
  It used to span two Airtable bases; it no longer touches the inventory base at all — and since
  the identity cutover's slice 4c (2026-08-24) it makes **no authoritative Airtable write at
  all.** The Airtable calls that remain all go to the **main** base and are: eight Neon-first
  reads with an Airtable fallback (login, employees, the four job pickers, the push's job index),
  the `getExpenseFields` schema debug action, and **one fail-soft mirror** — `handlePushExpenses`
  creates its materials/tax expenses in Neon and then best-effort-copies them to main-base
  `Expenses`, kept only because that table is still a Make trigger bus. It never stamps the rec id
  back (R2 receipt keys are `expenses/<handle>/`, so a handle that flips orphans every receipt)
  and never feeds the mirror response to `syncExpenseToNeon` (its `ON CONFLICT (airtable_id)`
  cannot fire on a NULL, so it would insert a **second** expense for the same spend).

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
**Employees table**, requiring `Active` and a non-empty PIN. On success it issues a **signed
session token** (`signToken`) and returns `{ id, name, role, token }`. Roles are normalized to
four values: `admin`, `office`, `viewer`, `employee`. `office` behaves like admin in the field
app but is filtered out of inventory and crew pickers; "strict admin" in the UI means
`role === "admin"` only.

**Server-side authorization (PR #19, `_auth.js`) — this is now enforced, not client-trusted.**
Tokens are stateless **HMAC-SHA256** over `base64url(JSON{id,role,iat,exp})`, 30-day TTL, no
session store. The client attaches `Authorization: Bearer <token>` on every call (persisted in
`localStorage`); the function rejects with **401** on missing/forged/expired tokens and **403**
on role violations, then the client bounces to login. There is **no `AUTH_SECRET` fallback — it
fails closed** (see env list above). `_auth.js` exports `signToken`/`verifyToken`/`tokenFromEvent`/
`authedUser`/`hasRole` and is shared by both functions, so a token from either validates in both.

Role policy lives in `authzFor(method, action)` (~airtable.js:407) returning an allowed-role
array (or `null` = any signed-in role): `_PAYROLL` (admin+employee) for payroll reads/self
time-writes, `_ADMIN` (admin only) for payroll runs, scheduling, and dev/backfill ops,
`_ADMIN_OFFICE` (admin+office) for back-office money ops (delete/approve expense, mark-paid,
billable-rate, createVendor), and `_NON_VIEWER` as the default for all other writes (viewers are
read-only). It's a **conservative first pass** — it does NOT harden the plaintext-PIN compare in
`handleLogin` (a separate pass). When adding a write action, decide its tier in `authzFor` or it
defaults to non-viewer.

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
