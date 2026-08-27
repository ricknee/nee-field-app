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
- `JOB_CREATE_SOURCE` — **where a job is born. Three values, and the third is the identity
  cutover's last switch.** `unset`/`airtable` = Airtable creates the job *and* assigns its PO
  number. **`neon` = production today**: Airtable still creates the job, Neon's `job_po_counters`
  assigns the PO (audit item 05, `db/schema/039`). **`native`** = the job is **born in Neon** and
  Airtable gets a fail-soft mirror it never reads back (cutover slice 6, `db/schema/061`).
  It is a switch rather than a code path because **a PO number cannot be handed back** — every
  attempt at a native create burns one permanently — and because jobs are the spine: every
  expense, photo, estimate, invoice, panel and schedule entry hangs off a job id.
  ⚠ `native` also makes the app the **only** source of two values Airtable used to compute: the
  `Job PO` string and `markup_pct` (a NULL markup bills material at **cost**, permanently, because
  allocations snapshot it). Both are reproduced in `_jobs.js`.
  ⚠⚠ **Never stamp the Airtable rec id back onto a native job.** This mattered because
  `_jobs-sync.js` upserted `ON CONFLICT (airtable_id)` **hourly** across 38 columns, so a stamped
  id would have let Airtable overwrite everything the app wrote, every hour, silently.
  **That sync was RETIRED 2026-08-25** — it is no longer called and must not be wired back (see
  its header) — so the rule now stands for a different reason: `airtable_id` is the handle every
  R2 key, token and stored id already speaks, and changing it strands all of them.
  ⚠⚠ **…but its Airtable MIRROR is not invisible to that sync, and that was a live bug** (fixed
  2026-08-24, `db/schema/062`). The mirror is a real Airtable record with a rec id no Neon row
  carries, so the hourly sync took the INSERT branch and re-imported it as a **second job** — same
  name, same PO, frozen at "New Lead", listed right beside the real one. `createJobNative` now
  records the mirror's id in **`jobs.airtable_mirror_id`** and `syncJobs` skips it. That column is a
  skip list and nothing else: never resolve or emit through it, and never copy it into
  `airtable_id`. **The general rule: "we never stamp the id back" protects the row we wrote — it
  says nothing about the row we caused to exist in Airtable. For any un-stamped mirror, ask what
  reads that table wholesale.**
- `AIRTABLE_WRITES` — **PRODUCTION TODAY: `off`. The app writes NOTHING to Airtable.**
  Unset or `on` = mirrors are written.
  `off` = every POST/PATCH/PUT/DELETE through any `atFetch` is skipped, and resolves to
  `{ id: null, fields: {}, skipped: true }` rather than null, because ~65 call sites read `data?.id`
  or `created.id`. One env var moves all of them; editing 40 handlers would be 40 chances to miss one.
  ⚠ **Safe only because every remaining Airtable write is a MIRROR** — verified 2026-08-25 by call
  graph, not grep (the grep pass produced four false positives: `handleAddLiftExpense` and
  `handleAddGeneralExpense` write Neon through an imported helper and 502 if it fails). Billing
  allocations were the one genuine Airtable-**first** writer and went Neon-native the same day.
  Before flipping, re-ask that question of anything added since: **is Neon written first, and does
  the caller consume the response?**
  ⚠⚠ `createJobRecord`'s non-native branch POSTs a job and then **re-reads its own write** for
  Airtable's computed `Job PO`. `AIRTABLE_WRITES=off` with `JOB_CREATE_SOURCE` ≠ `native` is refused
  in `_jobs.js` — before the PO is allocated, because a PO cannot be handed back.
- `GOOGLE_CONTACTS` / `GOOGLE_SA_KEY` / `GOOGLE_CONTACTS_DEST_1` / `_DEST_2` — **the Google
  contact sync (audit item 07). Optional as a group, fails soft, ships INERT.** Neon → Google
  People API, replacing five retired Make scenarios. `GOOGLE_CONTACTS` unset = the sync does
  nothing; `dry` = reports what it would write and writes nothing; `on` = live.
  ⚠⚠ **Run `dry` first and read it.** 230 of 240 contacts already exist in **both** accounts, and
  `contacts.google_person_id_1/2` are the only thing stopping a cold start creating **230
  duplicates twice over** in address books live on people’s phones. Id-first always; a row with a
  stored id is an update, never a create.
  ⛔ **There is no delete path and there must not be** — owner’s call 2026-08-27: both accounts
  stay and `nee@` is becoming the office address book. Cross-account copies are **two address
  books, not duplicates**; clearing one would empty the book staff depend on. Genuine
  within-account dupes belong to Google’s own Merge & fix.
  **Auth is OAuth refresh tokens** (`GOOGLE_OAUTH_CLIENT_ID`/`_SECRET`, `GOOGLE_REFRESH_TOKEN_1`
  for `rick@` and `_2` for `nee@`), scope `https://www.googleapis.com/auth/contacts`. A
  service-account + domain-wide-delegation route is also implemented and picked up automatically
  from `GOOGLE_SA_KEY`, but it is **blocked in this org** —
  `iam.disableServiceAccountKeyCreation` is enforced by Google's Secure by Default and the owner
  has Cloud IAM on the project only. OAuth is the better credential regardless: it never produces a
  long-lived downloadable key. Both routes use `node:crypto` / a plain form POST, so there is **no
  npm dependency** and the test suite stays offline.
  ⚠⚠ **The consent screen must be `Internal`.** External + Testing refresh tokens **expire after
  7 days** — the sync would run for a week then stop silently.
  ⚠⚠ **`GOOGLE_REFRESH_TOKEN_1`/`_2` transposed is a silent catastrophe:** nothing in a refresh
  token says whose it is, so a swapped pair writes every contact to the wrong address book and
  files its id in the wrong column. `googleStatus` asks Google who each token belongs to and
  refuses with `reason:"swapped-tokens"` — **reaching Google is not evidence the wiring is right.**
  ⚠ Both destinations must have a token or `googleConfigured()` is false: half-configured is worse
  than unconfigured, because the reconcile would report a clean pass over an account it never
  reached. Diagnose with
  `GET ?action=googleStatus`; `GET ?action=googleContactsReconcile` is a **read-only** audit of
  whether the stored ids still resolve (`&deep=1` also looks for within-account duplicates).
  See `netlify/functions/_google-contacts.js` and `docs/PLAN-google-contacts.md`.
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

### Netlify Functions (`netlify/functions/`)

⚠ Two modules added 2026-08-25 sit in front of everything else and are worth knowing first:
**`_airtable-write-guard.js`** — the single choke point every Airtable write passes through. Holds
`AIRTABLE_WRITES` (the kill switch) and strips uuids out of linked-record fields, because
`typecast: true` does not reject an unknown link value, it **CREATES the record**.
**`_integrity.js`** — nine SELECT-only checks run at the end of every hourly pull, and on demand via
`GET ?action=integrityCheck`. It exists because eleven defects were found by hand in one day and
**not one of them threw**: this system fails by matching nothing, which reads as "no data".

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
  the `getExpenseFields` schema debug action, and one fail-soft mirror in `handlePushExpenses`.
  ⚠ **That mirror no longer runs: `AIRTABLE_WRITES=off` since 2026-08-25.** Its old justification
  — "that table is still a Make trigger bus" — was **measured false** the same day: of the 20 Make
  scenarios in the only team, every one using the `airtable` package is `isActive: false`.
  The code stays, inert, and still carries its two rules for whenever anyone reads it: it never
  stamps the rec id back (R2 receipt keys are `expenses/<handle>/`, so a handle that flips orphans
  every receipt) and never feeds the mirror response to `syncExpenseToNeon` (its
  `ON CONFLICT (airtable_id)` cannot fire on a NULL, so it would insert a **second** expense for
  the same spend).

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

## 🔴 WHERE THIS STANDS — 2026-08-25 (read this before planning anything)

**The app no longer reads or writes Airtable in normal operation. Neon is the system of record.**

- `AIRTABLE_WRITES=off` in production. All ~65 mirror writes are skipped at the `atFetch` choke
  point. Verify with `GET ?action=jobCreateStatus` → `airtableWrites.enabled`.
- **Both hourly pulls are retired** — `syncJobs` (38 job columns) and `syncBillingTables`. Their
  modules carry a RETIRED banner. ⚠⚠ Re-enabling one would not "resume syncing", it would
  **OVERWRITE**: every app edit since the cut is newer than Airtable's copy.
- **39 read handlers now REFUSE (503) rather than fall back**, because Airtable is frozen and a
  fallback answers with yesterday's world, silently. Four fallbacks remain on purpose:
  `handleLogin` (governed by `LOGIN_SOURCE`; the failure mode is nobody can work — **owner's call**),
  and three value-returning helpers: `guardExpenseMutation`, `computePayrollDateRanges`,
  `handlePayrollBonusesRollup`.
- **Billing allocations are Neon-native for every row**, not just native ones (2026-08-25). The
  Airtable-first fork is gone and `bill_rate` is written on every allocation — nothing else fills
  it now, and a NULL rate values those hours at **$0 while they still print**.
- The hourly `qb-time-pull` still runs and is untouched: **QuickBooks Time → Neon → the app**.
  QB Time remains the source of truth for hours by owner's instruction. It writes
  `sync_state('hourly_pull')` every run — that is the liveness signal now that `jobs.synced_at`
  no longer moves.

**What is left of the Airtable exit:** the four fallbacks above, deleting the now-unreachable
Airtable read branches, then PAT read-only for a week and archive the base. After that: Google
contacts (item 07), then prevailing wage. Running order stays `docs/AUDIT-airtable-remaining.md`.

**⚠ The lesson that cost the most on 2026-08-25:** eleven defects were found by hand in one day and
**not one of them threw**. A native row does not crash a query, it *matches nothing*, which is
indistinguishable from "there is no data". Deploying is not evidence — **re-query production after
the next real run.** `_integrity.js` exists to break that silence; five static guards in
`tests/handlers.test.mjs` exist to stop the specific spellings coming back.

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
