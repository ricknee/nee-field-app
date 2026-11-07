# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Northeastern Electric (NEE) field-operations app for an electrical contractor: jobs,
estimates, invoices, time/payroll, fleet, generators/warranties, inspections, scheduling,
and materials/inventory. It is **not** a built project. The tracked repo holds two large static
HTML single-page apps served as-is, Netlify Functions, a service worker, and `netlify.toml`.

**Neon Postgres is the only database.** The app has nothing to do with Airtable any more (owner,
2026-11-07). Don't read, query, plan around, or suggest checking Airtable. If something seems to
need Airtable data, that is a Neon gap to close. See "Airtable leftovers" below for the dead code
that still carries the name.

`docs/SYSTEM-MAP.html` is a standalone whole-system architecture doc (architecture diagram, ERD,
integration map, "Where it hurts" trap list) that you can open directly. After structural changes,
keep it in sync with the `/system-map` skill.

## Commands, build, and tests

There is **no linter and no root `package.json`.** Don't look for one.

There is **one build command**, and only one: `netlify.toml` runs
`npm install --prefix netlify/functions --omit=dev`. It compiles nothing. It exists only so the
functions' declared dependencies get installed on the Netlify build machine.
`netlify/functions/package.json` sets `"type": "module"` and declares
`@neondatabase/serverless`. If that install command is ever removed, the function fails at runtime
with `Cannot find package … imported from /var/task/netlify/functions/airtable.js`, because
`netlify/functions/node_modules` is gitignored and never ships.

- **Deploy:** push to `main`. Netlify auto-deploys `main` to production. There is no staging.
  Before committing anything described as "push to live", confirm `git branch --show-current`
  is `main`, and say so if it isn't. Don't silently land on a feature branch.
- **Local preview:** `netlify dev` serves the static root and runs the functions locally.
  Functions need the env vars below (copy `.env.example` → `.env`) or they throw at `ensureEnv()`.
- **Tests** (`tests/`, see `tests/SMOKE.md` for the full matrix):
  - Tier 0 **smoke**: a manual ~5-min browser pass before every push (login, job list, expense,
    inventory push, invoice, payroll).
  - Tier 1 **backend handlers**: `node tests/handlers.test.mjs` (offline, mocked). Add a case
    for every bug fixed. This is the only automated check, so run it before pushing.
  - Tiers 2–3: Playwright money-path E2E and a manual domain matrix, run before larger releases.
- **Everything else is verified by hand:** load the app in a browser and smoke-test the affected
  flow. If `node` or lint binaries aren't on `PATH`, don't go hunting for them. Rely on the
  browser smoke test.

### Environment variables (set in the Netlify dashboard; `.env.example` is the template)

- `DATABASE_URL`: Neon Postgres connection string. Neon project `damp-silence-99074350` (PG17).
  Every read and write goes here.
- `AUTH_SECRET`: HMAC key for signing and verifying session tokens (`_auth.js`, shared by both
  functions). **Required, and auth fails closed:** if it's unset, `ensureEnv()` throws and every
  request gets a 401. Local `netlify dev` and `node tests/handlers.test.mjs` need it too.
- `GOOGLE_MAPS_API_KEY`: distance lookups in `handleCalculateMileage`.
- `ADMIN_BACKFILL_TOKEN`: gates one-off admin actions.
- `R2_ACCOUNT_ID` / `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` / `R2_BUCKET`: **optional as a
  group, fail soft.** This is the Cloudflare R2 store for photos (`_r2.js`,
  `docs/PLAN-job-photos.md`). If they're unset, the Photos buttons are disabled. The browser
  uploads and downloads through **presigned URLs**, so photo bytes never pass through the
  function. The bucket also needs a **CORS policy** that allows `PUT`/`GET` from
  `https://hub.northeasternelec.com`. Diagnose with `GET ?action=r2Status`.
- `TIME_CLOCK` / `TIME_CLOCK_PAYROLL`: **the in-app time clock's two kill switches.**
  **Production today: `TIME_CLOCK=on`, `TIME_CLOCK_PAYROLL` UNSET.** The crew punches, and
  **nothing they punch counts**. QuickBooks Time is still the book of record. When
  `TIME_CLOCK_PAYROLL` is off, punches land only in Neon's `clock_punches` ledger, which nothing
  else reads. Flipping it is the **owner's call**. Before that, work through the checklist at the
  top of `docs/PLAN-time-clock.md`, which covers a `promoteClockPunches` dry run and the
  hardcoded `SALARIED` name list. Rollback: `DELETE FROM time_entries WHERE source = 'Clock'`.
- `GENERATOR_SERVICE_CALLS`: **the generator service-call kill switch. Unset by default, which
  means inert.** `on` lets the hourly `qb-time-pull` open a service-call job for every generator
  whose plan has come due. `dry` reports what it would create and writes nothing. **Every job
  created burns a PO number that can't be handed back**, so preview first with
  `POST { action:"generatorServiceCheck", dryRun:true }`. See `_generator-service.js`.
- `GOOGLE_CONTACTS` + `GOOGLE_OAUTH_CLIENT_ID`/`_SECRET` + `GOOGLE_REFRESH_TOKEN_1` (`rick@`) /
  `_2` (`nee@`): **the Neon → Google contacts sync. Optional, fails soft, inert by default.**
  `dry` reports what it would do; `on` is live.
  ⚠⚠ Run `dry` first and read it. Most contacts already exist in **both** accounts, and the
  stored `contacts.google_person_id_1/2` ids are the only thing stopping a cold start from
  creating hundreds of duplicates. ⛔ **There is no delete path, and there must not be one.**
  ⚠⚠ The consent screen must be `Internal`: External/Testing tokens expire after 7 days.
  ⚠⚠ Transposed tokens write every contact to the wrong book, so check `GET ?action=googleStatus`,
  which refuses with `reason:"swapped-tokens"`. See `_google-contacts.js`,
  `docs/PLAN-google-contacts.md`.
- `PCLOUD_*`: **unused.** `_pcloud.js` and `tools/pcloud-*.mjs` are on disk but nothing imports
  them. pCloud is reached only through Make.com (see below).
- **Airtable-era vars** (`AIRTABLE_API_KEY`, `AIRTABLE_BASE_ID`, `AIRTABLE_WRITES`,
  `JOB_CREATE_SOURCE`, `INVENTORY_BASE_ID`, `LOGIN_SOURCE`): see "Airtable leftovers". Some of
  them still have to stay set.

## Architecture

### Two frontends (static, vanilla JS, no framework, no bundler)

- **`index.html`** (~35k lines): the main field app SPA. It talks to
  `/.netlify/functions/airtable`. The function name is historical; the function is Neon-backed.
- **`inventory.html`** (~11k lines): the materials/inventory + estimating SPA. It talks to
  `/.netlify/functions/inventory`.

Both apps are hand-written vanilla JS with a single `state` object and `render*()` functions that
rebuild the DOM from state. They share job context through common `localStorage` keys
(`nee_last_job`, `nee_last_job_id`), so a user can hand off from one app to the other.

Frontend conventions to know before editing `index.html`:
- `apiGet(action, params)` / `apiPost(action, body)` are the only ways the client talks to the
  backend; `const API = "/.netlify/functions/airtable"`.
- Session state lives only on the client: the logged-in user object and token are persisted in
  `localStorage` (`STORAGE_KEY` in index, `SESSION_KEY`/expiry in inventory). There is no
  session cookie.
- A **synthetic history back-stack** (`pushBackEntry`/`popBackEntry`, `window._neeBack`) makes
  the mobile back button close modals and job views instead of leaving the page. When you add a
  modal or full-screen view, push and pop a back entry.
- **jsPDF is lazy-loaded from a CDN** only when a PDF is built, so guard on `window.jspdf`.
- **pCloud uploads bypass the functions.** Invoice, estimate, and service-report PDFs go as
  base64 straight from the browser to a **Make.com webhook** (`MAKE_PCLOUD_UPLOAD_WEBHOOK` in
  `index.html`).
- `sw.js` caches non-HTML assets cache-first, so data tables inlined in the HTML ship with the
  document. `/shared/` is network-first.

### Netlify Functions (`netlify/functions/`)

- **`airtable.js`** (~17.6k lines): the field app's backend, **Neon-backed despite the name**.
  One `handler` (~line 17307) dispatches on an `action` string: GET reads
  `event.queryStringParameters.action`, and POST reads `JSON.parse(event.body).action`. The
  dispatcher is a flat `if (action === …)` chain at the bottom of the file. **To add an
  endpoint, write a `handleX` function, register it in that chain, and choose its role tier in
  `authzFor`.** Unknown actions return 400.
- **`inventory.js`** (~4k lines): the same dispatch shape, for the inventory/estimating app.
  It has no Airtable client at all. `tests/handlers.test.mjs` asserts that statically.
- **`qb-time-pull.js`**: the hourly scheduled function, **QuickBooks Time → Neon**. QB Time
  stays the source of truth for hours (owner's instruction). Each run writes
  `sync_state('hourly_pull')`, which is the liveness signal. The same run executes
  `_integrity.js` and the generator service check.
- **`_integrity.js`**: SELECT-only consistency checks, also available on demand through
  `GET ?action=integrityCheck`. It exists because this system **fails by matching nothing**,
  and that looks the same as "no data".
- Domain helpers: `_neon.js` (driver, lazy-imported so the tests stay offline), `_auth.js`,
  `_jobs.js`, `_employees.js`, `_expenses.js`, `_allocations.js`, `_r2.js`, `_vendor-*.js`,
  `_ticket-lines.js`, `_order-checklists.js`, `_google-contacts.js`, `_generator-service.js`.
- Schema lives in `db/schema/NNN_*.sql`, applied in order. ⚠ The view definitions there can be
  **stale**, so read live definitions with `pg_get_viewdef`.

### Routing & PWA

- `netlify.toml`: `/api/*` → `/.netlify/functions/:splat` (a 200 rewrite) and `/materials*` →
  `/inventory` (a 301). The publish dir is the repo root, so redirects need `force = true`.
- `sw.js`: network-first for the HTML document, **network-only for `/.netlify/` calls**, and
  cache-first for other assets.

### Authentication & roles

Login (`neonLoginCandidate` in `_employees.js`) matches the submitted identifier (full name,
first name, username, or email) plus PIN against Neon's `employees` table, requiring the employee
to be active and to have a PIN. If Neon is unreachable, the answer is a **503**. There is no
fallback. On success it issues a **signed session token** and returns `{ id, name, role, token }`.
Roles are normalized to `admin`, `office`, `viewer`, and `employee`. `office` behaves like admin
in the field app but is filtered out of inventory and crew pickers. "Strict admin" means
`role === "admin"`.

Tokens are stateless **HMAC-SHA256** over `base64url(JSON{id,role,iat,exp})` with a 30-day TTL.
The client sends `Authorization: Bearer <token>`. The function returns **401** for a
missing/forged/expired token and **403** for a role violation. A token from either function
validates in both.

Role policy lives in `authzFor(method, action)` (~airtable.js:800). It returns an allowed-role
array, or `null` for any signed-in role. The tiers are `_PAYROLL` (admin+employee), `_ADMIN`
(admin only: payroll runs, scheduling, dev ops), `_ADMIN_OFFICE` (back-office money ops), and
`_NON_VIEWER`, the default for all other writes. When you add a write action, decide its tier or
it falls to non-viewer.

## Rules that cost money when broken

- **A PO number can't be handed back.** Anything that creates a job burns one permanently.
- **Jobs are born in Neon** (`createJobNative` in `_jobs.js`). The app is the only source of the
  `Job PO` string and of `markup_pct`. A NULL markup bills material **at cost, permanently**,
  because allocations snapshot it.
- **Billing allocations must write `bill_rate`.** A NULL rate values those hours at **$0** while
  they still print.
- **Money needs more than 2 decimal places** in intermediate math.
- **Ids:** older rows carry a legacy `airtable_id` (`rec…`) as their stable external handle.
  R2 keys, tokens and stored ids all speak it. Emit `COALESCE(airtable_id, id::text)`, and never
  rewrite or re-stamp an existing `airtable_id`. Rows born natively have none, so **a query that
  matches only on `airtable_id` silently drops native rows**. Grep every WHERE clause for it.
- **Deploying is not evidence.** Re-query production after the next real run.

## Airtable leftovers (dead code, not a dependency)

Airtable is gone. The names and a few code paths remain, and these facts keep them harmless:

- **`AIRTABLE_API_KEY` / `AIRTABLE_BASE_ID` are no longer required (2026-11-07)** and can be
  deleted from Netlify. Without a key, every `atFetch` (all copies) **skips writes** and
  **refuses reads** with `AirtableRetiredError`, whatever `AIRTABLE_WRITES` says
  (`_airtable-write-guard.js`). Every live code path is Neon-only. The two "no key" cases at the
  end of `tests/handlers.test.mjs` fail if anything contacts Airtable.
- **`JOB_CREATE_SOURCE=native` must stay set.** `_jobs.js` refuses to create a job otherwise.
  With no key, `AIRTABLE_WRITES` no longer matters.
- Reads still in the code sit behind `if (neonEnabled())` blocks that always return, so they run
  only when there's no database at all. A database outage answers **503**, never a fallback.
  Helpers signal that with `unavailableError()`, which the top-level catch maps to 503.
- The one-off Airtable → R2/Neon copy tools (`copy*ToR2`, `backfillContacts`) are finished.
  Without a key they refuse.
- **`_jobs-sync.js` and `_billing-sync.js` are RETIRED. Never wire them back.** They would not
  resume syncing; they would **overwrite** every app edit with stale data.
- `INVENTORY_BASE_ID` and `LOGIN_SOURCE` are read by nothing, so delete them from the dashboard.
- ⛔ **Never add an Airtable call, read, fallback, or mirror.** Removing the remaining dead
  branches (`atFetch`, `TABLES`/`F` maps, unreachable fallbacks) is welcome cleanup work.
  Run `node tests/handlers.test.mjs` after each removal.

## Working with the large files

`index.html` and `airtable.js` are very large single files. Prefer targeted `Grep` and ranged
`Read` (offset/limit) over reading them whole. Handlers in `airtable.js` are named `handleX` and
are easy to grep. Frontend logic clusters under `render*`/`apiGet`/`apiPost`. Business-rule
nuances are documented in dense inline comments near the relevant code, so trust those comments.
Running order for new work: `docs/ROADMAP.md`.
