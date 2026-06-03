# Northeastern Electric — Field Operations App

Field-operations app for an electrical contractor: jobs, estimates, invoices,
time/payroll, fleet, generators/warranties, inspections, scheduling, and
materials/inventory. Airtable is the database; there is no other server or ORM.

This is **not** a built project — no build step, no bundler, no root `package.json`.
The tracked repo is a handful of files served as-is by Netlify.

## Layout

```
index.html              Main field-app SPA (vanilla JS)        → /
inventory.html          Materials/inventory + estimating SPA   → /inventory
sw.js                   Service worker (must stay at root)
netlify.toml            Routing, redirects, publish config
netlify/functions/      Airtable proxy functions
  airtable.js             main field-app proxy
  inventory.js            inventory/estimating proxy (spans two bases)
  package.json            sets "type": "module" for the functions
assets/                 Logo source files (regeneration only — not loaded at runtime)
docs/                   Internal docs (not published)
  SYSTEM-MAP.html         visual whole-system architecture map
  SYSTEM-MAP.md           agent-readable twin of the map
  TODO.md                 open work / known traps
tests/                  Smoke matrix + backend handler tests
CLAUDE.md               Authoritative guide for working in this repo
```

## Deploy

Push to `main` — Netlify auto-deploys `main` to production. There is no staging.
The repo root is the publish directory, so every committed root file is served
publicly. Internal docs live in `docs/` and are kept out of the deploy via
`.gitignore`.

## Local preview

```
netlify dev
```

Copy `.env.example` → `.env` and fill in the required variables (see CLAUDE.md →
"Required environment variables"). For local writes, point the base IDs at a
duplicated sandbox base so expense/payroll/invoice paths never touch production.

## Tests

```
node tests/handlers.test.mjs   # offline backend-handler tests (the only automated check)
```

Run a manual browser smoke pass before every push — see `tests/SMOKE.md`.

## More

See **CLAUDE.md** for architecture, Airtable conventions, authentication/roles,
and the gotchas worth knowing before editing the large files.
