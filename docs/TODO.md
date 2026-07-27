# TODO

## Deferred build: Receipt photos on expenses

Design decided (Cloudflare R2, presigned upload — bytes in object storage, reference in the DB),
build deferred as too big for now. Full plan in `docs/PLAN-expense-receipts.md`. Pick up as a
later slice, ideally alongside the Neon move so `expense_receipts` lands as a real table.

## Sweep: remaining `FIND(name, ARRAYJOIN(...))` substring filters

The cross-job filter bug fixed in commit `03f552a` (handleTimeEntries / handleExpenses) has
the same shape in four other places. Each is vulnerable to substring collisions when one job
name is a prefix of another (e.g. "Jenny Ln 1" matching "Jenny Ln 10/11/12") and to duplicate
exact names. Fix pattern: keep the substring `FIND` as a loose prefilter, then verify the
linked record ID in-memory — same as `handleGetJobInvoices`.

- `netlify/functions/airtable.js:444` — generator-related lookup
- `netlify/functions/airtable.js:522` — Job Inspections (`FIND(jobName, ARRAYJOIN({Job}))`)
- `netlify/functions/airtable.js:546` — Job Estimates (`FIND(jobName, ARRAYJOIN({Job}))`)
- `netlify/functions/airtable.js:744` — Contractor (`FIND(safeContractor, ARRAYJOIN({Contractor}))`); lower duplicate-name risk but same shape
