# TODO

## 🐛 NEXT UP — sweep the remaining `FIND(name, ARRAYJOIN(...))` substring filters

**Agreed 2026-08-02 as the next piece of work. ~1-2 h.** The only genuine *correctness* bug
outstanding; everything else on the list is a feature or a cleanup.

The cross-job filter bug fixed in `03f552a` (handleTimeEntries / handleExpenses) has the same
shape in four remaining places. Each matches a job by NAME via substring, so one job whose name
contains another's leaks across — and duplicate exact names collide outright.

**This is not hypothetical on this data.** Live job names include *Craig Davidson* alongside
*Davidson's Addition*, and *Harlin Smith (2 Barn)* alongside *Coltonas 2 Barn*. A collision shows
inspections or estimates **on the wrong job** — quiet, plausible-looking wrong data.

Line numbers verified 2026-08-02 (they drift; grep `FIND(` + `ARRAYJOIN` to confirm):

- `netlify/functions/airtable.js:2078` — generator lookup (`F.gen.job`)
- `netlify/functions/airtable.js:2194` — Job Inspections (`FIND(jobName, ARRAYJOIN({Job}))`)
- `netlify/functions/airtable.js:2224` — Job Estimates (`FIND(jobName, ARRAYJOIN({Job}))`)
- `netlify/functions/airtable.js:2480` — Contractor (`FIND(safeContractor, ARRAYJOIN({Contractor}))`);
  lower duplicate-name risk but the same shape

**Fix pattern** — keep the `FIND` as a loose prefilter, then verify the linked record ID in
memory. Already done correctly in `handleGetJobInvoices`; the newline-delimited variant at
airtable.js:2824 / :2851 is the reference to copy. Add a test per site.

> Worth noting the photo system sidesteps this entirely by keying on the Airtable **record id**
> (`jobs/<recId>/…`) rather than the name — record ids cannot collide. Same idea applies here.

## Receipts on expenses — partly shipped

- ✅ **Materials-PDF archiving** shipped 2026-08-01 (`54201d7` + `5b5ccf5`): the inventory app's
  generated PDF now uploads to R2 as well as downloading, admin/office only.
- ⬜ **Attach-a-receipt slices 1-3** (~8-11 h) — not started. Must accept BOTH phone photos
  (compress) and ScanSnap PDFs (upload untouched, no thumbnail). Full plan, decisions and
  gotchas in `docs/PLAN-expense-receipts.md`.

## Smaller, unscheduled

- **R2 lifecycle rule** to expire the photo recycle bin (`_deleted/`) at 30 days — ~15 min. Must
  **exclude `expenses/`**: receipts are financial records and are deliberately never auto-purged.
- **Retire the JotForm photo path** (~2026-08-08, after a week's soak): pause form
  `260246511955053` and Make scenario `4522457`. Pause, don't delete. No code change.
- **Archive Push History reprints** — `reprintPushHistoryPdf` rebuilds the PDF without a `pushId`
  or job id, so it downloads but doesn't archive. Deliberate; easy to add.
- **Offline upload queue** for photos — basements and steel buildings. Worth building around
  observed field behaviour rather than guesses.
