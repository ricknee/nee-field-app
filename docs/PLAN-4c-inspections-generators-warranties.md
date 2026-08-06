# Plan — Step 4c: Inspections, Generators, Warranties → Neon

*Written 2026-08-06. Roadmap §3 Step 4c. Started during the Step 3 soak, deliberately — see §1.*

---

## 1. Why this is starting now, during the soak

The roadmap said "don't start 4c before Step 3, to avoid two half-migrated domains at once."
**That reasoning was written about 4a, when Steps 1 and 2 weren't done.** It no longer holds:

- Time is on Neon in **both** directions (reads confirmed on prod, writes flipped and
  smoke-verified). Nothing about it is half-done.
- Step 3 is switching off one Make scenario. It is gated on **soak days**, which is waiting,
  not working — the same argument the roadmap already uses to let inventory C3 fill quiet days.
- 4c overlaps the blocked `handleJobs` flip: **10 of the ~35 missing `mapJob` keys are the
  inspection block** (lookups through Inspection Agencies / Inspection Contacts). Doing 4c
  builds exactly those dimension tables, so deferring it means touching them twice.

**The one carry-over caution:** if the reconciler goes red on Aug 7 or 8, time data wins.
Keep 4c in reviewable chunks so it can be set down, and never let it block a Step 3 go-ahead.

---

## 2. Scope — 75 rows across 7 tables

| Airtable table | Rows | Neon table |
|---|---|---|
| Job Inspections | 22 | `job_inspections` |
| Inspection Agencies | 15 | `inspection_agencies` |
| Inspection Contacts | 5 | `inspection_contacts` |
| Generators | 11 | `generators` |
| Generator Service | 8 | `generator_service` |
| Warranties | 12 | `warranties` |
| Warranty Templates | 2 | `warranty_templates` |

Smaller than Fleet + Lifts (110 rows). **The difficulty here is relational and formula-shaped,
not volumetric** — Generators alone carries 3 formulas and a rollup, and Job Inspections is
mostly lookups that become joins.

### Handlers to flip (13)

`jobInspections` · `createInspection` · `updateInspection` · `updateJobInspection` ·
`getInspectionAgencies` · `createInspectionAgency` · `inspectorsForAgency` ·
`createInspectionContact` · `generator` · `addGeneratorService` · `commissionGenerator` ·
`getWarrantyTemplates` · `getWarranties` · `addWarranty`

---

## 3. Decisions already taken (owner, 2026-08-06)

### Attachments — both paths dropped, no R2 in this slice

- **`Inspection Contacts.Files / Images` — dead.** 0 of 5 rows populated; nothing in
  `airtable.js` or `index.html` reads it. No column, no migration step.
- **`Job Inspections.Attachments` — 2 photos on 2 of 22 rows**
  (`20260320_133529.jpg`, `20260217_120545.jpg`), also read by no code. **Owner's call: let
  them go.** Inspection photos belong in the existing job-photo path if they are ever wanted;
  a second attachment path for a rarely-used field is not worth carrying.

> They remain in Airtable until that table is actually retired, so this is not yet irreversible.
> If it is ever revisited, `copyAirtablePhotosToR2` in `_r2.js` already does the job.

### All five contact syncs move, not just the two inspection ones

See §4. Writing one generic sync is barely more work than two, and the other three tables
migrate in later slices anyway.

---

## 4. The Google contact sync — what's actually wrong, and the replacement

**Diagnosed 2026-08-06 against the live Make account. The failure is not Google.**

Both OAuth connections used by these scenarios are healthy — `Google - Rick` (4769144) and
`NEE -Google` (4769161) both report `expire: null`. This is **not** the pCloud situation, where
no token can be obtained at all.

The execution history is the tell:

| Scenario | Executions ever |
|---|---|
| Sync **Inspection Contacts** → Google (4739000) | **zero** |
| Sync **Inspection Agencies** → Google (4739070) | **zero** |
| Sync Power Company Contacts (4735255) | 1, success, 2026-07-30 |
| Sync Vendor Contacts (4739137) | — |
| Sync Awarded Job Contacts (4729925) | several; 2× `BundleValidationError` in July, success 2026-08-01 |

The two inspection scenarios are **active but have never once fired** — an Airtable-side trigger
that was never wired, not a broken integration.

### What replacing them costs

Make's OAuth connections are **not exportable**, so a Neon-native sync needs its own Google
Cloud project, OAuth consent screen, and a stored refresh token. Unlike pCloud, Google's app
registration works fine, so this is real work (~2-3 h) but not blocked.

> ⚠ **Free intel from that `BundleValidationError`** ("Validation failed for 1 parameter(s)"):
> some contact record is missing a field the Google People API requires. **Handle nameless or
> partial contacts explicitly** rather than throwing — Make didn't, and it cost that scenario
> two failed runs.

### Design — one sync table, not six columns × five tables

Airtable duplicates the same six sync fields onto every contact-bearing table
(`Google Contact ID - Rick`, `Google Contact ID - NEE`, `Sync Status …` ×2, `Last Synced At`,
`Needs Sync to Google`). **Do not replicate that.** One table:

```sql
CREATE TABLE google_contact_sync (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type  text NOT NULL,   -- 'inspection_contact' | 'inspection_agency' | ...
  entity_id    uuid NOT NULL,
  google_account text NOT NULL, -- 'rick' | 'nee'
  contact_id   text,            -- Google People resourceName
  status       text,
  last_synced_at timestamptz,
  error        text,
  UNIQUE (entity_type, entity_id, google_account)
);
```

`Needs Sync to Google` was an Airtable **formula**; here it is a query (`last_synced_at IS NULL
OR entity.updated_at > last_synced_at`), so it can never drift out of step with the data.

**Still Make-owned until 4c-3 ships — do not write these from the app** (`Google Contact ID`,
`Sync Status`, `Last Synced At`, `Needs Sync to Google`), per the standing rule in CLAUDE.md.

---

## 5. Schema notes that matter

### Lookups become joins, not columns

Generators stores 37 Airtable fields, but ~11 are lookups arriving through the Job link —
`Customer`, `Customer Name`, `Customer Phone #`, `Site Address`, `Jobsite Steet Address`,
`Jobsite City/State/Zipcode`, `Customer 1st/Last Name`, `Contractor`. **None of these become
columns.** They are `JOIN jobs` in a view. Copying them would recreate the duplication the
migration exists to remove.

### Four derived fields belong in a view, not stored columns

| Airtable | Kind | Neon |
|---|---|---|
| `Last Service Date` | rollup | `MAX(service_date)` from `generator_service` |
| `Next Service Due` | formula | install/last service + `service_interval_months` |
| `Service Status` | formula | derived from `Next Service Due` |
| `Battery Age` | formula | from `battery_install_date` |

> ⚠ **THE MONTH-ARITHMETIC GOTCHA APPLIES HERE.** `PLAN-job-warranty-service-log.md` §2 already
> documents that JS and Postgres disagree on month addition (Jan 31 + 1 month). `Next Service
> Due` and `Warranty Expiration` are both month-add fields. **Compute in one place — Postgres —
> and never re-derive client-side**, or the app and the DB will quietly disagree about when a
> generator is due.

### Generator Service checkboxes stay booleans

Fleet used `text[]` for `service_types` because Airtable had it as a **multipleSelects**. These
are nine separate named checkbox fields (`Oil Changed`, `Air Filter Changed`, …) — they map 1:1
to booleans and are clearer that way. Don't collapse them into an array by analogy.

### Job Inspections has both a lookup and a real link to the agency

`Inspection Agency` (lookup) **and** `Inspection Agency (Linked)` (multipleRecordLinks). Migrate
the **link**; the lookup is its shadow.

---

## 6. Traps carried forward from 4b

- **⚠ `grep 'startsWith("rec")'` before flipping any handler.** The fleet slice nearly shipped
  a bug where `handleLogMileage` rejected every truck the moment ids became uuids. Same shape
  will exist here.
- **Write handlers must resolve *either* id form** (`rec…` or uuid) for as long as an Airtable
  read fallback exists — permanent, not a shim.
- **Reads fail soft, writes fail CLOSED** (`neonWrite`). Established at Step 2; applies here.
- **Linked-record writes use bare `["rec…"]`**, never `[{id:"rec…"}]`, on any Airtable mirror.

---

## 7. Slices

| # | What | Depends on | Size |
|---|---|---|---|
| **4c-1** | The five un-entangled tables: `job_inspections`, `generators`, `generator_service`, `warranties`, `warranty_templates` (55 rows). Schema → ETL → read flip → write flip. **No Make attachment at all.** | nothing | ~3-4 h |
| **4c-2** | `inspection_agencies` + `inspection_contacts` (20 rows) | 4c-1 | ~1 h |
| **4c-3** | Generic Neon→Google contact sync; retire scenarios 4739000, 4739070, 4735255, 4739137, 4729925 | 4c-2 + Google OAuth app | ~2-3 h |
| **4c-4** | Job service visit log, Neon-native — `PLAN-job-warranty-service-log.md` §3 | 4c-1 | ~4-6 h |

**4c-1 starts immediately** — it needs neither the Google decision nor the soak to finish.

> Housekeeping spotted while surveying: `db/schema/` has **two `007_` files**
> (`007_panel_schedules.sql` and `007_schedule.sql`). Not harmful — they were applied by hand,
> not by a migration runner — but the next file should be `011_`, and the collision is worth
> knowing about before anyone writes a runner that orders by prefix.
