# Plan: panel schedules in the field app

**Status:** **BUILT 2026-08-05 — slices 1 and 2.** ⬜ Needs a prod smoke test. Owner idea.
Slice 3 (watts/amps/poles + phase totals) and slice 4 (save the PDF into Prints) are not built;
their columns already exist in the schema, so slice 3 is UI-only.

**One-line:** A `⚡ Panels` button beside `📐 Prints` on the job action row. Enter how many circuits
the panel has, get the numbers laid out odd-left / even-right, fill in what each circuit feeds
while standing at the panel, and export a print-ready PDF.

---

## 1. Why it's worth doing

Panel schedules are tracked today as **Trello checklists** — one item per breaker,
`"1 AC"`, `"3 AC"`, `"9 spare"`, `"23 Special Ed"`. That works for capture and fails at everything
after it:

- **It isn't a panel schedule.** A checklist is one column of text. A panel is two columns, odd on
  the left and even on the right, because that is how the breakers are physically arranged. Reading
  a flat list while standing in front of a panel means counting.
- **It can't be printed.** The finished artifact is a one-page grid that goes *in the panel door*.
  Today that is rebuilt by hand in Excel or Word from the checklist.
- **The checkbox is meaningless.** Every item is an unchecked box that will never be checked — the
  data is the label, and Trello is being used as a text list with extra furniture.

This is the same shape as prints: the information already exists, and the win is having it in the
job, in the right format, on the phone that is already in the electrician's hand.

## 2. What already exists

| Piece | Where | Reuse |
|---|---|---|
| Action-row button + count badge | `index.html:2205` (`btnJobPrints`), `paintPrintBadge` ~`5365` | copy wholesale |
| `.action-btn` / `.count-badge` styling | `index.html:752` | add one colour variant |
| Full-screen modal + back-stack entry | prints modal, `openJobPrints` ~`5388` | copy the shape |
| jsPDF lazy CDN load | `ensureJsPDF()` `index.html:3338` | as-is |
| Hand-drawn PDF tables (y-cursor idiom) | payroll PDF ~`index.html:17149` | copy the idiom |
| Neon query/write helpers | `_neon.js` — `neonQuery`, `neonWrite` | as-is |
| Handler + authz registration | `airtable.js` dispatcher ~`5270`, `authzFor` ~`451` | as-is |

Nothing here needs a new dependency or a new mechanism. It is assembly.

## 3. Where the data lives — **Neon, not Airtable**

This is the one decision that is expensive to change later, so it gets made first.

A panel schedule is structured, edited repeatedly, and has no Airtable table today. That makes it
the first domain in this app that can be **born in Neon instead of migrated to it.** Building it in
Airtable would mean building it twice — once now, once during Step 4 — for a domain that has no
legacy to preserve.

It is also a safe place to prove the pattern: panel schedules touch no money, no payroll, and
nothing on the Make critical path. If Neon-native goes wrong here, nothing that matters breaks.

```sql
-- db/schema/007_panel_schedules.sql

CREATE TABLE panel_schedules (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  -- The KEY is the Airtable record id, not a FK to jobs.id. See the trap in §8:
  -- a job created ten minutes ago does not exist in Neon yet, and a FK would
  -- reject the first panel anyone tries to add to a brand-new job.
  job_airtable_id text NOT NULL,
  job_id          uuid REFERENCES jobs(id),      -- nullable, backfilled by the hourly sync
  name            text NOT NULL,                 -- "MOP", "Classroom Panel", "Panel A"
  circuits        int  NOT NULL CHECK (circuits BETWEEN 2 AND 84 AND circuits % 2 = 0),
  -- Header metadata off image 2. All optional: they print if set, and the MVP
  -- form only asks for `name` and `circuits`.
  voltage         text,                          -- "120/240V, 1-PHASE, 3-WIRE"
  feed            text,                          -- "MLO" | "MAIN BREAKER"
  mounting        text,                          -- "SURFACE MOUNT" | "FLUSH"
  enclosure       text,                          -- "NEMA1"
  location        text,                          -- "Boiler room, north wall"
  fed_from        text,                          -- "Panel D"
  notes           text,
  created_at      timestamptz DEFAULT now(),
  updated_at      timestamptz DEFAULT now(),
  updated_by      text                           -- employee name, for "last edited by"
);

CREATE INDEX panel_schedules_job ON panel_schedules (job_airtable_id);

CREATE TABLE panel_circuits (
  panel_id    uuid NOT NULL REFERENCES panel_schedules(id) ON DELETE CASCADE,
  number      int  NOT NULL,                     -- 1..circuits; odd = left, even = right
  description text NOT NULL DEFAULT '',
  -- Slice 3 columns, in the schema from day one so adding them to the UI is not
  -- a migration. Empty until the UI exposes them.
  watts       int,
  amps        int,
  poles       int,
  PRIMARY KEY (panel_id, number)
);
```

**Why the slice-3 columns ship in slice 1's schema:** they cost nothing empty, and adding a column
to a live Neon table later means a migration plus a deploy ordering problem. The UI stays
description-only until slice 3 — that is a UI decision, not a schema one.

**Rejected alternatives.** *Airtable table* — adds to the pile Step 4 has to migrate, for a domain
with zero legacy; explicitly against the roadmap. *JSON blob in R2* — reuses the prints machinery
and needs no schema, but a panel schedule is structured data that people will eventually want to
query ("which panels on this job still have spares?"), and blob-per-panel makes two people editing
the same panel a last-write-wins data loss.

## 4. The screen

**Two levels, matching prints:**

1. **`⚡ Panels` button** in the action row beside `📐 Prints`, with a count badge, so a crew can see
   at a glance whether this job has schedules at all. Badge loads fire-and-forget after the job
   renders — the header never waits on it (copy `loadPrintBadge`).
2. **Panel list modal** — one row per panel: name, circuit count, how many circuits are filled in
   (`31 / 42`), last edited by/when. Plus `+ New panel`.
3. **Grid editor** — the actual schedule.

**The grid.** Two columns of rows. Left column = odd numbers ascending, right = even. A 42-circuit
panel is 21 rows; the number is fixed furniture and the description is the only editable part:

```
    CKT   DESCRIPTION              CKT   DESCRIPTION
     1    [ AC              ]       2    [ Panel D        ]
     3    [ AC              ]       4    [ Panel D        ]
     5    [ AC              ]       6    [ Panel A        ]
     …
```

**On a phone it stacks.** Under ~640px the two columns become one list in circuit order
(1,2,3,4…) rather than shrinking two columns to unusable width. The number stays a fixed prefix so
position is never ambiguous.

**Two entry helpers, both earned by the Trello data.** That screenshot has `9 spare` through
`21 spare` — seven identical entries typed one at a time:

- **Fill down** — a small `↓` on each row that copies this description into every empty circuit
  below it *on the same side*. Same side matters: 9→21 odd is what a run of spares actually looks
  like.
- **`Mark remaining as spare`** — one button, fills every still-empty circuit with `spare`. Most
  panels finish this way.

**Saving.** One explicit `Save` that writes the whole panel in a single request (not per-cell —
a panel room has no signal, and 42 in-flight autosaves is 42 chances to fail). Every keystroke also
writes a **localStorage draft** keyed by panel id. If the save fails, the draft survives the failure
and the banner says so, so a crew never loses 42 typed descriptions to one dropped connection. A
real offline queue is out of scope — roadmap §7 already parks that for photos with "wait until
crews actually hit it", and the same logic applies here.

## 5. The PDF

`Download PDF` in the grid editor. jsPDF is already lazy-loaded from CDN; the payroll report at
`index.html:17149` is the idiom to copy (fixed row height, `y` is always the top of the next thing
to draw, `checkSpace` before each block).

- **Portrait letter for slice 1.** Four columns (CKT / DESCRIPTION / DESCRIPTION / CKT) with the
  numbers on the outer edges, exactly like image 2. 21 rows at ~18pt is ~380pt of grid — one page
  with room to spare. Slice 3's watts/amps/poles columns are what forces landscape; make that
  switch when they land, not before.
- **Header band** from the metadata that is set: `PANEL "MOP"  120/240V, 1-PHASE, 3-WIRE   MLO
  SURFACE MOUNT   NEMA1`, plus the job name and today's date. Fields left blank simply don't print.
- **Filename:** `Panel MOP - Bethel School.pdf`.
- **Empty circuits print as empty numbered rows** — image 2 does exactly this (17 through 41 are
  blank but numbered). A blank row is a real statement: that breaker space exists and is unused.

## 6. Authorization

Follows prints, for the same reason: this is field information, and gating it on admin means it
never gets used.

| Action | Tier | Why |
|---|---|---|
| `panelSchedules`, `panelSchedule` (GET) | *any signed-in role* — absent from every set in `authzFor` | A crew reading the panel on site is the entire feature |
| `savePanelSchedule`, `createPanelSchedule` (POST) | `_NON_VIEWER` | The electrician at the panel is the one who knows what circuit 23 feeds. Viewers stay read-only |
| `deletePanelSchedule` (POST) | `_ADMIN_OFFICE` | Destructive and rare |

Writes use **`neonWrite`, not `neonExec`** — Neon is the only home for this data, so a failed write
must fail the request. `neonExec` swallows failures, which is correct only for a mirror where the
real write already landed somewhere else. There is no somewhere else here.

## 7. Slices

| # | What | Est. | Ships something usable? |
|---|---|---|---|
| ~~**1**~~ | ✅ Schema + 5 handlers + button/badge + list modal + grid editor + save + localStorage draft | **4-5 h** | Yes — replaces the Trello checklist |
| ~~**2**~~ | ✅ PDF export | **~2 h** | Yes — replaces the hand-rebuilt Excel sheet |
| **3** | Watts / amps / poles columns, per-phase totals, total connected amps | **2-3 h** | Completes image 2 |
| **4** | `Save to Prints` — push the generated PDF into the job's prints via `jobPrintUploadUrls` | **~30 min** | The schedule lands with the drawings |

**What actually shipped in 1-2, beyond the plan above:** Enter moves to the next circuit (filling a
panel is one long run of typing); a `↓` fill-down per row that copies into empty circuits *on the
same side only*; `Fill blanks with "spare"`; a live `n of 42 circuits filled in` counter; filled
circuits tinted so unwalked breakers stand out; size chips (12/18/20/24/30/42) beside the circuit
count; and the odd-count round-up happening client-side with a message rather than as a bare 400.

Slices 1 and 2 are the feature as described. 3 and 4 are worth listing because they are cheap
*after* 1 and 2 and expensive to retrofit if slice 1 is built without them in mind — which is why
the schema in §3 already has the columns.

## 8. Traps

1. **Do not FK to `jobs.id`.** Neon's `jobs` table refreshes hourly, so a job created minutes ago
   is not there yet — a FK would reject the first panel added to a brand-new job with a foreign-key
   error the user cannot act on. Key on `job_airtable_id text`; let `job_id` backfill. This is the
   same trap that produced "new job = empty Time Entries tab for ~1 h".
2. **Circuit count must be even.** Odd/even column layout requires it. Offer preset buttons
   (12 / 18 / 20 / 24 / 30 / 42) and round a typed odd number up, saying so — don't reject silently.
3. **Shrinking the circuit count destroys data.** Going 42 → 30 orphans circuits 31-42, which may
   have descriptions. Warn with the count of non-empty circuits about to be lost, and require
   confirmation. Growing is always safe.
4. **Multi-pole breakers are not modelled in slice 1.** In image 2 a 2-pole breaker occupies
   circuits 1 *and* 3 with the same description. Until slice 3 adds `poles`, that is just the same
   text typed on both rows — which fill-down makes cheap. Don't half-build pole spanning in slice 1;
   it changes how rows render and is a slice-3 concern.
5. **`updated_by` is a name, not a link.** Same reasoning as `job_name` on time entries: the text
   snapshot is the history, and an employee record that changes later must not rewrite who filled in
   a panel schedule two years ago.

## 9. Where this sits on the roadmap

**This is a detour.** It is not in ROADMAP §3 and does not move anything off Airtable. Said plainly
so it's a choice rather than a drift.

Two things make it a cleaner detour than most:

- **It overlaps no Step 4 slice.** 4a is fleet/lifts, 4b inspections/generators, 4c expenses,
  4d estimates/invoices. Panel schedules are a new domain with no Airtable table, so nothing here
  gets built twice — *provided* it is built in Neon per §3. Built in Airtable, it becomes a fifth
  thing Step 4 has to migrate.
- **The timing is unusually cheap.** The next roadmap action is *soaking* the reconciler for several
  days before Step 3 retires Make. That is a waiting period, and this is work that doesn't touch
  time entries, payroll, or Make.

It should still not jump ahead of Step 3 once the soak is done.
