# Plan: a time clock in the field app

**Status:** **BUILT 2026-08-08 — shipped INERT, not in use.** The gate this was parked behind
cleared: Step 3 landed 2026-08-07 and the field-app migration completed 2026-08-08.

Owner's call on building it, 2026-08-08: *"right now just build the app and replace qb time later
on. not going to use time tracking in the app until its complete and ready for use."*

So it is built and switched **off**. QuickBooks Time keeps running and keeps being the book of
record. Nothing below happens to anyone until two env vars are set — see **§11 Switching it on**,
which is also the record of what was actually built and where it differs from the original draft.

*Original status, kept for the reasoning: NOT BUILT — parked deliberately. Owner's call 2026-08-07:
finish the migration first, then look at time.*

**One-line:** Punch in and out from the phone that is already in the electrician's hand, against a
job, and have it become a time entry in Neon — where time entries already live.

---

## 1. Why it's worth doing

Time is captured today in **QuickBooks Time**, pulled into Airtable by Make scenario `4546051`,
and (after Step 3) pulled straight into Neon by the puller. The app already does everything
*downstream* of capture: payroll runs, the payroll PDF, city-tax hours, true labor cost, GP.

So the app owns the whole time pipeline except the one step where a person says "I started."

That is worth closing for three reasons:

- **It is the last thing QB Time is for.** Payroll already comes out of this app —
  `NEE_Payroll_2026-08-08.pdf` was produced here and reconciled to the cent. If capture moves, the
  subscription has no remaining job.
- **The data would arrive already linked.** A punch made *inside* the app knows the job, because
  the user is standing in a job. Make's habit of silently dropping the Job link — hours paid but
  never costed — is a capture-time problem that a capture-time fix removes at the source.
- **One fewer place to look.** Two systems hold hours today. This is the move that makes it one.

## 2. What already exists

The reason this is smaller than it sounds. Since 2026-08-05 the substrate is done:

| Piece | Where | Reuse |
|---|---|---|
| Time entries authoritative in Neon | `ROADMAP.md` §3 Step 2 | as-is — this is the whole foundation |
| `handleCreateTimeEntry`, Neon-first | `airtable.js:920` | writes the entry on punch-out |
| `source = 'Manual'` accepted at insert | `db/schema/002_qb_puller.sql:31,45` | a clock row is already legal |
| Self-write authz tier | `_TIME_SELF_WRITES` → `_PAYROLL`, `airtable.js:426,441,534` | employees can already write their own time |
| `⏱ My Hours` screen | `index.html:2341` (`btnMyHours`) | the clock's natural home |
| `myHoursRollup` / `myHoursBreakdown` | `airtable.js` dispatcher ~`7053` | shows the punches back |
| Labor cost + OT computed from entries | `db/schema/006_true_labor_cost.sql` | clock data flows into job GP with no extra work |
| Offline replay queue idiom | `docs/PLAN-job-checklists.md` §5 | the pattern the punch queue copies |
| Neon write helper (fails closed) | `_neon.js` — `neonWrite` | with the caveat in §6 |

**No new dependency, no new mechanism.** The genuinely new parts are §4 and §6.

## 3. The fork — decide this before anything is drawn

> **Does the app REPLACE QuickBooks Time, or FEED it?**

Everything downstream changes on the answer, so it is not a detail to settle during the build.

| | Replace | Feed |
|---|---|---|
| QB Time | cancelled | stays, app pushes punches to it |
| Book of record for hours | this app, alone | QB Time |
| Payoff | one system, one subscription, capture and costing in one place | safe — QB keeps the ledger |
| Cost | the app is now the only proof of what someone worked | two systems forever, still paying for both |
| Recovery if a punch is lost | none outside our own backups | QB still has it |

**Do not decide this now.** It is much clearer after Step 3, when Make is out of the path and QB is
the only remaining upstream — at that point the question is simply "is QB still earning its keep",
and the answer is visible rather than predicted.

## 4. Where the data lives — Neon, with one new idea

`time_entries` has **no concept of an open shift**. It stores `work_date` and `duration_seconds`
and nothing else — there is no `started_at`, no `ended_at`, no "who is on the clock right now".
That absence is the real schema work.

Shape (to be written as `db/schema/0NN_time_clock.sql`):

- **`open_punches`** — at most one row per employee. `employee_id`, `started_at timestamptz`,
  `job_id`, `job_name`, `class`, optional `notes`, optional lat/lon. A **unique constraint on
  `employee_id`** is what makes double-punching impossible rather than merely discouraged.
- **On punch-out**, the row is deleted and a normal `time_entries` row is inserted —
  `source = 'Manual'` (or a new `'Clock'`, see §8), `duration_seconds` = the elapsed seconds.
- Optionally carry `started_at` / `ended_at` onto `time_entries` as nullable columns, so a clocked
  entry can show *when*, and a QB or hand-entered one simply has them NULL.

The generated `hours` column keeps doing what it does — see the rounding trap in §8.

## 5. The screen

Lives under `⏱ My Hours`, which every payroll-eligible role already sees.

- **Clocked out:** one large green **Clock In** button. Job defaults to `nee_last_job` (the same
  key the inventory app hands off with), changeable by tap.
- **Clocked in:** a running elapsed timer, the job it is against, and a red **Clock Out**. The
  running state should also be visible from the top bar — being accidentally on the clock overnight
  is the classic failure of every clock app, and it is a UI problem, not a data one.
- **Punch-out sheet:** confirm job, class, city (§8), optional note. Short, because it stands
  between a person and going home.
- **Below it:** today's punches and the existing My Hours tiles, unchanged.

## 6. Offline is the feature, not a detail

⚠ **This is most of the build, and the part that decides whether it is trustworthy.**

Two facts collide:

1. Crews work in basements and a shop **where signal dies** — job checklists already carry a
   localStorage replay queue for exactly this reason.
2. **Neon writes now fail CLOSED** (`neonWrite`, deliberate — see `ROADMAP.md` §3 Step 2). A write
   that lands in Airtable but not Neon is invisible, so failing closed is correct for payroll.

Failing closed is right for *editing* payroll. It is **wrong for a punch**: it means a person
cannot clock out, and their hours are lost. Nobody stands in a parking lot retrying.

So a punch must be **recorded locally first and replayed**, with the timestamp taken from the
*punch*, not from when the replay succeeded. That is the one place this feature is allowed to
diverge from the fail-closed contract, and the divergence has to be explicit and commented, or
someone will later "fix" it back and quietly reintroduce lost punches.

Consequence to design for: a punch may arrive **hours late**, and out of order.

## 7. Authorization

Nothing new. `createTimeEntry` is already in `_TIME_SELF_WRITES` → `_PAYROLL` (admin + employee),
which is exactly the population that should be clocking. Viewers and office are correctly excluded.

New actions (`clockIn`, `clockOut`, `clockStatus`) go in the same tier in `authzFor`. An employee
must only ever be able to punch **themselves** — the employee id comes from the token, never from
the request body.

## 8. Traps

⚠ **Quarter-hour rounding is baked into the schema and must not be touched.**
`hours` is `ROUND((duration_seconds/3600)*4)/4` (`001_time_entries.sql:54`), verified against 200
real rows — plain division mismatched 131 of them, and "fixing" it moves roughly 90 h of payroll
across the table. A to-the-second clock is therefore cosmetic beyond 15-minute granularity. Say so
in the UI rather than implying precision that payroll discards.

⚠ **City taxes are free text carrying QB Time's spellings** — `Massilon`, `New Philadephia`
(`001_time_entries.sql:56-60`), and `PR_CITY_TAXES` must match them **verbatim** or values fall
back to "A No Tax". Today QB supplies them. If the app becomes the clock, something has to choose a
jurisdiction per punch — the cheapest honest answer is to default it from the job's city and let it
be overridden, not to ask on every punch.

⚠ **Three sources of hours during the soak.** Building before Step 3 means QB Time, Airtable and an
app clock all writing while the reconciler is meant to be proving itself clean. This is the main
reason the feature is parked. The `source` column separates them, but it muddies the one gate
standing between here and retiring Make.

⚠ **Consider `source = 'Clock'` rather than reusing `'Manual'`.** `'Manual'` currently means "a
human typed hours into the payroll screen". A punch is a different provenance and will want to be
counted separately the first time anything is audited. Requires touching `te_has_a_key`.

⚠ **`employee_id` is nullable and historically unlinked on ~38% of rows.** A clock row must always
carry it — it comes from the session, so there is no excuse for a NULL, and a `NOT NULL` on the
clock path is worth having even though the column stays nullable overall.

⚠ **No background geofencing.** QB Time auto-punches on arrival. A web app cannot: iOS Safari has
no background geolocation and a PWA gets no reliable wake-up. A one-shot `navigator.geolocation`
stamp *at* the punch is possible and is the honest version of this. Do not promise the QB
behaviour; it is not achievable on this stack.

⚠ **Overnight punches cross `work_date`.** A shift starting 22:00 and ending 02:00 belongs to
whichever date the business says it does, and `week_start_date` is generated from `work_date`, so
getting this wrong silently moves hours between pay weeks. Decide the rule explicitly.

## 9. Not in scope

Breaks/lunch deduction, PTO and holiday accrual, crew-lead punching a whole crew in at once,
approval workflow beyond the existing `labor_reviewed`, and any QB Time write-back (that only
exists at all under the "feed" arm of §3).

## 10. Rough size

Assuming the **replace** arm and after Step 3:

| Piece | Size |
|---|---|
| Schema + `clockIn` / `clockOut` / `clockStatus` handlers + tests | ~2-3 h |
| The screen (punch, running timer, top-bar indicator, punch-out sheet) | ~3-4 h |
| **Offline punch queue + replay** (§6) | ~3-4 h |
| Job / class / city selection UX | ~1-2 h |

**~10-14 h**, of which the offline queue is the part that can overrun. The "feed" arm adds a QB
Time write path on top and is not costed here.

---

## 11. What was actually built — and switching it on

Built 2026-08-08. Everything above still describes the intent; this section records where the
build **diverged** from the draft, and is the operating manual.

### The one real design change: punches do not go straight into `time_entries`

§4 drew punch-out as an insert into `time_entries` with `source = 'Clock'`. That is the right end
state, but it is unsafe as the *starting* state given the owner's decision to keep QB Time running
throughout. `handlePayrollHoursRollup` sums `time_entries` with no source filter, and so do seven
other reader sites plus the labor-cost views in `004`/`006` — so a punch landing there is a punch
landing in payroll, against hours QB Time is also being paid for.

Filtering `'Clock'` out of all eight readers would fail **open**: miss one and money is wrong.
So the clock got its own ledger instead, and promotion became a separate, switched step:

```
punch  ->  clock_punches (always)  ->  time_entries (only when TIME_CLOCK_PAYROLL=on)
```

Nothing existing reads `clock_punches`, so while the switch is off the clock cannot touch payroll
**by construction** rather than by remembering to filter — and zero existing readers were edited.
Same shadow-then-flip shape as the login migration.

### The two switches

| Var | Values | Meaning |
|---|---|---|
| `TIME_CLOCK` | unset/`off` (default), `admin`, `on` | **Who can punch.** `off` = the feature does not exist: `clockStatus` answers `enabled:false`, the UI renders nothing, `clockIn`/`clockOut` 403. `admin` = admins only, for shaking it out on prod. `on` = all payroll-eligible roles (admin + employee). |
| `TIME_CLOCK_PAYROLL` | unset/`off` (default), `on` | **Do punches become payroll hours.** Off = recorded in `clock_punches` only, and the UI says so in as many words. On = each punch-out also writes a `time_entries` row with `source = 'Clock'`. |

They are separate because the dangerous half is not letting people punch — it is letting a punch
turn into money while QB Time is still being paid from. Keeping them apart means the whole path
can be exercised end to end before any of it counts.

**Suggested order:** `TIME_CLOCK=admin` → punch a few real shifts yourself → `TIME_CLOCK=on` for
the crew, still not counting → decide the §3 fork → `TIME_CLOCK_PAYROLL=on` → run
`promoteClockPunches` once to count everything punched before the flip.

**Rollback is exact:** `DELETE FROM time_entries WHERE source = 'Clock'` removes everything the
clock ever contributed to payroll and leaves every punch intact in `clock_punches`. That precision
is the whole reason `'Clock'` is its own source value rather than reused `'Manual'`.

### Decisions the plan left open, now made

- **Overnight rule (§8):** a shift belongs to the **local date it started**. 22:00 Tue → 02:00 Wed
  is a Tuesday shift, all of it. Keeps one shift inside one pay week. Verified against the real
  pooler: a 22:30 EDT punch files under the 11th, not the 12th, and `week_start_date` lands on the
  correct Monday.
- **Timezone:** `America/New_York`, named explicitly in SQL. This is the first timestamp→date
  conversion in the whole schema — every other date arrived pre-made from QuickBooks — and Neon's
  pooler connects in UTC, which would otherwise file every evening punch under tomorrow.
- **`source = 'Clock'`**, not `'Manual'` (§8's open question). `te_has_a_key` was extended in `018`
  to accept it; without that, promotion fails on every row.
- **No Airtable mirror.** `handleCreateTimeEntry` still mirrors; the clock does not. Make left the
  time path at Step 3 and the Airtable Time Entries table is a frozen historical copy — writing new
  punches into it would put rows in something nothing reads.
- **Clock skew:** a punch-out earlier than its punch-in is **clamped** to zero, not rejected. The
  `clock_punch_ordered` CHECK would otherwise roll back the shift-closing delete and strand someone
  on the clock with no way off it. A zero-length punch is a far better failure.
- **Punch window:** client timestamps are trusted (that is what makes offline replay honest) but
  bounded to **±36 h**, wide enough for a genuinely late replay and tight enough that a device with
  a wrong year cannot file hours into a closed pay period.

### ⬜ TODO BEFORE CUTOVER — a dry run for `promoteClockPunches`

**Not built. Deferred deliberately on 2026-08-08** ("make a note to remember 5"), because it is
only needed on the day the switch is thrown, and that day hasn't come.

`promoteClockPunches` is the single riskiest action in this feature: it turns recorded punches into
payable hours, in bulk, for everyone at once, and it has only ever run on a Neon test branch. It
currently offers no way to see what it would do first.

**What to build:** a `dryRun: true` option that reports *"this would create 47 entries totalling
312.5 hours across 5 people, earliest 2026-08-11"* — grouped by person, writing nothing. Read the
output, agree it matches what the reconciliation screen has been showing all along, and only then
run it for real.

Roughly an hour. Do it **before** flipping `TIME_CLOCK_PAYROLL`, not after.

### If the job picker gets long — filter by year (owner, 2026-08-09, "not now")

The picker renders every job in the pool (~76 of 112 today) with no cap. A 60-row
cap existed briefly and was removed: at this scale it protected against nothing, and it
twice created a job that existed but could not be found by scrolling.

When the list does become unwieldy, **filter by year — not by truncating**. `jobs.job_year`
already exists and the main sidebar's year filter is the pattern to copy. Truncation is the
wrong tool because it gives no clue which jobs are missing; a year filter is visible, and the
person choosing it knows what they excluded.

⚠ Whatever gets added, **overhead jobs must stay exempt** (`clock_visibility` in `027`). They
have no meaningful year and Shop Work alone carries ~500 h/yr.

### Verification done

- Backend: 9 new cases in `tests/handlers.test.mjs` (126 pass, 0 fail) covering the off state, the
  admin gate, role authz, replay-key validation and the punch window.
- SQL: exercised by hand on a Neon branch, because **offline tests die at the connection and would
  pass over broken SQL**. `PREPARE` on all three parameterised writes deduces clean parameter types;
  a full cycle — punch in, double-punch refused, overnight punch out, promote, re-promote — behaves
  correctly, including quarter-hour rounding (3 h 47 m → 3.75 h) and idempotent promotion.
- ⬜ **Not done: a browser smoke test.** Nothing has been exercised through the actual UI, and the
  offline queue in particular has never been tested on a real phone losing signal.
