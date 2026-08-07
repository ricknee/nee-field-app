# Plan: a time clock in the field app

**Status:** **NOT BUILT — parked deliberately.** Owner's call 2026-08-07: *finish the migration
first, then look at time.* Nothing here should start before §3 Step 3 of `ROADMAP.md` (Make leaves
the time path). Written down now so it is costed and the reasoning survives.

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
