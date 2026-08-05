# Plan: job checklists in the field app

**Status:** **BUILT 2026-08-05.** ⬜ Needs a prod smoke test. Owner idea.

**One-line:** A `✅ Lists` button on the job action row. Name a list ("Supplies from shop"),
type items one per line, and tick them off as you load the truck the next morning.

---

## 1. Why it's worth doing

Crews keep these as **Trello checklists** today. That works for capture and then gets in the way:

- **It's a second app, with a second login**, holding information that belongs to the job the
  crew already has open.
- **The checkbox is the only useful part**, and Trello wraps it in cards, boards, members, due
  dates and power-ups that nobody on a truck at 6am wants to navigate.
- **There is no "what do I still need?" number** anywhere the crew already looks. It's inside a
  card, inside a list, inside a board.

Same shape as prints and panel schedules: the information already exists, and the win is having
it on the job, in the app that's already open, in the format the work actually takes.

## 2. Where the data lives — Neon, again

Second domain **born in Neon** rather than migrated to it (`db/schema/008_job_checklists.sql`).
There is no Airtable checklist table and never was, so building it in Airtable would add a sixth
thing for roadmap Step 4 to migrate, for a domain with no legacy. Writes use `neonWrite` and fail
**closed** — there is no Airtable to fall back to, so answering "no lists" when the database is
unreachable would tell a crew there is nothing to bring.

**Keying repeats the panel-schedule trap deliberately:** `job_airtable_id text NOT NULL` is the
key and `job_id uuid` is a nullable FK that backfills. Neon's jobs table refreshes hourly, so a
job created ten minutes ago isn't there and a NOT NULL FK would refuse the first list made on it.

## 3. The one decision worth recording — ticked is not deleted

"I click the checkbox and it removes it from the checklist" could mean three things. It means:
**the item leaves the active list and lands in a collapsed `✓ Loaded (7)` section**, one tap to
put it back. `done` flips; the row stays.

Chosen over deleting outright because the failure mode is asymmetric: a mis-tap on a phone, in a
truck, with gloves on, would otherwise delete the line **and leave no trace that it ever
existed** — you arrive without the pipe and can't work out what you missed. Chosen over Trello's
strike-through-in-place because the question the crew is answering is "what's left?", and a list
that keeps everything visible answers it worse the longer it gets.

## 4. The screen

- **`✅ Lists`** in the action row, badge = **open items across every list on the job**, so
  `✅ Lists 7` means seven things still to bring. (Panels' badge counts panels; this one counts
  what's outstanding, because that's the question.)
- **List of lists** — name, `3 to go · 7 loaded`, tap to open.
- **The list** — open items with big tap targets, an add box pinned under them, and the collapsed
  `✓ Loaded (n)` section beneath that.
- **Add box:** type, Enter, it lands at the bottom and the cursor stays put for the next line.
  Re-tapping the field between items is exactly the friction that stops a list being written down.

## 5. No Save button — the offline tick queue

The panel editor saves the whole grid on one button. A checklist must not: you're stood at a shelf
ticking items one at a time, and each tick has to persist on its own.

So every tick is **optimistic** — the row moves the instant it's tapped — and the write follows.
If the write fails, the tick is **queued in `localStorage`** rather than bounced, because the item
went on the truck whether or not there was signal to say so. The queue:

- keeps **one entry per item, last write wins**, so tick-then-untick replays as one final state;
- **flushes when the Lists modal is opened**, before the counts are fetched, or the badge would
  report stale numbers on coming back into signal;
- is **replay-safe** — `setChecklistItemDone` is idempotent, so a tick that actually landed before
  the connection dropped is a no-op on replay, not an error;
- **drops 404s** — an item deleted meanwhile is gone, not something to retry forever.

Items waiting on the queue render with a dashed amber border and the header says
`2 ticks waiting for signal`. **Adds are not queued**: a failed add leaves the typed text in the
box with an error beside it, which loses nothing and avoids inventing client-side ids that would
have to be reconciled later.

## 6. Authorization

| Action | Tier | Why |
|---|---|---|
| `jobChecklists`, `jobChecklist` (GET) | any signed-in role | The crew loading the truck is the audience |
| `createChecklist`, `addChecklistItem`, `setChecklistItemDone`, `deleteChecklistItem` | `_NON_VIEWER` | The crew keeps the list. Removing one line you typed wrong is not destructive |
| `deleteChecklist` | `_ADMIN_OFFICE` | Takes every item with it, and there is no bin |

## 7. Not built

- **Reusable templates** ("standard shop supplies" copied onto a new job). Worth doing if the same
  list gets retyped; premature before anyone has retyped one.
- ~~**Reordering** items by drag.~~ ✅ **Built 2026-08-05 on owner request** — see §9.
- **Assigning** an item to a person, or due dates. That's the Trello furniture this replaces.
- **A PDF.** Explicitly not wanted — this list lives on a phone.

## 8. Traps

1. **Don't FK to `jobs.id`** — see §2. Same trap as panel schedules and time entries.
2. **`position` is explicit, not `created_at`.** Two items typed in the same millisecond would
   otherwise sort arbitrarily, and typed order is load order.
3. **Re-rendering the list wipes the add box.** Ticking an item rebuilds the body, so half-typed
   text is held in `ckAddDraft` across renders and cleared *before* the re-render on a successful
   add — clear it after and the draft is restored into the box you just emptied.
4. **`created_by` / `done_by` are names, not links** — same snapshot reasoning as everywhere else.

## 9. Drag to reorder (built 2026-08-05)

- **Pointer events, not the HTML5 drag-and-drop API.** That API never fires on a touchscreen, and
  this list lives on a phone. Pointer events cover mouse, touch and stylus in one path.
- **`touch-action: none` on the grip** is the line that makes it work on a phone at all — without
  it the browser claims the vertical gesture as a scroll and the drag never starts. Only the grip
  gives that up, so the list still scrolls normally everywhere else.
- **A dedicated grip, not the whole row.** Dragging the row would fight scrolling, and the row
  also carries a checkbox and a delete button — every mis-drag would be a mis-tap on one of them.
- **The real row moves during the drag**, swapping against whichever neighbour's midpoint the
  pointer crossed. No placeholder, so what you see mid-drag is what gets saved.
- **Only open items are draggable.** Once something is on the truck its position is history.
- **The server rewrites positions for every item from the client's full ordered id list**, and
  scopes the UPDATE with `WHERE checklist_id = $1`. Without that scope a crafted id list would
  renumber another list's items — verified against live Neon by smuggling a foreign id into the
  order and confirming it stayed put.
- **A drop that changed nothing doesn't post.** A grip tapped and released is not a reorder.
- **A failed reorder re-fetches** rather than guessing: the screen must never keep showing an
  order the database doesn't have. (Unlike a tick, which queues — order is cosmetic, a tick is
  the record of what's on the truck.)
