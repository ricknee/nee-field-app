-- Neon slice — adjusting punch start/stop times, with an audit trail.
--
-- Applied BARE via the Neon MCP (which mangles inline comments); this file is the
-- annotated source of truth. Same convention as 001/002/018/019/020.
--
-- Owner, 2026-08-08: "i arrive at the job at 7am but forget to click in until 8am
-- i would like to be able to edit that start time."
--
-- This is the single most common failure of any time clock, and refusing to support
-- it doesn't make the data more honest — it makes people write the hour down
-- somewhere else and hand it to payroll on paper, which is strictly worse.
--
-- ── WHAT MAKES THIS SAFE RATHER THAN AN HONESTY BOX ──────────────────────────
--
-- Editing your own start time moves your own pay. Three things keep that honest,
-- and none of them is "trust":
--
--   1. WHO. An employee may edit their OWN punches, and nobody else's. Admin may
--      edit anyone's.
--
--      ⚠ There is NO payroll cutoff — owner's call, "all people need to be able to
--      edit time". An employee can correct a shift that has already been counted.
--      This deliberately DIFFERS from the expenses rule (own-until-approved):
--      expenses freeze on approval because someone else signed them off, whereas a
--      wrong clock-in is wrong whenever it is noticed.
--
--      The consequence is that editing a promoted punch MUST also correct the
--      time_entries row it produced, in the SAME statement — otherwise the clock
--      and payroll hold two versions of one shift and the wrong one is what people
--      are paid from. See handleClockEditTimes; it is one statement over two
--      tables for exactly this reason, and it logs loudly if the payroll row did
--      not move. The 14-day bound is what stops any of this reaching a genuinely
--      closed pay period.
--
--   2. WHAT IT LOOKED LIKE BEFORE. The columns below keep the ORIGINAL punch
--      timestamps forever, set once on the first edit and never overwritten after.
--      So "the entry says 7:00 but the phone was actually tapped at 8:04" stays
--      answerable a year later. Without this the edit is invisible and the feature
--      really would be an honesty box.
--
--   3. WHO CHANGED IT AND WHEN. edited_by / edited_at, so a pattern of someone
--      always adjusting their own start is visible rather than folklore.
--
-- Bounds live in the handler, not here: not in the future, start before end, and
-- within a sane window. A CHECK constraint would be the wrong place — a rejected
-- edit must return a message a person can act on, not a constraint violation.

-- ── The open shift ───────────────────────────────────────────────────────────
-- Only the start can be wrong on a shift that hasn't ended yet, so that is the
-- only original worth keeping here.
ALTER TABLE open_punches ADD COLUMN IF NOT EXISTS original_started_at timestamptz;
ALTER TABLE open_punches ADD COLUMN IF NOT EXISTS edited_at timestamptz;
ALTER TABLE open_punches ADD COLUMN IF NOT EXISTS edited_by uuid REFERENCES employees(id);

-- ── The completed punch ──────────────────────────────────────────────────────
-- ⚠ On punch-out these must CARRY OVER from the open shift, or an edit made while
-- clocked in would lose its own audit trail the moment the shift ended. See the
-- carry-over in handleClockOut's INSERT.
ALTER TABLE clock_punches ADD COLUMN IF NOT EXISTS original_started_at timestamptz;
ALTER TABLE clock_punches ADD COLUMN IF NOT EXISTS original_ended_at   timestamptz;
ALTER TABLE clock_punches ADD COLUMN IF NOT EXISTS edited_at timestamptz;
ALTER TABLE clock_punches ADD COLUMN IF NOT EXISTS edited_by uuid REFERENCES employees(id);

COMMENT ON COLUMN clock_punches.original_started_at IS
  'The timestamp actually punched, before any human adjustment. NULL means never '
  'edited. Set once on the first edit and never overwritten, so the real punch time '
  'stays answerable however many times the row is later corrected.';

-- Finds edited punches for review — "show me every shift where someone moved their
-- own start time". Partial, because edits should be the small minority; if this
-- index ever stops being small, that is itself worth knowing.
CREATE INDEX IF NOT EXISTS clock_punches_edited_idx
  ON clock_punches (edited_at DESC) WHERE edited_at IS NOT NULL;
