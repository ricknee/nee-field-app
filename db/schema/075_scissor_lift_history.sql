-- ── Scissor lift history ───────────────────────────────────────────────────
-- Added 2026-09-14. APPLIED BARE to the default branch of Neon project
-- damp-silence-99074350 (the Neon MCP mangles inline SQL comments), reasoning
-- kept here — same convention as 009.
--
-- Owner's ask: "i need history. be able to see where they were last."
-- scissor_lifts holds only NOW — every save overwrote the job and the person,
-- so the moment a lift left a job nobody could say where it had been.
--
-- One row = a snapshot of the lift AFTER a change. Written by the handlers in
-- the SAME statement as the change (a data-modifying CTE), so a save cannot land
-- without its history row or the other way round. Every write path in the app —
-- the Lifts screen, the job view's Deploy button, the job view's lift card —
-- goes through handleUpdateScissorLift, so there is one place to get this right.
--
-- A row is written only when something that says WHERE THE LIFT IS changes:
-- status, job, person, hooks, box. A notes-only edit updates the lift and adds
-- no row — otherwise the trail fills with typo fixes and "where was it" gets
-- harder to read, not easier. Notes are still captured on every row written.
CREATE TABLE IF NOT EXISTS scissor_lift_history (
  id            bigserial PRIMARY KEY,
  -- CASCADE by the owner's 2026-08-05 rule: selling a lift removes everything.
  lift_id       uuid NOT NULL REFERENCES scissor_lifts(id) ON DELETE CASCADE,
  changed_at    timestamptz NOT NULL DEFAULT now(),
  -- A NAME, resolved server-side by actorName(). The session token carries only
  -- { id, role }, so the client cannot be trusted to say who it is.
  changed_by    text,
  status        text,
  current_job   text,
  assigned_to   text,
  date_deployed date,
  notes         text,
  hooks_left    boolean,
  box_left      boolean,
  -- TRUE on the one row per lift seeded when this table was created. It records
  -- where the lift stood on 2026-09-14, NOT when it got there — nothing before
  -- that date was ever kept. The UI labels it so, rather than as a move.
  is_baseline   boolean NOT NULL DEFAULT false
);

-- Applied with the table: one baseline row per existing lift (10 rows).
-- INSERT INTO scissor_lift_history (lift_id, changed_by, status, current_job,
--        assigned_to, date_deployed, notes, hooks_left, box_left, is_baseline)
-- SELECT l.id, 'History began', l.status, l.current_job, l.assigned_to,
--        l.date_deployed, l.notes, l.hooks_left, l.box_left, true
--   FROM scissor_lifts l
--  WHERE NOT EXISTS (SELECT 1 FROM scissor_lift_history h WHERE h.lift_id = l.id);

-- No index beyond the PK: ~10 lifts, a handful of moves a week. Add
-- (lift_id, id) if this ever shows up in list_slow_queries.
