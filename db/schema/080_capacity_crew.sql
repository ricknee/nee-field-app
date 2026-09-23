-- WHO COUNTS TOWARD CAPACITY — the crew the 📊 Workload view divides the year by.
-- APPLIED to the default branch of Neon project damp-silence-99074350 on 2026-09-23.
--
-- Same convention as the other files here: applied BARE via the Neon MCP
-- (which mangles inline SQL comments), with the reasoning kept here.
--
-- ── WHY A FLAG AND NOT A NUMBER ────────────────────────────────────────────
-- Capacity is people x hours/day x working days left. The tempting versions are
-- both wrong:
--
--   * a typed "4" goes stale the day someone is hired and nobody remembers the
--     box exists;
--   * a hardcoded name list is the SALARIED list in _payroll.js all over again
--     (see CLAUDE.md: a live payroll hazard on its own).
--
-- So capacity counts ACTIVE, payroll-eligible employees (role not viewer/office)
-- and this flag takes individuals OUT. Owner's rule, 2026-09-23: "if I hire
-- somebody, an active employee, I want them to be included in the math."
-- Default TRUE is what delivers that — a new hire is counted the moment they
-- exist, without anyone editing a setting.
--
-- ── WHO IS EXCLUDED TODAY, AND WHY IT IS NOT ABOUT ROLE ────────────────────
-- Larry Unruh. He is active and he can work; he simply is not scheduled, so his
-- hours are not capacity the office can plan against. That is a scheduling fact
-- about one person, not a property of his role (he is admin, as are Miles and
-- Rick, who both count), which is exactly why it cannot be derived and has to
-- be recorded.
--
-- Counted today: Jeff Koehn, Miles Unruh, Patrick Gingerich, Rick Unruh = 4.
-- The flag is editable from the Workload screen's crew list, so the number is
-- never a mystery: the screen names who it counted.

ALTER TABLE employees ADD COLUMN IF NOT EXISTS counts_toward_capacity boolean NOT NULL DEFAULT true;

UPDATE employees SET counts_toward_capacity = false WHERE name = 'Larry Unruh';
