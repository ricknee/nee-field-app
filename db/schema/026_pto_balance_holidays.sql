-- Neon slice — show paid holidays alongside PTO on the balance view.
--
-- Applied BARE via the Neon MCP; this file is the annotated source of truth.
--
-- v_pto_balances counted `class = 'PTO'` only, which is right for a BALANCE —
-- holidays are not drawn from an allowance, they are simply given. But it left the
-- admin roster unable to answer half of the obvious question ("how much time off
-- has this person had, paid?"), so the holiday figure is carried alongside rather
-- than folded in.
--
-- ⚠ holiday_hours is REPORTING ONLY. It must never be subtracted from
-- remaining_hours — a paid holiday costs nobody any of their two weeks, and
-- deducting it would quietly take six days a year off everyone's entitlement.
CREATE OR REPLACE VIEW v_pto_balances AS
SELECT p.employee_id,
       e.airtable_id,
       e.name,
       p.year,
       p.allowance_hours,
       p.carried_in_hours,
       p.allowance_hours + p.carried_in_hours                             AS entitled_hours,
       coalesce(u.used_hours, 0)                                          AS used_hours,
       p.allowance_hours + p.carried_in_hours - coalesce(u.used_hours, 0) AS remaining_hours,
       coalesce(u.holiday_hours, 0)                                       AS holiday_hours
  FROM pto_years p
  JOIN employees e ON e.id = p.employee_id
  LEFT JOIN (
    SELECT employee_id,
           EXTRACT(YEAR FROM work_date)::int AS yr,
           sum(hours) FILTER (WHERE class = 'PTO')          AS used_hours,
           sum(hours) FILTER (WHERE class = 'Paid Holiday') AS holiday_hours
      FROM time_entries
     WHERE class IN ('PTO', 'Paid Holiday')
       AND employee_id IS NOT NULL
     GROUP BY 1, 2
  ) u ON u.employee_id = p.employee_id AND u.yr = p.year;
