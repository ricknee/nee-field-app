-- 073_qb_service_item.sql — record QuickBooks Time's Service Item, so the two
-- systems' answers about prevailing wage can be COMPARED rather than assumed to
-- agree.
--
-- ── WHAT THIS IS FOR, AND WHAT IT IS NOT FOR ───────────────────────────────
-- ⛔⛔ NOTHING MAY EVER PRICE AN HOUR FROM THIS COLUMN. It is a cross-check and
-- only a cross-check.
--
-- Prevailing wage is decided by the JOB (`jobs.prevailing_wage`, db/schema/072).
-- That is deliberate: the crew already picks a job, and a second answer they can
-- pick independently produces the most dangerous mismatch available — the right
-- hours on the right job at the wrong cost. If this column ever starts deciding
-- anything, that property is gone.
--
-- What it IS for: QuickBooks pays and certifies prevailing wage off its own
-- Service Item, and this app costs it off the job flag. Two systems, two
-- switches, no shared enforcement. Recording QB's answer lets the hourly
-- integrity run ask "do they still agree?" — see _integrity.js
-- `pw-disagrees-with-quickbooks`.
--
-- ── WHAT QUICKBOOKS ACTUALLY HAS (probed live 2026-09-12) ──────────────────
-- Five timesheet custom fields; the pull reads two of them.
--
--   65840  Taxes         -> city_taxes      ⚠ SEE THE TRAP BELOW
--   71181  Billable      -> ignored (billable is hardcoded true, Make parity)
--   71183  Service Item  -> service_item    ← THIS FILE. Options: exactly
--                                             'LABOR' and 'LABOR:PREVAILING WAGE'
--   71185  Class         -> class AND labor_type (one field, two columns)
--   71833  Job Services  -> ignored
--
-- ⚠⚠ THE TRAP, AND IT IS WHY THE SECOND CHECK EXISTS. 'Prevailing Wage' is ALSO
-- an option in the **Taxes** list (65840) — sitting among Alliance Tax, Canton
-- Tax and Massilon Tax. That field DOES flow into this app, as `city_taxes`,
-- which drives payroll's city tax. A wage classification picked there becomes a
-- city tax, and the person picking it would be doing something that looks
-- entirely reasonable: two dropdowns offer a prevailing-wage-ish answer and only
-- one of them is right.
--
-- Measured 2026-09-12: **0 time entries have ever used it**, and it is absent
-- from the app's own PR_CITY_TAX_OPTS so the app would reject it on its own
-- writes. But the QB pull does not validate against that list — it takes what QB
-- sends. Deleting or renaming that option in QuickBooks is the real fix; the
-- integrity check is the tripwire until someone does.

ALTER TABLE time_entries         ADD COLUMN service_item text;
ALTER TABLE time_entries_deleted ADD COLUMN service_item text;

COMMENT ON COLUMN time_entries.service_item IS
  'QuickBooks Time custom field 71183, verbatim: ''LABOR'' or ''LABOR:PREVAILING WAGE''. RECORDED FOR COMPARISON ONLY — nothing prices an hour from it, and nothing may start to. Prevailing wage is decided by jobs.prevailing_wage (db/schema/072); this is QuickBooks'' independent answer, kept so the hourly integrity run can detect the two drifting apart. See db/schema/073.';

-- Where the two answers are compared. A view rather than inline SQL so the
-- definition is inspectable on its own and the integrity check stays readable.
--
-- ⚠ Scoped to a 45-day window and to entries that actually carry a service item,
-- matching the house rule at the top of _integrity.js: a clean system reports
-- ZERO. Every row that predates the Service Item being used at all would
-- otherwise fire forever, and a check that always fires is how the one that
-- matters gets ignored.
CREATE OR REPLACE VIEW v_pw_source_disagreement AS
SELECT t.id,
       t.employee_name,
       t.work_date,
       t.hours,
       t.job_name,
       t.service_item,
       j.name AS job,
       j.prevailing_wage,
       CASE
         WHEN t.service_item ILIKE '%prevail%' AND NOT j.prevailing_wage
           THEN 'quickbooks says prevailing wage, the job is not flagged'
         WHEN t.service_item IS NOT NULL
          AND t.service_item NOT ILIKE '%prevail%' AND j.prevailing_wage
           THEN 'the job is flagged prevailing wage, quickbooks says ordinary labor'
       END AS disagreement
  FROM time_entries t
  JOIN jobs j ON j.id = t.job_id
 WHERE t.work_date > current_date - 45
   AND t.service_item IS NOT NULL
   AND ((t.service_item ILIKE '%prevail%') <> j.prevailing_wage);

COMMENT ON VIEW v_pw_source_disagreement IS
  'Hours where QuickBooks Time''s Service Item and jobs.prevailing_wage disagree about whether the hour is prevailing wage. Neither side is authoritative here BY DESIGN — the app costs off the job, QuickBooks pays off the service item, and this view exists to notice when a human needs to reconcile them. 45-day window so a clean system reports zero.';
