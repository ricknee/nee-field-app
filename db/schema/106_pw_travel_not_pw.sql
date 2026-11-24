-- 106 — Travel on a prevailing-wage job is NOT prevailing wage (owner 2026-11-24).
-- Travel to/from a PW job (Seneca TWP Building) is paid at the person's normal
-- rate. It still costs to that job; it just isn't priced at the PW rate, and it
-- doesn't make the week a "PW week".
--
-- Edits the LIVE view text (pg_get_viewdef) rather than restating it, because
-- the .sql copies of these views are stale. Each replace must hit exactly once.

DO $$
DECLARE d text; n int;
BEGIN
  -- 1. Job cost: is_pw is false for Travel entries.
  d := pg_get_viewdef('v_job_labor_cost_true'::regclass, true);
  n := (length(d) - length(replace(d, 'j.prevailing_wage IS TRUE AS is_pw', ''))) / length('j.prevailing_wage IS TRUE AS is_pw');
  IF n <> 1 THEN RAISE EXCEPTION 'v_job_labor_cost_true: expected 1 match, got %', n; END IF;
  d := replace(d, 'j.prevailing_wage IS TRUE AS is_pw',
       $r$j.prevailing_wage IS TRUE AND COALESCE(t.class, ''::text) <> 'Travel'::text AS is_pw$r$);
  EXECUTE 'CREATE OR REPLACE VIEW v_job_labor_cost_true AS ' || d;

  -- 2. Week reconcile: same definition of a PW hour.
  d := pg_get_viewdef('v_pw_week_reconcile'::regclass, true);
  n := (length(d) - length(replace(d, 'j.prevailing_wage IS TRUE AS is_pw', ''))) / length('j.prevailing_wage IS TRUE AS is_pw');
  IF n <> 1 THEN RAISE EXCEPTION 'v_pw_week_reconcile: expected 1 match, got %', n; END IF;
  d := replace(d, 'j.prevailing_wage IS TRUE AS is_pw',
       $r$j.prevailing_wage IS TRUE AND COALESCE(t.class, ''::text) <> 'Travel'::text AS is_pw$r$);
  EXECUTE 'CREATE OR REPLACE VIEW v_pw_week_reconcile AS ' || d;

  -- 3. QB-vs-job disagreement: travel logged as ordinary LABOR on a PW job is right.
  d := pg_get_viewdef('v_pw_source_disagreement'::regclass, true);
  n := (length(d) - length(replace(d, 'WHERE t.work_date > ', ''))) / length('WHERE t.work_date > ');
  IF n <> 1 THEN RAISE EXCEPTION 'v_pw_source_disagreement: expected 1 match, got %', n; END IF;
  d := replace(d, 'WHERE t.work_date > ',
       $r$WHERE COALESCE(t.class, ''::text) <> 'Travel'::text AND t.work_date > $r$);
  EXECUTE 'CREATE OR REPLACE VIEW v_pw_source_disagreement AS ' || d;
END $$;
