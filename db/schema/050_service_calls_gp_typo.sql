-- 050_service_calls_gp_typo.sql — every service call showed as a loss
--
-- `v_job_financials` and `v_job_financials_true` decided T&M-style revenue with
--
--     job_type = ANY (ARRAY['Time & Material'::text, 'Service Call'::text])
--
-- SINGULAR. The Airtable single-select option — and therefore every row in
-- `jobs.job_type` — is 'Service Calls', PLURAL. The branch could never match, so
-- all 20 service-call jobs fell to the ELSE and took `expected_revenue`, which is
-- 0 for a job with no estimate. Their COST still counted.
--
-- Measured before and after, live:
--
--                    revenue        gross profit     jobs showing a loss
--   before            $0.00          -$3,477.69            20 of 20
--   after           $5,540.11        +$2,062.42             0 of 20
--
-- Time & Material ($416,874.53 / $150,154.06) and Contract ($3,114,195.84 /
-- $1,975,447.58) are unchanged to the cent — that is the check that matters,
-- because it proves the branch that already worked was not disturbed.
--
-- ⚠⚠ THE FIX EDITS THE LIVE DEFINITION, ON PURPOSE. `db/schema/*.sql` view
-- bodies in this repo are STALE — rebuilding a view from a file has already
-- reinstated a fixed OT bug once (006 vs 024). So this takes pg_get_viewdef,
-- replaces exactly one literal, and puts it back. Nothing else in ~9,800
-- characters of definition can drift as a side effect, and the guard makes the
-- statement refuse rather than silently no-op if the literal is not found.
--
-- ⚠ Re-runnable and self-cancelling: after it has run, the singular literal is
-- gone, so a second run raises instead of doing anything.
--
-- ⚠ There is no legitimate 'Service Call' singular anywhere — `jobs.job_type`
-- holds only 'Contract' (78), 'Service Calls' (20) and 'Time & Material' (19).
-- If a singular value ever appears, it is bad data, not a case this should match.

DO $$
DECLARE d text;
BEGIN
  d := pg_get_viewdef('public.v_job_financials'::regclass, true);
  IF position('''Service Call''::text' in d) = 0 THEN
    RAISE EXCEPTION 'v_job_financials: singular literal not found — refusing to touch the view';
  END IF;
  d := replace(d, '''Service Call''::text', '''Service Calls''::text');
  EXECUTE 'CREATE OR REPLACE VIEW public.v_job_financials AS ' || d;
END $$;

DO $$
DECLARE d text;
BEGIN
  d := pg_get_viewdef('public.v_job_financials_true'::regclass, true);
  IF position('''Service Call''::text' in d) = 0 THEN
    RAISE EXCEPTION 'v_job_financials_true: singular literal not found — refusing to touch the view';
  END IF;
  d := replace(d, '''Service Call''::text', '''Service Calls''::text');
  EXECUTE 'CREATE OR REPLACE VIEW public.v_job_financials_true AS ' || d;
END $$;
