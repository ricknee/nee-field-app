-- 036 — give Neon-native labor allocations a bill rate
-- =============================================================================
-- v_invoices computes an invoice's labor as sum(allocated_hours * bill_rate).
-- In Airtable that column is a lookup through Time Entry → Job, so Airtable
-- fills it and the hourly sync carries the value into Neon. A NEON-NATIVE
-- allocation (schema 033) has no Airtable counterpart to do the filling, so its
-- bill_rate stayed NULL — and a NULL rate makes the product NULL, which sum()
-- skips. Those hours are valued at ZERO in the invoice's own total while
-- printing correctly on the PDF.
--
-- Found on Bethel School invoice 1665 (2026-08-11): 10.75 hours across three
-- native allocations, $698.75 short in invoice_total_calc. Since Step 3 retired
-- Make from the time path on 2026-08-07 every new time entry arrives without an
-- Airtable twin, so every allocation created from one would have been rate-less.
--
-- The write side is fixed in _allocations.js (createLaborAllocation now freezes
-- the job's rate into the native INSERT). This is the backfill for rows already
-- written.
--
-- Scope check before running (2026-08-11): 28 allocations had a NULL bill_rate —
-- 6 native ones on jobs that DO have a rate (3 Bethel School, 3 Shop Work), and
-- 22 mirrored ones on Wayne TWP Fire Dept., a job with no billable rate at all.
-- The predicate below deliberately leaves those 22 alone: a missing rate on a
-- rate-less job is a real finding (see the GP audit's three rate-less T&M jobs),
-- not a gap to paper over, and inventing a number would bill work at a price
-- nobody set.
--
-- Idempotent — re-running touches nothing, because the rows it fixes no longer
-- match `bill_rate IS NULL`.
-- =============================================================================

UPDATE labor_billing_allocations la
   SET bill_rate = j.billable_hourly_rate
  FROM time_entries t
  JOIN jobs j ON j.id = t.job_id
 WHERE la.time_entry_id = t.id
   AND la.airtable_id IS NULL          -- native rows only; Airtable owns the rest
   AND la.bill_rate IS NULL
   AND j.billable_hourly_rate IS NOT NULL;

-- Verification — expect zero native rows left rate-less on a rated job:
--   SELECT count(*) FROM labor_billing_allocations la
--     JOIN time_entries t ON t.id = la.time_entry_id
--     JOIN jobs j ON j.id = t.job_id
--    WHERE la.airtable_id IS NULL AND la.bill_rate IS NULL
--      AND j.billable_hourly_rate IS NOT NULL;
