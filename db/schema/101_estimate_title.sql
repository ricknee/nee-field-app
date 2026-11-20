-- 101: a short title on each estimate, set by hand in Est GP.
--
-- Every estimate is named "<type> – <job>", so two Originals on one job read
-- the same (Sullivan Pullet, 2026-11-19). The title is what tells them apart
-- ("Standby generator – 1 unit"). NULL means "not set"; the card then falls back
-- to the first ## heading of the proposal text (notes). Display only: nothing
-- that computes money reads it.
ALTER TABLE job_estimates ADD COLUMN IF NOT EXISTS title text;
