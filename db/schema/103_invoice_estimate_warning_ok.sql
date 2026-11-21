-- "OK, I know" on the invoice/estimate warning (owner 2026-11-21).
-- Holds the warning KEYS someone okayed (see _invoice-estimate-check.js).
-- A key carries the numbers, so a warning whose numbers change comes back.
-- Not money: nothing reads it except the warning check.
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS estimate_warning_ok text[];
