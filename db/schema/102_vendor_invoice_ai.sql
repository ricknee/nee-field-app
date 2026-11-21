-- 102: AI job suggestion on supplier invoices that matched no job.
--
-- When an invoice parks on 🧾 Invoice Review (its PO matched no job, or two),
-- Claude reads the PDF and suggests which job it belongs to. It is ONLY a
-- suggestion: nothing here creates an expense. A person clicks "Use this", which
-- goes through the ordinary vendorInvoiceAssign path with all its guards.
-- See netlify/functions/_ai-invoice-match.js.
ALTER TABLE vendor_invoices ADD COLUMN IF NOT EXISTS ai_job_id     uuid REFERENCES jobs(id) ON DELETE SET NULL;
ALTER TABLE vendor_invoices ADD COLUMN IF NOT EXISTS ai_confidence text;   -- high | medium | low | none
ALTER TABLE vendor_invoices ADD COLUMN IF NOT EXISTS ai_reason     text;
ALTER TABLE vendor_invoices ADD COLUMN IF NOT EXISTS ai_read_at    timestamptz;  -- NULL = not read yet
ALTER TABLE vendor_invoices ADD COLUMN IF NOT EXISTS ai_error      text;
