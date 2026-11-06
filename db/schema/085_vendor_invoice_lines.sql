-- 085 — Supplier ticket LINES, read off the invoice PDF.
--
-- vendor_invoices (069) holds one row per ticket: vendor, number, PO, total, and
-- the PDF in R2. The LINES on the ticket — part number, qty, what each one cost —
-- existed only inside that PDF. This table is what the PDF says, line by line,
-- so the inventory app can see what was actually paid for each part.
--
-- Filled by _ticket-lines.js, which the hourly qb-time-pull runs over any ticket
-- whose lines_status is still NULL. It READS: it never creates, changes or
-- prices an expense, and it never touches inventory_items.default_unit_cost.
-- A price only moves when a person taps Update.
--
-- ⚠⚠ ONLY lines_status = 'ok' MAY FEED A PRICE. 'ok' means every line's
-- qty × price matched its own extension AND the lines added up to the ticket's
-- printed total (CED: MERCHANDISE; Wolff: lines + tax = total) AND that total
-- matched the amount already stored on vendor_invoices. A ticket that fails any
-- of those keeps its lines for a human to look at, but a misread PER column
-- ("275.25 C" read as each) is exactly the error those checks exist to stop.

ALTER TABLE vendor_invoices
  ADD COLUMN IF NOT EXISTS lines_status  text
    CHECK (lines_status IN ('ok', 'mismatch', 'unreadable', 'unsupported')),
  ADD COLUMN IF NOT EXISTS lines_read_at timestamptz,
  ADD COLUMN IF NOT EXISTS lines_note    text;

CREATE TABLE IF NOT EXISTS vendor_invoice_lines (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  invoice_id   uuid NOT NULL REFERENCES vendor_invoices(id) ON DELETE CASCADE,
  line_no      integer NOT NULL,
  -- The vendor's own catalogue number (CED "ST300", Wolff "PLC5111"). This is
  -- the key part links will match on — NOT the description, which is free text.
  vendor_part  text NOT NULL,
  -- CED prints a manufacturer/line code above the part ("ORBIT", "SWIRE").
  -- Wolff folds it into the part number, so NULL there.
  mfr          text,
  description  text,
  -- Signed: a credit memo's lines are negative.
  qty          numeric NOT NULL,
  -- 'ea' or 'ft' — what ONE unit of unit_price buys.
  unit         text NOT NULL,
  -- Per ONE unit, always positive, after the PER divisor is applied.
  -- CED "275.25 C" is stored as 2.7525. Wolff prints per-unit already.
  unit_price   numeric NOT NULL,
  -- As printed, kept so a reader can see the arithmetic: price_raw / per.
  price_raw    numeric,
  price_per    text,
  extension    numeric NOT NULL,
  -- Wolff bills a $0.00 "CUT CHARGE" line beside every cut of wire. Kept so
  -- the ticket still adds up, but it is never a price for anything.
  is_charge    boolean NOT NULL DEFAULT false,
  UNIQUE (invoice_id, line_no)
);

CREATE INDEX IF NOT EXISTS vendor_invoice_lines_part_idx ON vendor_invoice_lines (vendor_part);
CREATE INDEX IF NOT EXISTS vendor_invoices_lines_todo_idx
  ON vendor_invoices (received_at) WHERE lines_status IS NULL AND pdf_key IS NOT NULL;
