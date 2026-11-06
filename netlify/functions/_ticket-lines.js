// ── Supplier ticket LINES — read off the invoice PDF (db/schema/085) ─────────
//
// Every CED and Wolff ticket already arrives through vendorInvoiceIntake with its
// PDF, stored in R2 under vendor_invoices.pdf_key. This module opens that PDF and
// writes what each LINE says — part number, qty, what one unit cost — into
// vendor_invoice_lines, so the inventory app can compare its cost on file with
// what was actually paid.
//
// WHY IT EXISTS (measured 2026-11-06, 147 ticket lines vs inventory_items):
// a third to half of everyday parts were off by 10–30% in BOTH directions, and
// heavy copper was ~40% low. The errors cancel in job totals, so nothing looked
// wrong. Return credits on Push Expenses are valued at the cost on file, so a
// stale cost is also a wrong credit.
//
// ⛔ THIS MODULE ONLY READS. It never creates, changes or reprices an expense, and
// it never writes inventory_items.default_unit_cost. A price moves only when a
// person taps Update in the inventory app.
//
// HOW IT IS SAFE TO TRUST — a ticket's lines are only `ok` when ALL of:
//   1. every line's |qty| × unit price rounds to its own printed extension,
//   2. the lines add up to the ticket's printed total
//      (CED: MERCHANDISE subtotal; Wolff prints none, so lines + tax = total),
//   3. that total equals the amount ALREADY stored on vendor_invoices — so the
//      PDF really is the ticket the row describes.
// Check 1 is what catches the trap that matters: CED's PER column. "275.25 C" is
// $2.7525 each, not $275.25. Read it wrong and the extension stops matching.
//
// HOW THE TEXT IS READ: no OCR and no AI. Both vendors' PDFs carry a real text
// layer at fixed positions, so each word comes with its x/y and the columns are
// fixed x-ranges. The parsers below are pure functions over
// [{ x, y, s }] per page; only extractPdfPages() needs the PDF library, and it is
// lazy-imported (like aws4fetch in _r2.js) so the test suite stays offline.
// ⚠ A vendor changing its invoice layout makes every ticket fail check 2 and land
// as 'mismatch' — loud on ticketLinesStatus, never a wrong price.

// ── Shared ────────────────────────────────────────────────────────────────────

// Groups positioned words into rows, top of page first. Words on one printed
// line share a baseline to within a point.
export function toRows(items) {
  const rows = [];
  for (const it of items) {
    const y = Math.round(it.y * 10) / 10;
    let row = rows.find(r => Math.abs(r.y - y) <= 1.5);
    if (!row) { row = { y, its: [] }; rows.push(row); }
    row.its.push(it);
  }
  for (const r of rows) r.its.sort((a, b) => a.x - b.x);
  return rows.sort((a, b) => b.y - a.y);
}
const inX  = (row, lo, hi) => row.its.filter(i => i.x >= lo && i.x < hi);
const text = (row, lo, hi) => inX(row, lo, hi).map(i => i.s).join(" ").trim();
const r2   = n => Math.round(n * 100) / 100;

// A money/qty token. Handles "1,063.08", ".9885" (Wolff drops the leading zero),
// "-23.50" and Wolff's TRAILING minus on credits ("773.85-").
export function num(s) {
  let t = String(s ?? "").replace(/,/g, "").trim();
  let neg = false;
  if (t.endsWith("-")) { neg = true; t = t.slice(0, -1); }
  if (t.startsWith("-")) { neg = !neg; t = t.slice(1); }
  if (!/^(\d+(\.\d*)?|\.\d+)$/.test(t)) return null;
  const n = Number(t);
  return neg ? -n : n;
}
const firstNum = (row, lo, hi) => {
  for (const i of inX(row, lo, hi)) { const n = num(i.s); if (n !== null) return n; }
  return null;
};

// What one printed price buys. CED and Wolff both use C = per 100, M = per 1000.
const PER_DIVISOR = { E: 1, EA: 1, FT: 1, C: 100, M: 1000 };

function lineChecks(qty, unitPrice, ext) {
  return Math.abs(r2(Math.abs(qty) * unitPrice) - Math.abs(ext)) <= 0.015;
}

// ── CED ──────────────────────────────────────────────────────────────────────
// Layout (Activant/Epicor "Invoice" / "Credit Memo", letter portrait, pt):
//   qty ordered x 30–72 · product code x 72–200 (mfr on the row, PART NUMBER on
//   the line below) · description x 200–400 · qty shipped x 400–455 ·
//   price x 455–493 · PER x 493–503 (E/C/M) · disc x 503–540 ·
//   extension x 540–586 · C/D x 586+.
// Totals block: label at x≈433 (MERCHANDISE / SALES TAX / TOTAL DUE), value at
// x 540+. On a Credit Memo qty AND price print negative.
export function parseCedPages(pages) {
  const out = { vendor: "CED", lines: [], merchandise: null, tax: null, total: null,
                invoiceNo: null, invoiceDate: null, po: null, isCredit: false };
  for (const items of pages) {
    const rows = toRows(items);
    for (let k = 0; k < rows.length; k++) {
      const row = rows[k];
      const all = text(row, 0, 1000);
      if (row.y > 700 && /\bCredit Memo\b/.test(all)) out.isCredit = true;
      if (!out.invoiceNo) {
        const inv = row.its.find(i => /^\d{4}-\d{7}$/.test(i.s));
        if (inv) {
          out.invoiceNo = inv.s;
          const d = row.its.find(i => /^\d\d\/\d\d\/\d{4}$/.test(i.s));
          if (d) out.invoiceDate = d.s;
        }
      }
      if (!out.po && /CUSTOMER ORDER NO\./.test(all)) {
        const below = rows.find(r => r.y < row.y && r.y > row.y - 20 && inX(r, 480, 620).length);
        if (below) out.po = text(below, 480, 620) || null;
      }
      const label = text(row, 425, 500);
      if (/^MERCHANDISE/.test(label)) out.merchandise = firstNum(row, 540, 620);
      if (/^SALES TAX/.test(label))   out.tax = firstNum(row, 540, 620);
      if (/^TOTAL DUE/.test(label)) {
        // The value sits a couple of points off its label's baseline.
        for (const r of rows.filter(r => Math.abs(r.y - row.y) <= 4)) {
          const v = firstNum(r, 540, 620);
          if (v !== null) { out.total = v; break; }
        }
      }

      const qty   = firstNum(row, 400, 455);
      const price = firstNum(row, 455, 493);
      const per   = inX(row, 493, 503).map(i => i.s).find(s => s === "E" || s === "C" || s === "M");
      const ext   = firstNum(row, 540, 586);
      if (qty === null || price === null || !per || ext === null) continue;
      const next  = rows[k + 1];
      const part  = next && row.y - next.y < 14 ? text(next, 72, 200) : "";
      const unitPrice = Math.round((Math.abs(price) / PER_DIVISOR[per]) * 1e6) / 1e6;
      out.lines.push({
        vendorPart: part, mfr: text(row, 72, 200) || null, description: text(row, 200, 400),
        qty, unit: "ea", unitPrice, priceRaw: price, pricePer: per, extension: ext,
        isCharge: false, check: lineChecks(qty, unitPrice, ext),
      });
    }
  }
  // CED prints no unit-of-measure column; a conduit or wire line is priced by the
  // foot. Product codes COND/WIRE are the two that sell by length.
  for (const l of out.lines) if (/^(COND|WIRE)$/.test(l.mfr || "")) l.unit = "ft";
  out.lineSum = r2(out.lines.reduce((s, l) => s + l.extension, 0));
  out.printedTotal = out.total;
  out.sumOk = out.merchandise !== null && Math.abs(out.lineSum - out.merchandise) <= 0.015;
  return out;
}

// ── Wolff Brothers ───────────────────────────────────────────────────────────
// Layout ("INVOICE", one printed row per line):
//   shipped x 20–80 · B/O x 80–110 · UM x 110–135 · item (part) x 135–238 ·
//   description x 238–440 · price x 440–500 (already per ONE unit, 4dp) ·
//   PU x 500–525 · amount x 525+.
// Notes like "1x60 foot coil" and "Please return any empty reels" have no qty
// and are skipped. Totals: SALES TAX label, then a row below with tax and
// PAY THIS AMOUNT. There is no merchandise subtotal.
const WOLFF_UM = new Set(["EA", "FT", "C", "M", "LB", "BX", "PK", "RL", "CS", "PR", "ST", "BG"]);
export function parseWolffPages(pages) {
  const out = { vendor: "Wolff Brothers", lines: [], merchandise: null, tax: null, total: null,
                invoiceNo: null, invoiceDate: null, po: null, isCredit: false };
  for (const items of pages) {
    const rows = toRows(items);
    for (const row of rows) {
      if (row.its.some(i => i.s === "CUSTOMER NO.")) {
        const v = rows.find(r => r.y < row.y && r.y > row.y - 20);
        if (v) {
          if (!out.po) out.po = text(v, 70, 250) || null;
          const m = text(v, 370, 520).match(/(\d{6,}-\d{3})\s+(\d\d\/\d\d\/\d{4})/);
          if (m && !out.invoiceNo) { out.invoiceNo = m[1]; out.invoiceDate = m[2]; }
        }
      }
      if (row.its.some(i => i.s === "SALES TAX")) {
        const v = rows.find(r => r.y < row.y && r.y > row.y - 30 &&
                                 inX(r, 470, 620).some(i => num(i.s) !== null));
        if (v) {
          const ns = inX(v, 470, 620).map(i => num(i.s)).filter(n => n !== null);
          if (ns.length >= 2) { out.tax = ns[0]; out.total = ns[ns.length - 1]; }
        }
      }
      const shipped = firstNum(row, 20, 80);
      const um      = inX(row, 110, 135).map(i => i.s).find(s => WOLFF_UM.has(s));
      const part    = text(row, 135, 238);
      const price   = firstNum(row, 440, 500);
      const pu      = inX(row, 500, 525).map(i => i.s).find(s => WOLFF_UM.has(s));
      const amount  = firstNum(row, 525, 620);
      if (shipped === null || !um || !part || price === null || !pu || amount === null) continue;
      const description = text(row, 238, 440);
      const unitPrice = Math.round((Math.abs(price) / (PER_DIVISOR[pu] || 1)) * 1e6) / 1e6;
      out.lines.push({
        vendorPart: part, mfr: null, description, qty: shipped,
        unit: um === "FT" ? "ft" : "ea", unitPrice, priceRaw: price, pricePer: pu, extension: amount,
        // Cut charges and delivery are on the ticket but are not a price for a part.
        isCharge: /^LAB/.test(part) || /^CUT CHARGE/.test(description) || /^FRT/.test(part),
        check: lineChecks(shipped, unitPrice, amount),
      });
    }
  }
  if (out.lines.some(l => l.qty < 0) && out.lines.every(l => l.qty <= 0)) out.isCredit = true;
  out.lineSum = r2(out.lines.reduce((s, l) => s + l.extension, 0));
  out.printedTotal = out.total;
  out.sumOk = out.total !== null && out.tax !== null &&
              Math.abs(r2(out.lineSum + out.tax) - out.total) <= 0.015;
  return out;
}

export const TICKET_PARSERS = { "CED": parseCedPages, "Wolff Brothers": parseWolffPages };

// The verdict for one parsed ticket against the amount vendor_invoices already
// holds (signed: a credit is negative). Returns { status, note }.
export function judgeTicket(parsed, storedAmount) {
  if (!parsed.lines.length) return { status: "unreadable", note: "no item lines found" };
  const bad = parsed.lines.filter(l => !l.check);
  if (bad.length) return { status: "mismatch",
    note: `${bad.length} line(s) where qty × price ≠ extension, e.g. ${bad[0].vendorPart}` };
  if (!parsed.sumOk) return { status: "mismatch",
    note: `lines sum ${parsed.lineSum} does not match the printed total` };
  if (storedAmount != null && parsed.printedTotal != null &&
      Math.abs(Math.abs(Number(storedAmount)) - Math.abs(parsed.printedTotal)) > 0.015) {
    return { status: "mismatch",
      note: `PDF total ${parsed.printedTotal} ≠ stored amount ${storedAmount} — is this the right PDF?` };
  }
  return { status: "ok", note: `${parsed.lines.length} lines, total ${parsed.printedTotal}` };
}

// ── PDF → positioned words ───────────────────────────────────────────────────
// unpdf is pdf.js packaged for serverless (no canvas, no worker). Lazy-imported:
// declared in netlify/functions/package.json AND listed in netlify.toml's
// external_node_modules, or the import fails at runtime — the same trap that
// cost three days on the Neon driver (CLAUDE.md).
export async function extractPdfPages(bytes) {
  const { getDocumentProxy } = await import("unpdf");
  // ⚠ A Node Buffer IS a Uint8Array subclass, and pdf.js refuses it by name — so
  // `instanceof Uint8Array` is not enough. Always hand it a plain copy.
  const pdf = await getDocumentProxy(Uint8Array.from(bytes));
  const pages = [];
  for (let p = 1; p <= pdf.numPages; p++) {
    const tc = await (await pdf.getPage(p)).getTextContent();
    pages.push(tc.items.filter(i => i.str && i.str.trim())
      .map(i => ({ x: i.transform[4], y: i.transform[5], s: i.str.trim() })));
  }
  return pages;
}

// ── Runner ───────────────────────────────────────────────────────────────────
// Reads up to `limit` tickets whose lines have not been read yet, oldest first.
//   q(text, params) → rows, THROWS on failure (neonWrite / sql.query).
//   getBytes(key)   → Uint8Array (getObjectBytes from _r2.js).
// One bad ticket never stops the batch: it is marked 'unreadable' with the reason
// and the next one is read. Each ticket's lines and its status land in ONE
// statement, so a ticket is never left half-written.
//
// ⚠ `budgetMs` is a wall-clock cap, not a nicety. This runs inside the hourly
// qb-time-pull, whose timesheet pull is the job that must not stop, and Netlify
// kills a scheduled function at ~30 s. A ticket takes ~0.2–0.5 s (R2 GET + parse),
// so the backlog of ~130 drains over a few hours rather than in one risky run.
export async function readPendingTicketLines(q, getBytes, { limit = 40, budgetMs = 8000 } = {}) {
  const t0 = Date.now();
  const todo = await q(
    `SELECT id, vendor_name, invoice_no, amount, pdf_key
       FROM vendor_invoices
      WHERE lines_status IS NULL AND pdf_key IS NOT NULL AND status <> 'deleted'
      ORDER BY received_at
      LIMIT $1`, [limit]);
  const report = { read: 0, ok: 0, mismatch: 0, unreadable: 0, unsupported: 0, problems: [] };
  for (const inv of todo) {
    if (Date.now() - t0 > budgetMs) { report.stoppedForTime = true; break; }
    let status, note, lines = [];
    const parser = TICKET_PARSERS[inv.vendor_name];
    if (!parser) {
      status = "unsupported"; note = `no reader for ${inv.vendor_name}`;
    } else {
      try {
        const parsed = parser(await extractPdfPages(await getBytes(inv.pdf_key)));
        ({ status, note } = judgeTicket(parsed, inv.amount));
        lines = parsed.lines;
      } catch (e) {
        status = "unreadable"; note = String(e?.message || e).slice(0, 300);
      }
    }
    try {
      await q(
        `WITH ins AS (
           INSERT INTO vendor_invoice_lines
             (invoice_id, line_no, vendor_part, mfr, description, qty, unit,
              unit_price, price_raw, price_per, extension, is_charge)
           SELECT $1::uuid, t.n, t.part, t.mfr, t.descr, t.qty, t.unit,
                  t.unit_price, t.price_raw, t.price_per, t.ext, t.is_charge
             FROM unnest($2::int[], $3::text[], $4::text[], $5::text[], $6::numeric[], $7::text[],
                         $8::numeric[], $9::numeric[], $10::text[], $11::numeric[], $12::boolean[])
                  AS t(n, part, mfr, descr, qty, unit, unit_price, price_raw, price_per, ext, is_charge)
           ON CONFLICT (invoice_id, line_no) DO NOTHING
           RETURNING 1)
         UPDATE vendor_invoices
            SET lines_status = $13, lines_note = $14, lines_read_at = now()
          WHERE id = $1::uuid`,
        [inv.id,
         lines.map((_, i) => i + 1), lines.map(l => l.vendorPart), lines.map(l => l.mfr),
         lines.map(l => l.description), lines.map(l => l.qty), lines.map(l => l.unit),
         lines.map(l => l.unitPrice), lines.map(l => l.priceRaw), lines.map(l => l.pricePer),
         lines.map(l => l.extension), lines.map(l => l.isCharge),
         status, note]);
    } catch (e) {
      // The write failed: leave lines_status NULL so the next run retries it.
      report.problems.push(`${inv.vendor_name} ${inv.invoice_no}: write failed — ${String(e?.message || e).slice(0, 200)}`);
      continue;
    }
    report.read++;
    report[status]++;
    if (status !== "ok") report.problems.push(`${inv.vendor_name} ${inv.invoice_no}: ${status} — ${note}`);
  }
  // A full batch means more may be waiting; the next run picks them up.
  report.moreWaiting = todo.length === limit || !!report.stoppedForTime;
  return report;
}
