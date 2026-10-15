// Vendor STATEMENTS (db/schema/083) — pure helpers, no network, no env.
//
// ⛔ Statements are reference documents. Nothing in this module, or in the
// handlers that use it, creates an expense, matches a job or touches
// vendor_invoices. See the schema file for why that is a separate table.

import { createHash } from "node:crypto";

// Two vendors, stored as exactly "CED" and "Wolff" (the schema CHECKs it).
// Same spellings vendor_invoices accepts for these two suppliers, but the
// stored names are NOT the expense_vendors names — nothing here is ever an
// expense, so there is no vendor handle to keep in step with.
const STATEMENT_VENDOR_ALIASES = [
  [/^ced\b|consolidated\s+electrical/i, "CED"],
  [/^wol[f]{1,2}\b|wolff\s+bros/i,      "Wolff"],
];

export const STATEMENT_VENDORS = STATEMENT_VENDOR_ALIASES.map(([, name]) => name);

export function canonicalStatementVendor(v) {
  const s = String(v || "").trim();
  if (!s) return null;
  for (const [re, name] of STATEMENT_VENDOR_ALIASES) if (re.test(s)) return name;
  return null;
}

export const STATEMENT_PDF_MAX_BYTES = 8 * 1024 * 1024;

// Decode and check the PDF. Returns { bytes } or { error }.
// ⚠ The magic number is checked because the hash is the dedupe key: an HTML
// error page or an empty attachment would otherwise be stored as "a statement"
// and its hash would then block nothing useful.
export function decodeStatementPdf(pdfBase64) {
  if (!pdfBase64) return { error: "Missing pdfBase64." };
  let bytes;
  try { bytes = Buffer.from(String(pdfBase64), "base64"); } catch { return { error: "pdfBase64 is not valid base64." }; }
  if (!bytes.length) return { error: "pdfBase64 decoded to nothing." };
  if (bytes.length > STATEMENT_PDF_MAX_BYTES) {
    return { error: `PDF is ${bytes.length} bytes; the limit is ${STATEMENT_PDF_MAX_BYTES}.` };
  }
  if (bytes.subarray(0, 5).toString("latin1") !== "%PDF-") return { error: "That file is not a PDF." };
  return { bytes };
}

export function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

// The statement date, as 'YYYY-MM-DD', or null. Lenient because the bot reads
// it off paper: "2026-09-30", "09/30/2026", "9/30/26". Anything else is null —
// the statement is still kept, it just has no date.
// ⚠ Never `new Date(str)`: "2026-09-30" parses as UTC midnight and lands on the
// 29th west of UTC. Built from the parts instead, and range-checked so
// "02/31/2026" is null rather than quietly March 3rd.
export function statementDateOrNull(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  let y, m, d;
  let mt = /^(\d{4})-(\d{1,2})-(\d{1,2})$/.exec(s);
  if (mt) { [, y, m, d] = mt.map(Number); }
  else if ((mt = /^(\d{1,2})[/-](\d{1,2})[/-](\d{2}|\d{4})$/.exec(s))) {
    [, m, d, y] = mt.map(Number);
    if (y < 100) y += 2000;
  } else return null;
  if (m < 1 || m > 12 || d < 1) return null;
  const dim = new Date(Date.UTC(y, m, 0)).getUTCDate();
  if (d > dim) return null;
  return `${String(y).padStart(4, "0")}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
}

// R2 key for a statement's PDF. Keyed on the Neon uuid; a top-level prefix so
// the recycle bin's `_deleted/` lifecycle rule can never match it.
export function vendorStatementKey(statementId) {
  return `vendor-statements/${String(statementId)}/statement.pdf`;
}

// What a downloaded copy is called. The original name when there is one, else
// "CED statement 2026-09-30.pdf".
export function statementDownloadName(vendor, statementDate, originalFilename) {
  const orig = String(originalFilename || "").trim();
  if (orig) return /\.pdf$/i.test(orig) ? orig : `${orig}.pdf`;
  return `${vendor || "Vendor"} statement${statementDate ? ` ${statementDate}` : ""}.pdf`;
}
