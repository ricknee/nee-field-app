// Vendor-invoice inbox — normalisation and PO matching.
// See db/schema/069_vendor_invoices.sql for the table and the reasoning.
// ---------------------------------------------------------------------------
// This file holds the two pieces of logic that decide whether real money lands
// on a job automatically or waits for a person, so they live apart from the
// 200 KB dispatcher and are covered by tests/handlers.test.mjs directly.
//
// CONTRACT: nothing here touches the network. `matchJobByPoCode` takes the rows
// a caller already read; the SQL that produces them is exported as a string so
// the expression exists in exactly one place.

// Reduce a PO to its comparable form: A–Z0–9 only, uppercased.
//
// ⚠ THE TWO VENDORS DISAGREE ABOUT SPACING and neither is wrong. PDF.co's CED
// template reads "CAJ 436"; its Wolff template reads "CAJ436" off the same
// job's paperwork. `jobs.po_locked` spells it "Joe Yoder (CAJ 436)". Comparing
// any two of those literally matches nothing — and matching nothing is this
// system's characteristic silent failure, because it reads as "no such job"
// rather than as an error.
export function normalizePoCode(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).replace(/[^A-Za-z0-9]/g, "").toUpperCase();
  return s || null;
}

// A job's PO code, reduced the same way, as a SQL expression.
//
// `po_locked` is the locked Job PO string; `po` is the unlocked one, and the
// COALESCE is not cosmetic — 14 jobs (all New Lead / Not Awarded) have only
// `po`, and those are exactly the jobs young enough to still be buying
// material. Reading po_locked alone would park every one of their invoices.
//
// The `substring(… from '\(([^)]*)\)')` pulls the code out of the parentheses.
// The outer COALESCE falls back to the whole string for a PO that was never
// written in that shape, and to '' so a job with no PO at all reduces to NULL
// rather than to something an invoice could accidentally equal.
export const JOB_PO_CODE_SQL = `
  NULLIF(upper(regexp_replace(
    COALESCE(substring(COALESCE(j.po_locked, j.po) from '\\(([^)]*)\\)'),
             COALESCE(j.po_locked, j.po), ''),
    '[^A-Za-z0-9]', '', 'g')), '')`;

// Decide what happens to an invoice, given the job rows whose PO code equals
// this invoice's. Returns { jobId, reason } — jobId non-null ONLY on a single
// unambiguous hit.
//
// ⚠⚠ TWO MATCHES PARK. They do not get the first row, or the most recent job,
// or the one that is still open. "Harlin Smith (2 Barn)" and "Rebecca Smith
// (2 Barn)" both reduce to 2BARN in production today, so this is a live case,
// not a defensive one. Picking between them silently charges one customer's
// material to another customer's job, where nothing ever surfaces it: the job
// still has a plausible cost, the GP is merely wrong, and the invoice looks
// filed. A person can resolve it in ten seconds from the screen. Code cannot
// resolve it at all.
export function matchJobByPoCode(poCode, candidateRows) {
  if (!poCode) return { jobId: null, reason: "no-po-on-invoice" };
  const rows = Array.isArray(candidateRows) ? candidateRows : [];
  if (rows.length === 0) return { jobId: null, reason: "no-job-match" };
  if (rows.length > 1)   return { jobId: null, reason: "ambiguous-po" };
  return { jobId: rows[0].id, reason: "auto" };
}

// Money off an invoice, signed.
//
// ⚠ WOLFF PRINTS CREDIT MEMOS WITH A TRAILING MINUS — "773.85-", and the total
// on a full credit invoice is "-" on every line. `Number("773.85-")` is NaN,
// and a NaN that reaches the expense would be stored as NULL, which the GP
// views read as "no material cost on this invoice" — a returned $773 of gear
// silently costing the job nothing back. Leading minus, trailing minus,
// parenthesised and thousands separators all normalise here.
//
// Returns null (not 0) when there is no number to be had: NULL and 0 are
// different to the GP views, and the expense columns are nullable on purpose.
export function parseSignedAmount(v) {
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  let s = String(v).trim();
  if (!s) return null;
  let negative = false;
  if (/^\(.*\)$/.test(s)) { negative = true; s = s.slice(1, -1); }
  if (s.endsWith("-"))    { negative = true; s = s.slice(0, -1); }
  if (s.startsWith("-"))  { negative = true; s = s.slice(1); }
  s = s.replace(/[$,\s]/g, "");
  if (!/^\d*\.?\d+$/.test(s)) return null;
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return negative ? -n : n;
}

// Which expense column an invoice total belongs in.
//
// The two are NOT one signed column. `manual_material_cost` and
// `material_credit` are read separately by v_expenses and by every GP rollup,
// and a credit written as a negative cost double-counts once the views subtract
// the credit again. Splitting here keeps `createExpenseNative`'s existing
// credit-only contract intact — the same one handleAddGeneralExpense uses for
// returned supplies.
export function amountToExpenseFields(amount) {
  const n = parseSignedAmount(amount);
  if (n === null || n === 0) return { manualMaterialCost: null, materialCredit: null };
  return n < 0
    ? { manualMaterialCost: null, materialCredit: Math.abs(n) }
    : { manualMaterialCost: n,    materialCredit: null };
}

// Vendors this inbox accepts. Kept as a list rather than "anything the bot
// sends" so a parser that starts reading the wrong letterhead — or a scenario
// pointed at the wrong Gmail label — lands as a rejected intake instead of
// quietly opening an expense account for a vendor nobody agreed to.
//
// The value is the canonical `expenses.vendor_name` spelling, which is what the
// existing 215 bot-written expenses already carry. Keep the right-hand side in
// step with `expense_vendors.name`, not with how the PDF spells itself: the
// invoices say "CED CONSOLIDATED ELECTRICAL DISTRIBUTORS, INC." and "WOLFF
// BROS. SUPPLY, INC.".
const VENDOR_ALIASES = [
  [/^ced\b|consolidated\s+electrical/i, "CED"],
  [/^wol[f]{1,2}\b|wolff\s+bros/i,      "Wolff Brothers"],
];

export function canonicalVendor(v) {
  const s = String(v || "").trim();
  if (!s) return null;
  for (const [re, name] of VENDOR_ALIASES) if (re.test(s)) return name;
  return null;
}
