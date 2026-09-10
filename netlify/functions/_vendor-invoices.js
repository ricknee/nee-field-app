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
//
// ⚠⚠ ZERO IS NEITHER, AND IT MUST NOT BECOME A CREDIT BY INFERENCE. Contractor
// Lighting prints a running balance, so an ordinary charge can show a 0.00
// balance — and a zero-balance CREDIT looks identical on the paper. Deciding
// between them here would be guessing at the sign of real money from a number
// that carries no sign, and the guess would be invisible: a charge filed as a
// credit SUBTRACTS from the job's material cost, so the job's GP simply reads
// better than it is. Nobody chases a number that looks good. The Mini Bee reads
// the actual credit status off the document; `isCredit` is how it says so.
//
// `isCredit` is honoured only when explicitly `true`, and it can only ever turn
// an amount INTO a credit, never out of one. A vendor that prints credit memos
// with a minus (Wolff's trailing "773.85-") keeps working whether the bot sets
// the flag, omits it, or sends it false.
export function amountToExpenseFields(amount, isCredit) {
  const n = parseSignedAmount(amount);
  if (n === null) return { manualMaterialCost: null, materialCredit: null };
  if (isCredit === true) {
    const c = Math.abs(n);
    return c === 0
      ? { manualMaterialCost: null, materialCredit: null }
      : { manualMaterialCost: null, materialCredit: c };
  }
  if (n === 0) return { manualMaterialCost: null, materialCredit: null };
  return n < 0
    ? { manualMaterialCost: null, materialCredit: Math.abs(n) }
    : { manualMaterialCost: n,    materialCredit: null };
}

// The invoice PDF, decoded and size-checked. It lives here — pure, no network —
// so the CAP IS TESTABLE and is written down exactly once.
//
// The samples are 15–26 KB. This cap is two orders of magnitude above that and
// still well under Netlify's request ceiling, so hitting it means the bot sent
// something that is not one supplier invoice. Returns null rather than throwing,
// because the PDF path fails SOFT: an invoice with no readable PDF still shows
// its parsed figures and can still be assigned to a job. Losing the picture is
// bad; losing the money is worse.
// A date-only 'YYYY-MM-DD', or null. The BACKSTOP for expense dates, not the
// mechanism — every caller formats with `to_char(col, 'YYYY-MM-DD')` in SQL.
//
// ⚠⚠ THIS BUG HAS BEEN WRITTEN THREE TIMES IN THIS CODEBASE. `String(v).slice(0, 10)`
// is right for a date string off the wire and garbage for the JS Date the Neon
// driver returns for a DATE column: it yields "Wed Aug 12", which Postgres
// refuses with `invalid input syntax for type date`. Here that took down the
// whole assignment — a real $2,096.49 invoice could not be placed on its job.
// `toISOString().slice(0, 10)` is not the fix either: it shifts the day
// backwards for anyone west of UTC.
//
// So this accepts ONLY the shape SQL produces, and returns null for anything
// else rather than passing an unknown one through to be rejected downstream.
export function ymdOrNull(v) {
  if (v === null || v === undefined || v === "") return null;
  const m = /^(\d{4}-\d{2}-\d{2})$/.exec(String(v).trim());
  return m ? m[1] : null;
}

export const INVOICE_PDF_MAX_BYTES = 8 * 1024 * 1024;

export function decodeInvoicePdf(pdfBase64) {
  if (!pdfBase64) return null;
  let bytes;
  try { bytes = Buffer.from(String(pdfBase64), "base64"); } catch { return null; }
  if (!bytes.length || bytes.length > INVOICE_PDF_MAX_BYTES) return null;
  return bytes;
}

// Vendors this inbox accepts. Kept as a list rather than "anything the bot
// sends" so a parser that starts reading the wrong letterhead — or a scenario
// pointed at the wrong Gmail label — lands as a rejected intake instead of
// quietly opening an expense account for a vendor nobody agreed to.
//
// ⚠ THE RIGHT-HAND SIDE IS `expense_vendors.name`, VERBATIM, AND IT IS LOAD-
// BEARING — not a label. It is the canonical `expenses.vendor_name` the 215
// bot-written expenses already carry, and `vendorHandleFor` looks it up with
// `lower(name) = lower($1)`. A name matching no row resolves to NULL and the
// expense is created with NO VENDOR AT ALL. It does not throw — the cost still
// lands on the job, simply attributed to nobody, which is this system's
// characteristic silent failure. So keep it in step with `expense_vendors`, NOT
// with how the letterhead spells itself: the paper says "CED CONSOLIDATED
// ELECTRICAL DISTRIBUTORS, INC." and "WOLFF BROS. SUPPLY, INC.".
//
// Verified against Neon 2026-09-09: "Lowe's" (recNZLNmYciizye23) and
// "Contractor Lighting & Supply" (6c773531-…, native, no rec id) both exist and
// are spelled exactly as below. The curly apostrophe in "Lowe’s" is an INPUT
// spelling only — the stored name uses the straight one. "Home Depot"
// (recwkYML0GVfOonxp) verified the same way 2026-09-10.
//
// ⚠ THE ALLOWLIST IS NOT "ANY VENDOR WE KNOW". `expense_vendors` holds dozens of
// names — Menards among them — and being in that table does NOT make a supplier
// acceptable here. This list is the set whose invoices a bot is trusted to turn
// into money unattended; everything else is a 400 somebody has to look at.
//
// ⚠ ADDING A VENDOR IS TWO PLACES: an alias here AND an `expense_vendors` row.
// Doing only the first gives an endpoint that accepts the invoice and files it
// against nobody.
const VENDOR_ALIASES = [
  [/^ced\b|consolidated\s+electrical/i, "CED"],
  [/^wol[f]{1,2}\b|wolff\s+bros/i,      "Wolff Brothers"],
  // Lowe's: the receipts say "LOWE'S", the bot's folder says "Lowes", and a
  // copy-paste out of a PDF or a phone keyboard gives the curly "Lowe’s". All
  // three are the same supplier.
  // ⚠ THE `s` IS REQUIRED, and the apostrophe is what is optional — not the
  // other way round. "Lowe Electric Supply" is a real electrical distributor,
  // and an `s?` here would quietly file its invoices as Lowe's. Anchored at the
  // start for the same reason.
  [/^lowe['’]?s\b/i,                                        "Lowe's"],
  // Contractor Lighting: the ampersand is spelled "&" on the letterhead and
  // "and" by anyone typing it. Matching on the first two words covers both
  // without caring which, and without caring about a trailing ", INC.".
  [/^contractor\s+lighting\b|contractor\s+lighting\s*(?:&|and)\s*supply/i,
                                                            "Contractor Lighting & Supply"],
  // Home Depot: the receipts say "THE HOME DEPOT", the card statement says
  // "HOME DEPOT #4512", and the legal name is "HOME DEPOT U.S.A., INC." The
  // leading "The" is optional and so is the space, because the domain spells it
  // "homedepot".
  // ⚠ The store number after the name is why this stops at \b rather than
  // anchoring the whole string — "HOME DEPOT #4512" is one vendor, not a new one
  // per store.
  [/^(?:the\s+)?home\s*depot\b/i,                           "Home Depot"],
];

// The canonical names this inbox accepts, in alias order. Exported so the
// rejection message and the review screen's vendor chips are built from the
// same list a vendor is actually added to — a hard-coded "Expected CED or
// Wolff" goes stale the moment this array grows, and points the bot's operator
// at the wrong problem.
export const ACCEPTED_VENDORS = VENDOR_ALIASES.map(([, name]) => name);

export function canonicalVendor(v) {
  const s = String(v || "").trim();
  if (!s) return null;
  for (const [re, name] of VENDOR_ALIASES) if (re.test(s)) return name;
  return null;
}
