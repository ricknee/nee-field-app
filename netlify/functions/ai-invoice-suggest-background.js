// Background worker: AI job suggestions for parked supplier invoices (schema 102).
//
// Netlify runs any function named *-background asynchronously: the caller gets
// 202 at once and this has up to 15 minutes. That is why the AI read lives here
// and not in airtable.js, where a slow PDF read would hit the 10 s limit.
//
// ⚠ NO USER SESSION. The only accepted credential is a short-lived scope token
// signed with AUTH_SECRET for exactly AI_SCOPE (see triggerAiInvoiceSuggest).
// Anything else is ignored. What it can do is narrow: write the ai_* columns
// on vendor_invoices rows that are still needs_review. It never creates an
// expense and never changes an invoice's status.

import { verifyScope } from "./_auth.js";
import { neonEnabled, neonQuery } from "./_neon.js";
import { r2Enabled, getObjectBytes } from "./_r2.js";
import { aiEnabled, AI_SCOPE, AI_BATCH, AI_CANDIDATE_JOBS_SQL, suggestJob } from "./_ai-invoice-match.js";

export async function runAiInvoiceSuggestions({ invoiceId = null, q = neonQuery, readPdf = getObjectBytes, create } = {}) {
  if (!aiEnabled()) return { ok: true, read: 0, reason: "no-key" };
  if (!neonEnabled()) return { ok: false, read: 0, reason: "no-database" };
  if (!r2Enabled()) return { ok: false, read: 0, reason: "no-r2" };

  // A named invoice is re-read even if it was read before (the 🤖 button);
  // the sweep takes only rows never read. Both: still parked, with a PDF.
  const pending = await q(
    `SELECT id::text AS id, vendor_name, invoice_no, po_text, amount, match_reason, pdf_key
       FROM vendor_invoices
      WHERE status = 'needs_review' AND pdf_key IS NOT NULL
        AND ($1::text IS NULL AND ai_read_at IS NULL OR id::text = $1)
      ORDER BY received_at
      LIMIT ${AI_BATCH}`, [invoiceId]);
  const rows = pending?.rows || [];
  if (!rows.length) return { ok: true, read: 0 };

  const jq = await q(AI_CANDIDATE_JOBS_SQL, []);
  const jobs = jq?.rows || [];
  if (!jobs.length) return { ok: false, read: 0, reason: "no-jobs" };

  let read = 0, failed = 0;
  for (const inv of rows) {
    let result;
    try {
      const pdfBytes = await readPdf(inv.pdf_key);
      result = await suggestJob({ pdfBytes, inv, jobs, ...(create ? { create } : {}) });
    } catch (e) {
      result = { ok: false, error: String(e?.message || e).slice(0, 300) };
    }
    // Stamped either way, so a broken PDF isn't re-sent (and re-billed) every hour.
    // The 🤖 button re-reads on demand.
    if (result.ok) {
      read++;
      await q(`UPDATE vendor_invoices SET ai_job_id = $2::uuid, ai_confidence = $3, ai_reason = $4,
                                          ai_error = NULL, ai_read_at = now()
                WHERE id::text = $1 AND status = 'needs_review'`,
              [inv.id, result.jobId, result.confidence, result.reason]);
    } else {
      failed++;
      await q(`UPDATE vendor_invoices SET ai_error = $2, ai_read_at = now()
                WHERE id::text = $1 AND status = 'needs_review'`, [inv.id, result.error]);
    }
  }
  return { ok: true, read, failed };
}

export async function handler(event) {
  let body = {};
  try { body = JSON.parse(event.body || "{}"); } catch { /* ignored */ }
  if (!verifyScope(body.scope, AI_SCOPE)) {
    console.warn("ai-invoice-suggest: refused (no valid scope token)");
    return { statusCode: 401, body: "" };
  }
  try {
    const report = await runAiInvoiceSuggestions({ invoiceId: body.invoiceId ? String(body.invoiceId) : null });
    console.log("ai-invoice-suggest:", JSON.stringify(report));
  } catch (e) {
    console.error("ai-invoice-suggest failed:", e?.message || e);
  }
  return { statusCode: 202, body: "" };
}
