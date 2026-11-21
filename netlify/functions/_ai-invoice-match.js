// 🤖 AI job suggestion for supplier invoices that matched no job (schema 102).
//
// Owner 2026-11-20. When an invoice parks on 🧾 Invoice Review, Claude reads the
// PDF (ship-to, job name, address, customer, PO box) and suggests which job it
// belongs to. ⛔ It ONLY SUGGESTS. Nothing here creates an expense or changes
// an invoice's status. A person clicks "Use this", which goes through
// vendorInvoiceAssign with its double-charge guard like any manual pick.
//
// Runs in the background function ai-invoice-suggest-background (15-minute
// limit). A normal function has 10 s and a PDF read can take longer. It's
// triggered at intake, by the hourly pull as a sweep, and by the 🤖 button.
//
// Inert without ANTHROPIC_API_KEY: every entry point returns early, nothing
// throws. Sends the invoice PDF and a list of open jobs (name, PO, customer,
// address) to Anthropic's API.
//
// ⚠ The model's answer is UNTRUSTED. The job id it returns must be one of the
// ids we sent, or the suggestion is dropped. It is never used to write money.

import { signScope } from "./_auth.js";

export const AI_MODEL = "claude-opus-5-5";
export const AI_SCOPE = ["ai-invoice-suggest"];
export const AI_CONFIDENCE = ["high", "medium", "low", "none"];
// Invoices read per background run. A backlog drains over a few runs, never in a burst.
export const AI_BATCH = 10;

export function aiEnabled() {
  return !!process.env.ANTHROPIC_API_KEY;
}

// Jobs an invoice could plausibly belong to: anything open, plus jobs completed
// in the last 120 days (a late invoice for finished work is common).
export const AI_CANDIDATE_JOBS_SQL = `
  SELECT j.id::text AS id, j.name, COALESCE(j.po_locked, j.po) AS po, j.status,
         NULLIF(trim(concat_ws(' ', j.customer_first_name, j.customer_last_name)), '') AS customer,
         j.contractor_name, j.address_full, j.address_city
    FROM jobs j
   WHERE j.status IN ('Awarded', 'Ready to Invoice', 'Service Call Scheduled', 'Estimating', 'New Lead')
      OR (j.status = 'Completed' AND COALESCE(j.project_completed_at, j.synced_at) > now() - interval '120 days')
   ORDER BY j.name`;

export function jobListText(jobs) {
  return jobs.map(j => [
    j.id, j.name, j.po ? `PO ${j.po}` : "", j.status, j.customer || "", j.contractor_name || "",
    j.address_full || j.address_city || "",
  ].map(s => String(s).replace(/[|\n]/g, " ").trim()).join(" | ")).join("\n");
}

export function buildPrompt(inv, jobs) {
  return `You work in the office of an electrical contractor. The attached PDF is a supplier invoice (materials bought for a job).
The PO the supplier printed did not match any job automatically, so a person has to place it. Your job is to suggest which job it belongs to.

What the automatic reader found:
- Vendor: ${inv.vendor_name || "?"}
- Invoice #: ${inv.invoice_no || "?"}
- PO as printed: ${inv.po_text || "(blank)"}
- Amount: ${inv.amount ?? "?"}
${inv.match_reason === "ambiguous-po" ? "- That PO matched MORE THAN ONE job; pick between them only if the PDF shows which.\n" : ""}
Open jobs (id | name | PO | status | customer | contractor | address):
${jobListText(jobs)}

Look at everything on the invoice: the ship-to name and address, the job or "deliver to" line, the PO box, customer names, notes.
Rules:
- Only pick a job when something on the invoice actually points to it. Say what that is in "reason" (quote the invoice).
- Two jobs that look alike (same customer, "2 Barn" on two farms, similar names): don't guess. Set confidence "low", pick neither (job_id ""), and name both in the reason.
- Shop stock, tools, overhead, or nothing that points to a job: job_id "", confidence "none".
- confidence "high" only when the address or a unique job name matches.
- job_id must be copied exactly from the list above, or "".
Keep "reason" to one or two short sentences.`;
}

export const AI_OUTPUT_SCHEMA = {
  type: "object",
  properties: {
    job_id: { type: "string" },
    confidence: { type: "string", enum: AI_CONFIDENCE },
    reason: { type: "string" },
  },
  required: ["job_id", "confidence", "reason"],
  additionalProperties: false,
};

// Validates the model's JSON against the ids we sent. Never throws.
export function parseSuggestion(text, jobIds) {
  let o;
  try { o = JSON.parse(String(text || "")); } catch { return { ok: false, error: "unreadable answer" }; }
  const ids = jobIds instanceof Set ? jobIds : new Set(jobIds);
  let jobId = typeof o?.job_id === "string" ? o.job_id.trim() : "";
  let confidence = AI_CONFIDENCE.includes(o?.confidence) ? o.confidence : "none";
  if (jobId && !ids.has(jobId)) { jobId = ""; confidence = "none"; }   // invented id: drop it
  if (!jobId && confidence !== "low") confidence = "none";
  const reason = String(o?.reason || "").replace(/\s+/g, " ").trim().slice(0, 500);
  return { ok: true, jobId: jobId || null, confidence, reason };
}

let _client = null;
async function defaultCreate(params) {
  if (!_client) {
    // Lazy, like the Neon driver: the offline test suite never loads the SDK.
    const { default: Anthropic } = await import("@anthropic-ai/sdk");
    _client = new Anthropic({ maxRetries: 2 });
  }
  return _client.beta.messages.create(params);
}

// One invoice → one suggestion. `create` is injectable for tests.
export async function suggestJob({ pdfBytes, inv, jobs, create = defaultCreate }) {
  const response = await create({
    model: AI_MODEL,
    max_tokens: 8000,
    // A policy decline reruns on a fallback model inside the same call.
    betas: ["server-side-fallback-2026-07-01"],
    fallbacks: "default",
    output_config: { effort: "medium", format: { type: "json_schema", schema: AI_OUTPUT_SCHEMA } },
    messages: [{
      role: "user",
      content: [
        { type: "document", source: { type: "base64", media_type: "application/pdf",
                                      data: Buffer.from(pdfBytes).toString("base64") } },
        { type: "text", text: buildPrompt(inv, jobs) },
      ],
    }],
  });
  if (response?.stop_reason === "refusal") return { ok: false, error: "the model declined to read this invoice" };
  if (response?.stop_reason === "max_tokens") return { ok: false, error: "the answer was cut off" };
  const text = (response?.content || []).filter(b => b.type === "text").map(b => b.text).join("");
  return parseSuggestion(text, new Set(jobs.map(j => j.id)));
}

// Wakes the background worker. Fire-and-forget: a failure only delays the
// suggestion until the hourly sweep. Never throws.
export async function triggerAiInvoiceSuggest(invoiceId = null) {
  try {
    if (!aiEnabled()) return { triggered: false, reason: "no-key" };
    const base = process.env.URL || process.env.DEPLOY_PRIME_URL;
    if (!base) return { triggered: false, reason: "no-site-url" };
    const ctl = new AbortController();
    const t = setTimeout(() => ctl.abort(), 4000);
    try {
      const r = await fetch(`${base}/.netlify/functions/ai-invoice-suggest-background`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ scope: signScope(AI_SCOPE, 10 * 60 * 1000), invoiceId }),
        signal: ctl.signal,
      });
      return { triggered: r.status === 202 || r.ok, status: r.status };
    } finally { clearTimeout(t); }
  } catch (e) {
    return { triggered: false, reason: String(e?.message || e).slice(0, 120) };
  }
}
