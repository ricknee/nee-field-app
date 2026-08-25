// Tier-1.5: LIVE verification of the estimate cost/sell split (db/schema/065).
// ---------------------------------------------------------------------------
// ⚠⚠ THIS ONE TALKS TO A REAL DATABASE, AND IT WRITES. It is not part of
// `node tests/handlers.test.mjs` and must never be wired into it.
//
// WHY IT EXISTS. The offline suite mocks Airtable but has no Neon, so it can
// only pin the SHAPE of these statements, never their arithmetic. That is not
// paranoia — on 2026-08-22 the estimate create shipped broken and the first
// click in production failed with "inconsistent types deduced for parameter $5".
// The check that passed beforehand used `PREPARE name(text, numeric, …)`, which
// DECLARES the parameter types and so resolves the very ambiguity that broke.
// The driver sends parameters UNTYPED. Only a real call through the real driver
// proves a statement, which is what this file is.
//
// HOW TO RUN. Make a Neon branch — never point this at the production branch,
// it creates and deletes estimates on real jobs — and pass its connection
// string:
//
//   & "C:\Users\irick\nodejs\node.exe" tests/estimate-gp.live.mjs "postgresql://…"
//
// Everything it creates it deletes on the way out. The three job handles below
// are chosen for their billable rates (75 / 70 / none) because "the sell rate
// comes from the job" is the assertion a constant would silently break.
process.env.AIRTABLE_API_KEY = "test-key";
process.env.AIRTABLE_BASE_ID = "testbase";
process.env.GOOGLE_MAPS_API_KEY = "test-maps";
process.env.ADMIN_BACKFILL_TOKEN = "test-token";
process.env.AUTH_SECRET = "test-secret";
process.env.AIRTABLE_WRITES = "off";           // no mirror, no Airtable network
process.env.DATABASE_URL = process.argv[2];

if (!process.env.DATABASE_URL) {
  console.error("Usage: node tests/estimate-gp.live.mjs <neon-branch-connection-string>");
  process.exit(2);
}

// The Neon driver's transport is global fetch, so it has to pass; anything else
// reaching the network is a bug in this run and should say so loudly rather
// than quietly succeed against a real Airtable base.
const realFetch = globalThis.fetch;
globalThis.fetch = async (url, opts) => {
  if (String(url).includes("neon.tech")) return realFetch(url, opts);
  throw new Error(`UNEXPECTED OUTBOUND CALL: ${String(url).slice(0, 120)}`);
};

const { handler }   = await import("../netlify/functions/airtable.js");
const { signToken } = await import("../netlify/functions/_auth.js");
const hdr = { authorization: `Bearer ${signToken({ id: "recAdmin", role: "admin" })}` };

const GET  = (action, p = {}) => handler({ httpMethod: "GET",  queryStringParameters: { action, ...p }, headers: hdr });
const POST = (action, b = {}) => handler({ httpMethod: "POST", body: JSON.stringify({ action, ...b }), headers: hdr });
const json = (r) => { try { return JSON.parse(r.body); } catch { return { _raw: r.body }; } };

let pass = 0, fail = 0;
const eq = (a, b, what) => {
  const ok = Number(a) === Number(b) || String(a) === String(b);
  console.log(`${ok ? "  ok  " : "  FAIL"} ${what}${ok ? "" : `   got ${JSON.stringify(a)}, want ${JSON.stringify(b)}`}`);
  ok ? pass++ : fail++;
};

const JOB_75   = "rec2s6PxJ761sS9R4";  // Trail Cabinet — billable 75.00
const JOB_70   = "rec2xrVDgoLTi2jhW";  // Davidson's Addition — billable 70.00
const JOB_NULL = "rec3dKTdZEXyq5ITg";  // KDC Management — no billable rate

const created = [];
const mk = async (jobId, body) => {
  const r = json(await POST("createJobEstimate", { jobId, estimateDate: "2026-08-25", ...body }));
  if (!r.ok) throw new Error(`create failed: ${r.error}`);
  created.push(r.id);
  return r.id;
};
const readOne = async (jobId, id) => {
  const r = json(await GET("jobEstimates", { jobId }));
  if (!r.ok) throw new Error(`read failed: ${r.error}`);
  return { est: r.estimates.find(e => e.id === id), defaults: r.jobDefaults };
};

console.log("\n── 1. THE SENECA CASE: the number that started this ───────────────");
// Reported 21.9% for five years. The markup was being counted as cost.
const id1 = await mk(JOB_75, {
  baseAmount: 126000, laborHours: 650,
  materialRawCost: 70250, materialMarkup: 7000,
});
{
  const { est, defaults } = await readOne(JOB_75, id1);
  eq(est.materialRawCost, 70250, "raw material cost stored");
  eq(est.materialMarkup, 7000, "markup stored");
  eq(est.materialCost, 77250, "estimated_material_cost holds material SELL");
  eq(est.laborSellRate, 75, "sell rate resolved from the JOB, not a constant");
  eq(est.laborBurdenRate, 32.5, "burden rate stamped");
  eq(est.calculatedTotal, 91375, "direct cost = 70,250 + 650 x 32.50");
  eq(est.calculatedSellingPrice, 126000, "calculated price = 77,250 + 650 x 75");
  eq(est.legacyMaterialBasis, false, "not a legacy row");
  eq(defaults.markupPct, 0.1, "job markup% offered to the form");
  eq(defaults.sellRate, 75, "job sell rate offered to the form");
  const gpD = est.actualEstimate - est.calculatedTotal;
  eq(gpD, 34625, "GP $ = 34,625  (was 27,625 before this change)");
  eq((gpD / est.actualEstimate * 100).toFixed(1), "27.5", "GP % = 27.5  (was 21.9)");
}

console.log("\n── 2. THE SELL RATE COMES FROM THE JOB ────────────────────────────");
const id2 = await mk(JOB_70, { baseAmount: 10000, laborHours: 100, materialRawCost: 1000, materialMarkup: 100 });
eq((await readOne(JOB_70, id2)).est.laborSellRate, 70, "a 70/hr job stamps 70, not 75");
const id3 = await mk(JOB_NULL, { baseAmount: 10000, laborHours: 100, materialRawCost: 1000, materialMarkup: 100 });
eq((await readOne(JOB_NULL, id3)).est.laborSellRate, 75, "a job with no rate falls back to 75");
const id4 = await mk(JOB_70, { baseAmount: 10000, laborHours: 100, materialRawCost: 1000, laborSellRate: 92.5 });
eq((await readOne(JOB_70, id4)).est.laborSellRate, 92.5, "an explicit override wins over the job");

console.log("\n── 3. PARTIAL UPDATE RECOMPUTES FROM STORED VALUES ────────────────");
{
  // Change ONLY the markup. Price must move and direct cost must NOT, using
  // hours and raw cost that were never re-sent. This is the drift the
  // derivation lives in SQL to prevent.
  const r = json(await POST("updateEstimate", { estimateId: id1, materialMarkup: 9000 }));
  eq(r.ok, true, "markup-only update accepted");
  const { est } = await readOne(JOB_75, id1);
  eq(est.materialMarkup, 9000, "markup updated");
  eq(est.materialRawCost, 70250, "raw cost untouched");
  eq(est.materialCost, 79250, "material sell recomputed");
  eq(est.calculatedTotal, 91375, "direct COST did not move — markup is not a cost");
  eq(est.calculatedSellingPrice, 128000, "calculated price recomputed from stored hours");
}
{
  await POST("updateEstimate", { estimateId: id1, laborHours: 700 });
  const { est } = await readOne(JOB_75, id1);
  eq(est.calculatedTotal, 93000, "direct cost = 70,250 + 700 x 32.50");
  eq(est.calculatedSellingPrice, 131750, "price = 79,250 + 700 x 75");
}

console.log("\n── 4. A LEGACY ROW KEEPS TODAY'S ARITHMETIC ───────────────────────");
{
  // A client that has not picked up the new form sends `materialCost` alone. It
  // must produce a legacy-shaped row, not a half-populated one — the material
  // figure is unknowable as cost-or-sell and nothing may pretend otherwise.
  const idL = await mk(JOB_75, { baseAmount: 50000, laborHours: 200, materialCost: 20000 });
  const { est } = await readOne(JOB_75, idL);
  eq(est.materialRawCost, null, "no raw cost — the row is legacy");
  eq(est.legacyMaterialBasis, true, "and it says so");
  eq(est.materialCost, 20000, "the figure as typed survives untouched");
  eq(est.calculatedTotal, 26500, "direct cost = 20,000 + 200 x 32.50, exactly as before");
}

console.log("\n── 5. THE DOUBLE-COUNT IS REFUSED ─────────────────────────────────");
{
  const c = await POST("createJobEstimate", { jobId: JOB_75, materialMarkup: 500 });
  eq(c.statusCode, 400, "create: markup without a raw cost is refused");
  const u = await POST("updateEstimate", { estimateId: id2, materialMarkup: 500, materialRawCost: "" });
  eq(u.statusCode, 200, "update: markup alongside an already-stored raw cost is fine");
  const u2 = await POST("updateEstimate", { estimateId: created[created.length - 1], materialMarkup: 500 });
  eq(u2.statusCode, 400, "update: markup on a legacy row with no raw cost is refused");
}

console.log("\n── 6. THE JOB ROLLUPS ─────────────────────────────────────────────");
{
  // The FILTERED rollups exclude Draft, so promote one and check the job card.
  await POST("updateEstimateStatus", { estimateId: id1, status: "Sent" });
  const job = json(await GET("jobById", { jobId: JOB_75 })).job;
  eq(job.projectedEstimatedMaterialCost, 70250, "job card: RAW material cost");
  eq(job.projectedEstimatedMaterialMarkup, 9000, "job card: markup as its own line");
  eq(job.projectedEstimatedMaterialSell, 79250, "job card: material sell");
  eq(job.projectedEstimatedLaborSell, 52500, "job card: labor sell = 700 x 75");
  eq(job.projectedEstimatedLaborCost, 22750, "job card: labor cost = 700 x 32.50");
  eq(job.projectedEstimatedTotalCost, 93000, "job card: direct cost");
  eq(job.estimateLegacyCount, 0, "no legacy estimates counted on this job");
  eq(job.markupPct, 0.1, "job markup% reaches the client");
}

console.log("\n── 7. TEMPLATE DEFAULTS ROUND-TRIP ────────────────────────────────");
{
  // Two columns were added to a 14-parameter statement. A miscount throws
  // rather than corrupting, but nothing offline can see it at all.
  const name = "ZZ live-test template (delete me)";
  const c = json(await POST("estimateTemplateSave", {
    name, defaultLaborHours: 40, defaultMaterialCost: 5000,
    defaultMaterialMarkup: 500, defaultLaborSellRate: 82.5,
  }));
  eq(c.ok, true, "template create accepted");
  const all = json(await GET("estimateTemplatesAll"));
  const t = (all.templates || []).find(x => x.name === name);
  eq(t?.defaultMaterialMarkup, 500, "markup default round-trips");
  eq(t?.defaultLaborSellRate, 82.5, "sell rate default round-trips");
  const u = json(await POST("estimateTemplateSave", {
    templateId: t.id, name, defaultMaterialMarkup: 750, defaultLaborSellRate: "",
  }));
  eq(u.ok, true, "template update accepted");
  const all2 = json(await GET("estimateTemplatesAll"));
  const t2 = (all2.templates || []).find(x => x.name === name);
  eq(t2?.defaultMaterialMarkup, 750, "markup default updated");
  eq(t2?.defaultLaborSellRate, null, "an emptied sell rate is NULL, not 0 — NULL means 'use the job's rate'");
  await POST("estimateTemplateDelete", { templateId: t.id });
}

console.log("\n── 8. AIRTABLE CANNOT HALF-REVERT A SPLIT ESTIMATE ────────────────");
{
  // `syncEstimateToNeon` runs after a status change whose mirror succeeded, and
  // it used to overwrite the three money columns from Airtable. Airtable has no
  // raw-cost column, so that left the worst possible state: raw cost and markup
  // intact while the figures derived from them were reset to pre-split values.
  //
  // It cannot fire in production today — AIRTABLE_WRITES=off makes the mirror
  // return a null id — so the switch is flipped ON here, with a stubbed
  // Airtable, to exercise the path someone will one day re-enable.
  process.env.AIRTABLE_WRITES = "on";
  const outer = globalThis.fetch;
  globalThis.fetch = async (url, opts) => {
    if (String(url).includes("neon.tech")) return outer(url, opts);
    // Airtable answering with its own, pre-split view of the money.
    return { ok: true, status: 200, text: async () => JSON.stringify({
      id: "recSTUBBED", fields: {
        "Estimate Type": "Original", "Status": "Approved",
        "Actual Estimate Sent": 126000, "Estimated Labor Hours": 700,
        "Estimated Labor Cost": 22750,
        "Estimated Material Cost": 79250,      // the SELL figure, as Airtable holds it
        "Calculated Estimated Total": 102000,  // the OLD formula: labor cost + material SELL
      } }) };
  };
  try {
    const r = json(await POST("updateEstimateStatus", { estimateId: id1, status: "Approved" }));
    eq(r.ok, true, "status change with a live mirror succeeds");
    const { est } = await readOne(JOB_75, id1);
    eq(est.status, "Approved", "the status itself DID come back from Airtable");
    eq(est.materialRawCost, 70250, "raw cost survives");
    eq(est.materialMarkup, 9000, "markup survives");
    eq(est.materialCost, 79250, "material sell survives");
    eq(est.calculatedTotal, 93000, "direct cost NOT clobbered with Airtable's 102,000");
    eq(est.calculatedSellingPrice, 131750, "calculated price survives");
  } finally {
    globalThis.fetch = outer;
    process.env.AIRTABLE_WRITES = "off";
  }
}

console.log("\n── 9. CLEAN UP ────────────────────────────────────────────────────");
for (const id of created) await POST("deleteJobEstimate", { estimateId: id });
console.log(`  removed ${created.length} scratch estimates`);

console.log(`\n${fail ? "FAILED" : "PASSED"}  ${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);
