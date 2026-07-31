// Infers the AGGREGATION and FILTER of each Airtable rollup on Jobs from the data,
// because the Meta API exposes neither (see docs/GP-FORMULA-INVENTORY.md).
//
// Method, per rollup field:
//   1. Resolve link field -> linked table -> target field from the Meta schema.
//   2. For every Job, gather its linked records and their target values.
//   3. Try each candidate aggregation (sum / max / min / count / avg) UNFILTERED
//      against the rollup's stored value on every job.
//   4. If none matches, the field is filtered. Try every subset of each candidate
//      single-select / checkbox field on the linked table and report the subset
//      whose aggregation matches every job.
//
// A candidate must match on EVERY job that has linked records — one counter-example
// rejects it. That is what makes this evidence rather than a guess. Anything with no
// unique surviving answer is reported AMBIGUOUS, for a human to read off the UI.
//
// READ-ONLY. Usage:  node db/etl/gp-infer-rollups.mjs
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = process.env.REPO_ROOT || path.resolve(HERE, "..", "..");
const env = Object.fromEntries(
  fs.readFileSync(path.join(ROOT, ".env"), "utf8")
    .split(/\r?\n/).filter(l => l && !l.startsWith("#") && l.includes("="))
    .map(l => [l.slice(0, l.indexOf("=")).trim(), l.slice(l.indexOf("=") + 1).trim()])
);
const BASE = "appiqWg6SvKcGfMAu";
const AUTH = { Authorization: `Bearer ${env.AIRTABLE_PROD_READ_PAT}` };

const sleep = ms => new Promise(r => setTimeout(r, ms));
async function fetchAll(table) {
  const out = []; let offset;
  do {
    const p = new URLSearchParams({ pageSize: "100" });
    if (offset) p.set("offset", offset);
    const t0 = Date.now();
    const r = await fetch(`https://api.airtable.com/v0/${BASE}/${encodeURIComponent(table)}?${p}`, { headers: AUTH });
    if (!r.ok) throw new Error(`${table} ${r.status}: ${(await r.text()).slice(0, 200)}`);
    const d = await r.json();
    out.push(...d.records); offset = d.offset;
    const spent = Date.now() - t0;
    if (offset && spent < 220) await sleep(220 - spent);
  } while (offset);
  return out;
}

const meta = await (await fetch(`https://api.airtable.com/v0/meta/bases/${BASE}/tables`, { headers: AUTH })).json();
const tableById = new Map(meta.tables.map(t => [t.id, t]));
const fieldById = new Map();
for (const t of meta.tables) for (const f of t.fields) fieldById.set(f.id, { ...f, tableId: t.id });

const jobsT = meta.tables.find(t => t.name === "Jobs");
// Take EVERY formula and rollup on Jobs. An earlier keyword regex silently missed
// 5 of 28 rollups — including Actual Scissor Lift Expense and Actual Rental
// Equipment Expense, both referenced directly by Actual Job Cost (COGS) and
// Total Revenue (Live). A filter that quietly drops GP inputs is worse than noise.
const MONEY = /./;
const specs = jobsT.fields.filter(f => f.type === "rollup" && MONEY.test(f.name)).map(f => {
  const o = f.options || {};
  const linkF = fieldById.get(o.recordLinkFieldId);
  const tgtF  = fieldById.get(o.fieldIdInLinkedTable);
  const lt    = linkF ? tableById.get(linkF.options?.linkedTableId) : null;
  return { name: f.name, linkField: linkF?.name, linkedTable: lt?.name, target: tgtF?.name };
}).filter(s => s.linkField && s.linkedTable && s.target);

// Load Jobs + every linked table once.
console.log("loading Airtable ...");
const jobs = await fetchAll("Jobs");
const linkedTables = [...new Set(specs.map(s => s.linkedTable))];
const rows = {};
for (const tn of linkedTables) { rows[tn] = await fetchAll(tn); console.log(`  ${tn}: ${rows[tn].length}`); }
const byId = {};
for (const tn of linkedTables) byId[tn] = new Map(rows[tn].map(r => [r.id, r.fields]));

const N = v => (v === undefined || v === null || v === "" ? null : Number(v));
const near = (a, b) => Math.abs(a - b) < 0.005;

const AGGS = {
  sum:   vs => vs.reduce((s, v) => s + v, 0),
  max:   vs => (vs.length ? Math.max(...vs) : 0),
  min:   vs => (vs.length ? Math.min(...vs) : 0),
  count: vs => vs.length,
  avg:   vs => (vs.length ? vs.reduce((s, v) => s + v, 0) / vs.length : 0),
};

// Candidate filter fields: things a human would plausibly filter on.
function filterCandidates(tableName) {
  const t = meta.tables.find(x => x.name === tableName);
  return t.fields.filter(f =>
    (f.type === "singleSelect" || f.type === "checkbox") &&
    !/^Reviewed Wire Cost$|^Pipe Cost/.test(f.name)
  ).map(f => ({ name: f.name, type: f.type }));
}

const subsetsOf = arr => {
  const out = [];
  for (let m = 1; m < (1 << arr.length); m++) {
    out.push(arr.filter((_, i) => m & (1 << i)));
  }
  return out;
};

// Where data inference finds a CORRELATION but semantics give the real rule, the
// human judgement is recorded here with its evidence. Pure pattern-matching cannot
// make these calls — that is the limit of the method, not a bug in it.
const OVERRIDES = {
  "Total Contract Billed": {
    agg: "sum", filter: "Invoice Type in [Contract]",
    why: "inference offered 'Auto Allocate? = false', which matches only because every " +
         "Time & Material invoice happens to have Auto Allocate set. Reading the excluded " +
         "invoices directly, ALL of them are Invoice Type = Time & Material, and the field " +
         "is called *Contract* Billed. Verified: sum WHERE Invoice Type = Contract matches all 42 jobs.",
  },
  "Actual Subcontract Expense": {
    agg: "sum", filter: "Expense Type in [Subcontract]",
    why: "only 2 Subcontract expense rows exist ($10,255 total), so several filters fit the " +
         "data equally. The field name and the Expense Type value are the same word; the " +
         "sibling rollup Actual Material Cost is the identical shape on Expense Type = Materials.",
  },
  "Base Contract Amount": {
    agg: "sum", filter: "Status in [Approved] AND Estimate Type in [Original]",
    why: "the automatic pair search reports 7 equivalent explanations, but restricting the " +
         "search to the two semantically plausible fields (Status x Estimate Type) leaves " +
         "EXACTLY ONE match across all 49 jobs. Confirmed by hand on Gary Strauss (CAG 139): " +
         "two Approved estimates, Original 47,250 and Extra's 2,500, rollup = 47,250 — the " +
         "Original only. Every other job has no Approved+Original estimate and reads 0.",
  },
  // Pipe Usage and Wire Weigh-Ins are LEGACY (owner, 2026-07-31): pipe and wire are
  // tracked in the inventory app now and pushed across as expenses. These rollups are
  // frozen history — they still feed Actual Job Cost (COGS) for old jobs but will not
  // move again. Port them faithfully for history; expect 0 on anything new.
};

const results = [];
for (const s of specs) {
  const ov = OVERRIDES[s.name];
  if (ov) {
    results.push({ ...s, verdict: "CONFIRMED", agg: ov.agg, filter: ov.filter, evidence: ov.why });
    continue;
  }
  // Per job: the linked target values, plus each record's raw fields for filtering.
  const cases = [];
  for (const j of jobs) {
    const ids = j.fields[s.linkField];
    if (!Array.isArray(ids) || !ids.length) continue;          // no links -> uninformative
    const actual = N(j.fields[s.name]);
    if (actual === null) continue;                             // blank rollup -> uninformative
    const recs = ids.map(id => byId[s.linkedTable].get(id)).filter(Boolean);
    if (recs.length !== ids.length) continue;                  // couldn't resolve every link
    cases.push({ actual, recs });
  }
  if (!cases.length) { results.push({ ...s, verdict: "NO DATA", detail: "no job has linked records with a value" }); continue; }

  const vals = recs => recs.map(f => N(f[s.target])).filter(v => v !== null);

  // 1. unfiltered
  const unfiltered = Object.keys(AGGS).filter(a => cases.every(c => near(AGGS[a](vals(c.recs)), c.actual)));
  if (unfiltered.length) {
    // Every aggregation matching means the data cannot tell them apart — typically
    // all the underlying values are null or zero. Say so rather than pick one.
    const undetermined = unfiltered.length >= 4;
    results.push({ ...s,
      verdict: undetermined ? "UNDETERMINED" : "CONFIDENT",
      agg: undetermined ? "(any — cannot distinguish)" : unfiltered.join(" or "),
      filter: "none (all linked records)",
      evidence: undetermined
        ? `${cases.length} jobs, but every underlying value is null/zero so no aggregation is ruled out`
        : `${cases.length} jobs` });
    continue;
  }

  // 2. filtered — search subsets of each candidate field's values
  const found = [];
  for (const cf of filterCandidates(s.linkedTable)) {
    const distinct = [...new Set(cases.flatMap(c => c.recs.map(f =>
      cf.type === "checkbox" ? (f[cf.name] === true ? "true" : "false") : (f[cf.name] ?? "(empty)"))))];
    if (distinct.length > 8) continue;                          // too many to brute force
    for (const sub of subsetsOf(distinct)) {
      const keep = recs => recs.filter(f => {
        const v = cf.type === "checkbox" ? (f[cf.name] === true ? "true" : "false") : (f[cf.name] ?? "(empty)");
        return sub.includes(v);
      });
      for (const a of Object.keys(AGGS)) {
        if (cases.every(c => near(AGGS[a](vals(keep(c.recs))), c.actual))) {
          found.push({ agg: a, field: cf.name, values: sub });
        }
      }
    }
  }

  // 3. still nothing? try PAIRS of filter fields. Base Contract Amount needed this —
  // it is "Status = Approved AND Estimate Type = Original", which no single-field
  // search can ever express.
  if (!found.length) {
    const cands = filterCandidates(s.linkedTable);
    const valOf = (f, cf) => cf.type === "checkbox" ? (f[cf.name] === true ? "true" : "false") : (f[cf.name] ?? "(empty)");
    for (let i = 0; i < cands.length && !found.length; i++) {
      for (let k = i + 1; k < cands.length && !found.length; k++) {
        const A = cands[i], B = cands[k];
        const dA = [...new Set(cases.flatMap(c => c.recs.map(f => valOf(f, A))))];
        const dB = [...new Set(cases.flatMap(c => c.recs.map(f => valOf(f, B))))];
        if (dA.length > 6 || dB.length > 6) continue;
        for (const sa of subsetsOf(dA)) for (const sb of subsetsOf(dB)) {
          const keep = recs => recs.filter(f => sa.includes(valOf(f, A)) && sb.includes(valOf(f, B)));
          for (const a of Object.keys(AGGS)) {
            if (cases.every(c => near(AGGS[a](vals(keep(c.recs))), c.actual))) {
              found.push({ agg: a, field: `${A.name} AND ${B.name}`,
                           values: [`${A.name} in [${sa.join(", ")}]`, `${B.name} in [${sb.join(", ")}]`] });
            }
          }
        }
      }
    }
  }

  if (!found.length) {
    results.push({ ...s, verdict: "AMBIGUOUS", detail: `no aggregation/filter combination matched all ${cases.length} jobs — read this one off the UI` });
  } else {
    // Prefer the explanation using the fewest included values (the tightest filter).
    found.sort((a, b) => a.values.length - b.values.length);
    const best = found[0];
    const distinctAggs = [...new Set(found.map(f => f.agg))];
    results.push({ ...s,
      verdict: found.length === 1 || distinctAggs.length === 1 ? "CONFIDENT" : "LIKELY",
      agg: best.agg,
      filter: `${best.field} in [${best.values.join(", ")}]`,
      evidence: `${cases.length} jobs` + (found.length > 1 ? `, ${found.length} equivalent explanations` : ""),
    });
  }
}

const pad = (s, n) => String(s ?? "").padEnd(n);
console.log("\n\n=== INFERRED ROLLUP DEFINITIONS ===\n");
for (const v of ["CONFIRMED", "CONFIDENT", "LIKELY", "UNDETERMINED", "AMBIGUOUS", "NO DATA"]) {
  const grp = results.filter(r => r.verdict === v);
  if (!grp.length) continue;
  console.log(`--- ${v} (${grp.length}) ---`);
  for (const r of grp) {
    console.log(`  ${pad(r.name, 52)} ${pad(r.agg || "-", 7)} ${r.filter || r.detail || ""}`);
    if (r.evidence) console.log(`  ${" ".repeat(52)} (${r.evidence})`);
  }
  console.log("");
}
fs.writeFileSync(path.join(ROOT, "docs/GP-ROLLUP-INFERENCE.json"), JSON.stringify(results, null, 2));
console.log("full results -> docs/GP-ROLLUP-INFERENCE.json");
