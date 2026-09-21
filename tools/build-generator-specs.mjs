// Regenerates the GEN_MODELS / GEN_MARELLI data block inside index.html from
// the researched spec-sheet JSON in docs/generator-specs/.
//
//   & "C:\Users\irick\nodejs\node.exe" tools/build-generator-specs.mjs
//
// Why a generator and not hand-typed rows: the JSON carries a source URL and a
// note per row (every number was read off a manufacturer spec sheet), which is
// the thing you need when a value is questioned — and it is too heavy to ship
// in the SPA. The page gets a compact array; the provenance stays in docs/,
// which netlify.toml refuses to serve.
//
// Adding a brand = drop <brand>.json in docs/generator-specs/ and re-run. A
// missing file is skipped, not an error, so the tab ships with what exists.
//
// ⚠ Rows are copied, never adjusted. A row with no standby rating (e.g. a
// continuous-only lean-burn set) is left out rather than given one, and a
// null prime stays null — "standby only" on screen is a fact about the spec
// sheet, and filling it in with 90% would present a rule of thumb as a rating.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DIR = path.join(ROOT, "docs", "generator-specs");
const HTML = path.join(ROOT, "index.html");
const BEGIN = "  // @generated GEN_DATA begin — tools/build-generator-specs.mjs, do not hand-edit";
const END = "  // @generated GEN_DATA end";

const BRANDS = ["generac", "cummins", "cat", "kohler"];
const FUELS = new Set(["Diesel", "NG", "LP"]);

// Each research pass spelled phase its own way: ["1","3"] (Generac, Cummins),
// "1-phase and 3-phase" (Cat). Normalised to "1", "3", "13", or "" = the sheet
// does not say. ⚠ "(1-phase not confirmed)" must NOT count as 1-phase — the
// words "1-phase" are in it, and a naive match would offer a 3φ-only set on
// a single-phase job.
function phases(p, who) {
  if (p == null) return "";
  if (Array.isArray(p)) return [...new Set(p.map(String))].sort().join("");
  if (typeof p !== "string") throw new Error(`${who}: unreadable phase ${JSON.stringify(p)}`);
  const s = p.replace(/\(1-phase not confirmed\)/i, "");
  // Kohler: bare "1", "3", "1 or 3". Cat: "1-phase and 3-phase".
  const bare = /^\s*[13](\s*(or|and|,|\/)\s*[13])?\s*$/.test(s);
  const one = (bare && /1/.test(s)) || /\b1[- ]?(phase|ph)\b|single/i.test(s);
  const three = (bare && /3/.test(s)) || /\b3[- ]?(phase|ph)\b|three/i.test(s);
  if (!one && !three) throw new Error(`${who}: unreadable phase "${p}"`);
  return (one ? "1" : "") + (three ? "3" : "");
}

const models = [];
for (const b of BRANDS) {
  const f = path.join(DIR, b + ".json");
  if (!fs.existsSync(f)) { console.log(`skip ${b} (no ${path.relative(ROOT, f)})`); continue; }
  const raw = JSON.parse(fs.readFileSync(f, "utf8"));
  const rows = Array.isArray(raw) ? raw : raw.rows;
  let kept = 0;
  for (const r of rows) {
    if (typeof r.standby_kw !== "number") continue;
    if (!FUELS.has(r.fuel)) throw new Error(`${b}: ${r.model}: unknown fuel ${r.fuel}`);
    if (r.prime_kw != null && !(r.prime_kw > 0 && r.prime_kw <= r.standby_kw)) {
      throw new Error(`${b}: ${r.model}: prime ${r.prime_kw} is not in (0, standby ${r.standby_kw}]`);
    }
    const ph = phases(r.phase, `${b}: ${r.model}`);
    models.push([r.brand, r.model, r.fuel, r.standby_kw, r.prime_kw ?? null, ph, r.source_url || ""]);
    kept++;
  }
  console.log(`${b}: ${kept} of ${rows.length} rows`);
}
models.sort((a, b) => a[3] - b[3] || a[0].localeCompare(b[0]) || a[2].localeCompare(b[2]));

// Marelli heads: [model, leads, { "<V>": [H, F, B, standby150/40] }, url].
// Keyed "3:480" for three phase and "1:240" for the single-phase zig-zag
// connection (the 120/240 dog-leg). Only voltages Marelli tabulates appear.
const marelli = [];
const mf = path.join(DIR, "marelli.json");
if (fs.existsSync(mf)) {
  const raw = JSON.parse(fs.readFileSync(mf, "utf8"));
  const q = (o) => [o.class_h_cont_kva ?? null, o.class_f_cont_kva ?? null,
                    o.class_b_cont_kva ?? null, o.standby_150_40_kva ?? o.standby_kva ?? null];
  for (const r of raw.rows) {
    const v = { "3:480": q(r) };
    for (const [volts, o] of Object.entries(r.per_voltage_60hz || {})) v["3:" + volts] = q(o);
    const zz = r.single_phase && r.single_phase.zigzag_connection;
    if (zz && zz["240"]) v["1:240"] = q(zz["240"]);
    marelli.push([r.model, r.leads ?? null, v, r.source_url || ""]);
  }
  marelli.sort((a, b) => a[2]["3:480"][0] - b[2]["3:480"][0]);
  console.log(`marelli: ${marelli.length} heads`);
}

// Air-cooled home units: the whole spec sheet per model, for the "Air-Cooled
// Models" view (gas pressure, fuel burn, breaker, pad size…). Shipped as the
// researched objects minus nothing but empty fields — these are read one at a
// time, not scanned, so the verbose shape costs nothing at runtime.
const air = [];
for (const b of ["cummins", "generac", "kohler"]) {
  const f = path.join(DIR, `aircooled_${b}.json`);
  if (!fs.existsSync(f)) { console.log(`skip air-cooled ${b}`); continue; }
  const rows = JSON.parse(fs.readFileSync(f, "utf8"));
  for (const r of rows) {
    if (!r.brand || !r.model) throw new Error(`aircooled_${b}: row without brand/model`);
    const o = {};
    for (const [k, v] of Object.entries(r)) {
      if (v == null || v === "") continue;
      if (typeof v === "object" && !Array.isArray(v) && !Object.keys(v).length) continue;
      o[k] = v;
    }
    // Cummins' air-cooled sheet (NAS-6254) prints rated AMPS only, no kW.
    // The kW is taken from the same model's row on the Cummins rating card
    // (already in cummins.json, with its own source) — never from amps × 240,
    // which would be a conversion presented as a rating. RS20AC is the RS20A
    // genset sold with a 200 A ATS, so it reads the RS20A card row.
    for (const fuel of ["ng", "lp"]) {
      if (o[`standby_kw_${fuel}`] != null) continue;
      const base = o.model.replace(/^RS20AC\b/, "RS20A");
      const card = models.find(m => m[0] === o.brand && m[2] === fuel.toUpperCase() &&
                                    m[1].split(/[\s(]/)[0] === base);
      if (card) {
        o[`standby_kw_${fuel}`] = card[3];
        o.kw_source = `kW from ${o.brand} rating card row "${card[1]}" (${card[6]}); the air-cooled sheet prints amps only`;
      }
    }
    air.push(o);
  }
  console.log(`air-cooled ${b}: ${rows.length}`);
}
// Grouped by product line inside a brand, so Generac's two current
// generations (Guardian, and the 2026 Power Zone line) don't interleave by kW.
air.sort((a, b) => a.brand.localeCompare(b.brand) ||
  String(a.product_line || "").localeCompare(String(b.product_line || "")) ||
  (a.standby_kw_lp ?? a.standby_kw_ng ?? 0) - (b.standby_kw_lp ?? b.standby_kw_ng ?? 0));

const lines = [
  BEGIN,
  "  // [brand, model, fuel, standby kW, prime kW (null = not published), phases (\"\" = not on the sheet), spec sheet]",
  "  const GEN_MODELS = [",
  ...models.map(m => "    " + JSON.stringify(m) + ","),
  "  ];",
  "  // [model, leads, { \"<phase>:<volts>\": [Class H cont, Class F cont, Class B cont, standby 150/40] kVA @ 0.8 pf, 40°C }, source]",
  "  const GEN_MARELLI = [",
  ...marelli.map(m => "    " + JSON.stringify(m) + ","),
  "  ];",
  "  // Air-cooled home units — full spec sheet per model (docs/generator-specs/aircooled_*.json).",
  "  const GEN_AIR = [",
  ...air.map(m => "    " + JSON.stringify(m) + ","),
  "  ];",
  END,
];

const html = fs.readFileSync(HTML, "utf8");
const a = html.indexOf(BEGIN), z = html.indexOf(END);
if (a < 0 || z < 0) throw new Error("GEN_DATA markers not found in index.html");
const eol = html.includes("\r\n") ? "\r\n" : "\n";
fs.writeFileSync(HTML, html.slice(0, a) + lines.join(eol) + html.slice(z + END.length));
console.log(`wrote ${models.length} models + ${marelli.length} Marelli heads + ${air.length} air-cooled into index.html`);
