// Tier-1 regression harness for All Charts tab 7, LOAD CALC (index.html)
// ---------------------------------------------------------------------------
// Same approach as conduit-fill.test.mjs: the calculator lives INSIDE
// index.html (sw.js is cache-first for separate assets), so this slices the
// pure, DOM-free block back out of the SPA and evaluates it.
//
// ⚠ WHY. A load calculation that applies one wrong demand factor does not
// throw — it returns a smaller, plausible number. That is exactly what the
// Trail Cabinet drawings did (Revit's 220.44 receptacle factor on every
// "Power" load: 253 A on paper, ~391 A by Article 220). So each rule is pinned
// on its own, and the whole Trail Cabinet load list is pinned end to end.
//
// Run (portable node):
//   & "C:\Users\irick\nodejs\node.exe" tests/load-calc.test.mjs
// Exit code is 0 on all-pass, 1 on any failure.
// ---------------------------------------------------------------------------

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const html = fs.readFileSync(path.join(ROOT, "index.html"), "utf8");

// The load calc reads the motor FLC tables (HP entry) and genModels (the
// generator picks), so those blocks are sliced in ahead of it.
function slice(start, end) {
  const a = html.indexOf(start), b = html.indexOf(end, a);
  if (a === -1 || b === -1) {
    console.error(`✗ could not locate ${JSON.stringify(start)} … ${JSON.stringify(end)} in index.html.\n` +
      "  If the code moved, move these anchors — do not delete the test.");
    process.exit(1);
  }
  return html.slice(a, b);
}
const L = new Function(
  slice("const MOT_1P_V = [", "function motRender()") +
  slice("// ── TAB 7: LOAD CALC", "// ── Load Calc: screen ──") +
  "\nreturn { lcCompute, lcGen, lcLineVA, lcExampleCalc, genModels, genAmps, LC_T220_12, LC_STD_OCPD };"
)();

// ── harness ──
let pass = 0, fail = 0;
const log = [];
function test(name, fn) {
  try { fn(); log.push(["✓", name]); pass++; }
  catch (e) { log.push(["✗", `${name} — ${e.message}`]); fail++; }
}
const eq = (a, b, m) => {
  if (a !== b) throw new Error(`${m || ""} expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`);
};
const r1 = (n) => Math.round(n * 10) / 10;
const line = (type, val, o = {}) => ({ desc: o.desc || type, type, qty: o.qty ?? "1", val: String(val),
  unit: o.unit || "va", lv: o.lv || "480-3", start: o.start || "atl" });
const calc = (lines, o = {}) => L.lcCompute({ sys: o.sys || "480-3", sqft: o.sqft || "", occ: o.occ || "", lines });

// ══════════════════════════════════════════════════════════════════════════
test("220.44 — receptacles: first 10 kVA at 100%, the rest at 50%", () => {
  eq(calc([line("recep", 21900)]).recepD, 15950, "21,900 VA → 15,950");
  eq(calc([line("recep", 8000)]).recepD, 8000, "under 10 kVA is all counted");
  eq(calc([line("recep", 50, { unit: "rec" })]).sum.recep, 9000, "50 receptacles × 180 VA (220.14(I))");
});

test("the receptacle factor is NOT applied to motors (the Revit trap)", () => {
  const r = calc([line("motor", 188794)]);
  eq(r1(r.total), r1(188794 * 1.25), "one motor load: 100% + 25% largest — never 10k + 50%");
});

test("lighting: 125%, with the 220.12 floor only ever raising it", () => {
  eq(calc([line("light", 8000)]).lightD, 10000, "8,000 VA × 125%");
  const office = L.LC_T220_12.find(o => o[0] === "Office");
  const r = calc([line("light", 5000)], { sqft: "10000", occ: "Office" });
  eq(r1(r.lightD), r1(10000 * office[1] * 1.25), "floor area sets it when larger");
  const r2 = calc([line("light", 50000)], { sqft: "10000", occ: "Office" });
  eq(r2.lightD, 62500, "actual fixtures win when larger");
});

test("motors: HP uses the TABLE FLC at the rated column for the system voltage", () => {
  const x = L.lcLineVA(line("motor", "10", { unit: "hp", lv: "480-3" }));
  eq(x.flc, 14, "10 HP at 480 V 3φ → Table 430.250 460 V column, 14 A");
  eq(r1(x.unitVA), r1(14 * 480 * Math.sqrt(3)), "VA at the nominal 480 V");
  eq(L.lcLineVA(line("motor", "5", { unit: "hp", lv: "240-1" })).flc, 28, "5 HP 1φ 240 V → 230 V column of 430.248");
  eq(L.lcLineVA(line("motor", "5", { unit: "hp", lv: "208-3" })).flc, 16.7, "5 HP 3φ 208 V → its own column");
});

test("an unusable line REFUSES the total instead of being skipped", () => {
  const r = calc([line("motor", 10000), line("motor", "5", { unit: "hp", lv: "277-1" })]);
  eq(r.errors.length, 1, "277 V has no motor-table column");
  eq(r.total, null, "total withheld");
  eq(r.amps, null, "amps withheld");
  const b = calc([line("motor", 10000), { ...line("motor", ""), desc: "RTU-4" }]);
  eq(b.blanks.length, 1, "a named line with no size is reported…");
  eq(b.total > 0, true, "…but does not block the rest");
});

test("430.24 / 440.33 — 25% of the largest single motor, per UNIT", () => {
  const r = calc([line("motor", 3000, { qty: "6" })]);
  eq(r.bigD, 750, "six 3 kVA openers: the largest motor is 3 kVA, not 18");
});

test("220.60 — only the larger of heating and cooling counts; fans count either way", () => {
  const r = calc([line("cool", 30000), line("heat", 20000), line("fan", 2000)]);
  eq(r.hvacD, 32000, "cooling 30k + fans 2k; the 20k of heat drops out");
  const h = calc([line("cool", 10000), line("heat", 40000)]);
  eq(h.hvacD, 40000, "heating wins");
  eq(h.big, null, "…and a compressor on the side that lost is not the largest motor");
});

test("Table 220.56 — kitchen units, never below the two largest together", () => {
  eq(calc([line("kitchen", 10000, { qty: "3" })]).kitchenD, 27000, "3 units → 90%");
  eq(calc([line("kitchen", 10000, { qty: "6" })]).kitchenD, 39000, "6 units → 65%");
  const r = calc([line("kitchen", 40000), line("kitchen", 30000), line("kitchen", 1000, { qty: "4" })]);
  eq(r.kitchenD, 70000, "6 units at 65% is 48,100 — raised to the two largest, 70,000");
});

test("continuous other loads at 125%, non-continuous at 100%", () => {
  eq(calc([line("cont", 10000)]).total, 12500, "continuous");
  eq(calc([line("other", 10000)]).total, 10000, "not continuous");
});

test("next standard size is 240.6(A), never rounded down", () => {
  const r = calc([line("other", 400 * 480 * Math.sqrt(3) + 1)]);
  eq(r.std, 450, "a hair over 400 A → 450");
  eq(L.LC_STD_OCPD.includes(400) && L.LC_STD_OCPD.includes(1200), true, "standard sizes present");
});

// ── Trail Cabinet, end to end ──────────────────────────────────────────────
// The engineer's MDP classes: HVAC 113,041 + Power 188,794 + Lighting 9,179 =
// 311,014 VA. The example's lines reconcile to that within ~0.2% (the two
// Nederman motors come from Table 430.250 here, not the schedule).
test("Trail Cabinet — connected load reconciles to the engineer's schedules", () => {
  const r = L.lcCompute(L.lcExampleCalc());
  eq(r.errors.length, 0, "no bad lines");
  eq(Math.abs(r.connected - 311014) / 311014 < 0.003, true, `connected ${Math.round(r.connected)} vs 311,014`);
  eq(r.sum.recep, 21900, "receptacles");
  eq(r.sum.light, 9144, "lighting");
  eq(r.sum.cont, 27840, "the two forklift chargers");
});

test("Trail Cabinet — Article 220 comes to ~391 A: the 400 A main holds, barely", () => {
  const r = L.lcCompute(L.lcExampleCalc());
  eq(r.big.desc, "RTU-1 — MDP", "largest motor is RTU-1 (cooling wins 220.60)");
  eq(Math.round(r.total), 324823, "total VA");
  eq(r1(r.amps), 390.7, "amps at 480 V 3φ");
  eq(r.std, 400, "next standard size");
});

test("Trail Cabinet — generator: standby off the full load, prime off the measured peak", () => {
  const c = L.lcExampleCalc();
  const r = L.lcCompute(c);
  const g = L.lcGen(c, r);
  eq(r1(L.genAmps(g.standbyKw, 480, "3")), r1(r.amps), "standby kW is the Generators tab's own basis");
  eq(r1(g.standbyKw), 259.6, "390.7 A at 480 V 3φ");
  eq(g.fromMeas, true, "the example carries the 160 A measured at their current shop");
  eq(g.primeA, 200, "160 A × 125%");
  eq(r1(g.primeKw), 132.9, "prime kW");
  eq(g.start.desc, "RTU-1 — MDP", "hardest start");
  eq(r1(g.start.kva), r1(49.029 * 6), "≈ 6× running across the line");
  const withAdd = L.lcGen({ ...c, addA: "30" }, r);
  eq(withAdd.primeA, 230, "+ 30 A added since the measurement");
  const noMeas = L.lcGen({ ...c, measA: "" }, r);
  eq(noMeas.primeA, r.amps, "no measurement → prime falls back to the calculated load");
});

test("generator picks never offer a prime the spec sheet doesn't print", () => {
  const m = L.genModels({ ph: "3", kw: "132.9", basis: "prime", brand: "" });
  eq(m.rows.length > 0, true, "something carries 133 kW prime");
  eq(m.rows.every(x => x[4] != null && x[4] >= 132.9), true, "every pick has a published prime ≥ the need");
});

// ── report ──
console.log("\nAll Charts — Load Calc tests\n" + "-".repeat(48));
for (const [mark, name] of log) console.log(` ${mark} ${name}`);
console.log("-".repeat(48));
console.log(`${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);
