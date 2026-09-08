// Tier-1 regression harness for the All Charts → Conduit Fill calculator
// ---------------------------------------------------------------------------
// This one is unusual: the code under test lives INSIDE index.html, because
// sw.js is cache-first for separate .js assets and a stale conduit table is
// exactly the kind of wrong answer nobody would notice (see the comment above
// NEC_EDITION in index.html). So instead of importing a module, this file
// slices the pure, DOM-free part of the calculator back out of the SPA and
// evaluates it.
//
// ⚠ WHY THIS FILE EXISTS AT ALL. Everything here is transcribed NEC Chapter 9
// data — ~250 hand-typed decimals. A mistyped conduit area does not throw. It
// returns a plausible percentage and quietly tells an electrician that 1-1/2"
// is enough pipe. This project has been bitten repeatedly by failures that
// match nothing rather than crash; this is the same shape, so the numbers get
// pinned to known-good vectors instead of trusted.
//
// The vectors in cases 1 and 2 are not invented: they are read off a working
// commercial conduit-fill app (9 × #12 THHN, EMT then FMC), which makes them
// an independent oracle for both Table 4 and Table 5.
//
// Run (portable node):
//   & "C:\Users\irick\nodejs\node.exe" tests/conduit-fill.test.mjs
// or, if node is on PATH:
//   node tests/conduit-fill.test.mjs
// Exit code is 0 on all-pass, 1 on any failure (CI-friendly).
// ---------------------------------------------------------------------------

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const html = fs.readFileSync(path.join(ROOT, "index.html"), "utf8");

// ── slice the pure calculator out of the SPA ──
// Anchored on the first DOM-touching function that follows it. If someone
// reorders the block this throws loudly rather than testing nothing.
const START = "function cfAllowedPct(";
const END   = "function cfRenderExtras()";
const a = html.indexOf(START);
const b = html.indexOf(END);
if (a === -1 || b === -1 || b <= a) {
  console.error(
    "✗ could not locate the conduit-fill block in index.html.\n" +
    `  looked for ${JSON.stringify(START)} … ${JSON.stringify(END)}.\n` +
    "  If the code moved, move these anchors — do not delete the test."
  );
  process.exit(1);
}
const src = html.slice(a, b);

const CALC = new Function(
  src + "\nreturn { cfAllowedPct, cfCompute, CF_TRADE, CF_CONDUIT, CF_WIRE, CF_INSUL };"
)();
const { cfAllowedPct, cfCompute, CF_TRADE, CF_CONDUIT, CF_WIRE, CF_INSUL } = CALC;

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
// The UI prints one decimal, so that is the resolution the vectors are pinned at.
const pct1 = (p) => (p == null ? "–" : p.toFixed(1));
const run = (o) => cfCompute({
  qty: o.qty || {}, extras: o.extras || [],
  insul: o.insul || "THHN", conduit: o.conduit || "EMT", nipple: !!o.nipple,
});

// ── cases ──

test("Table 4 + Table 5 vector: 9 × #12 THHN in EMT (from a working app)", () => {
  const r = run({ qty: { "#12": 9 }, conduit: "EMT" });
  eq(r.count, 9, "conductor count");
  eq(r.area.toFixed(4), "0.1197", "total conductor area");
  eq(r.allowed, 40, "over 2 conductors = 40%");
  eq(r.pct.map(pct1).join(" "),
     "39.4 22.5 13.9 8.0 5.9 3.6 2.0 1.4 1.0 0.8 – –",
     "percent-fill row");
  eq(CF_TRADE[r.minIdx], "1/2", "minimum size allowed");
});

test("Table 4 vector: the same 9 × #12 THHN in FMC", () => {
  const r = run({ qty: { "#12": 9 }, conduit: "FMC" });
  const p = r.pct.map(pct1);
  eq(p[0],  "37.8", '1/2"');
  eq(p[1],  "22.5", '3/4"');
  eq(p[4],  "6.4",  '1-1/2"');
  eq(p[5],  "3.7",  '2"');
  eq(p[6],  "2.4",  '2-1/2"');
  eq(p[7],  "1.7",  '3"');
  eq(p[10], "–",    'FMC has no 5"');
  eq(p[11], "–",    'FMC has no 6"');
  eq(CF_TRADE[r.minIdx], "1/2", "minimum size allowed");
});

// A third independent vector off the same app, and a much better one than the
// first two: it mixes THREE conductor sizes, lands in a different conduit type,
// and its answer is a mid-table trade size rather than the smallest one.
test("mixed sizes vector: 1 × #1/0 + 1 × 500 + 2 × 750 THHN in PVC-40", () => {
  const r = run({ qty: { "#1/0": 1, "500": 1, "750": 2 }, conduit: "PVC40" });
  eq(r.count, 4, "conductor count");
  eq(r.area.toFixed(4), "2.9920", "total conductor area");
  const p = r.pct.map(pct1);
  eq(p[5],  "90.9", '2"');
  eq(p[6],  "63.7", '2-1/2"');
  eq(p[7],  "41.2", '3"');
  eq(p[8],  "30.7", '3-1/2"');
  eq(p[9],  "23.8", '4"');
  eq(p[10], "15.1", '5"');
  eq(p[11], "10.5", '6"');
  eq(CF_TRADE[r.minIdx], "3-1/2", '3" is 41.2% — just over the 40% line');
});

test("Table 1: allowed percentage by conductor count", () => {
  eq(cfAllowedPct(1, false), 53, "1 conductor");
  eq(cfAllowedPct(2, false), 31, "2 conductors");
  eq(cfAllowedPct(3, false), 40, "over 2");
  eq(cfAllowedPct(40, false), 40, "still over 2");
  eq(cfAllowedPct(2, true),  60, "Note 4 nipple overrides the count");
});

test("Note 4: a nipple lets a smaller conduit carry the same pull", () => {
  const q = { qty: { "#12": 13 }, conduit: "EMT" };
  eq(CF_TRADE[run(q).minIdx], "3/4", "56.9% is over 40% in a normal run");
  eq(CF_TRADE[run({ ...q, nipple: true }).minIdx], "1/2", "but under 60% in a nipple");
});

test("Note 9: an extra cable is figured as a circle of its O.D.", () => {
  const r = run({ extras: [{ od: "1.000", qty: "1" }], conduit: "EMT" });
  eq(r.count, 1, "one cable = one conductor");
  eq(r.area.toFixed(4), (Math.PI / 4).toFixed(4), "πr² on the overall diameter");
  eq(r.allowed, 53, "a single conductor gets 53%");
  eq(CF_TRADE[r.minIdx], "1-1/4", "52.5% of 1-1/4\" EMT just makes it");
});

test("Type-EB has NO 2-1/2\" — the scan must skip the hole, not fall in it", () => {
  const eb = CF_CONDUIT.find(c => c.id === "PVCEB");
  eq(eb.area[6], null, '2-1/2" is absent from Table 4');
  eq(eb.area.slice(0, 5).every(v => v === null), true, 'Type-EB starts at 2"');
  const r = run({ qty: { "500": 4 }, conduit: "PVCEB" });
  eq(CF_TRADE[r.minIdx], "3", 'too big for 2", so the answer is 3" and not 2-1/2"');
});

test("an over-stuffed pull reports 'does not fit' rather than a bogus size", () => {
  const r = run({ qty: { "1000": 30 }, conduit: "EMT" });
  eq(r.minIdx, -1, "no trade size works");
  eq(r.pct[9] > r.allowed, true, "even 4\" is over");
});

// ⚠ This is the specific wrong-number this calculator was most likely to ship.
// Several widely-copied Table 5 reproductions print the RHH*/RHW* small sizes
// (.0209/.0260/.0333) under a "THW" heading. They are not THW. Getting these
// four #14 values right is the difference between a correct pull and a pipe
// sized off the wrong column, and nothing about the wrong answer looks wrong.
test("Table 5: the four #14 columns are distinct and not transposed", () => {
  const at14 = (id) => CF_INSUL.find(i => i.id === id).area[0];
  eq(at14("THHN"), 0.0097, "#14 THHN");
  eq(at14("XHHW"), 0.0139, "#14 XHHW");
  eq(at14("TW"),   0.0139, "#14 TW/THW — NOT 0.0209");
  eq(at14("RHWS"), 0.0209, "#14 RHH/RHW without outer covering");
  eq(at14("RHW"),  0.0293, "#14 RHH/RHW with outer covering");
});

test("Table 5: TW and RHW* converge at #6 and stay equal (they do in the NEC)", () => {
  const tw = CF_INSUL.find(i => i.id === "TW").area;
  const rs = CF_INSUL.find(i => i.id === "RHWS").area;
  for (let i = 0; i < 4; i++) {
    if (tw[i] === rs[i]) throw new Error(`${CF_WIRE[i]} should differ between TW and RHW*`);
  }
  for (let i = 4; i < tw.length; i++) {
    eq(rs[i], tw[i], `${CF_WIRE[i]} should match`);
  }
});

test("every table is complete and aligned", () => {
  eq(CF_TRADE.length, 12, "trade sizes");
  eq(CF_WIRE.length, 21, "wire sizes");
  eq(CF_CONDUIT.length, 12, "conduit types");
  eq(CF_INSUL.length, 5, "insulation types");
  for (const c of CF_CONDUIT) {
    eq(c.area.length, CF_TRADE.length, `${c.id} area row length`);
    if (!c.area.some(v => v != null)) throw new Error(`${c.id} has no sizes at all`);
    for (const v of c.area) {
      if (v != null && !(v > 0)) throw new Error(`${c.id} has a non-positive area`);
    }
  }
  for (const i of CF_INSUL) {
    eq(i.area.length, CF_WIRE.length, `${i.id} area row length`);
    for (const v of i.area) {
      if (!(v > 0)) throw new Error(`${i.id} has a non-positive area`);
    }
  }
});

// A transposed digit (1.496 → 1.469) usually survives every other check here,
// because it is still a plausible number in a plausible place. It does not
// survive this one: conduit and conductor areas both rise with size, always.
test("areas ascend monotonically — catches a transposed digit", () => {
  for (const c of CF_CONDUIT) {
    const v = c.area.filter(x => x != null);
    for (let i = 1; i < v.length; i++) {
      if (v[i] <= v[i - 1]) throw new Error(`${c.id}: ${v[i]} follows ${v[i - 1]}`);
    }
  }
  for (const i of CF_INSUL) {
    for (let k = 1; k < i.area.length; k++) {
      if (i.area[k] <= i.area[k - 1]) {
        throw new Error(`${i.id} at ${CF_WIRE[k]}: ${i.area[k]} follows ${i.area[k - 1]}`);
      }
    }
  }
});

test("an empty form computes nothing rather than dividing by zero", () => {
  const r = run({});
  eq(r.count, 0, "no conductors");
  eq(r.minIdx, -1, "no answer offered");
  eq(r.pct.every(p => p === null), true, "no percentages");
});

// ── report ──
console.log("\nAll Charts → Conduit Fill (NEC Chapter 9) tests\n" + "-".repeat(48));
for (const [mark, name] of log) console.log(` ${mark} ${name}`);
console.log("-".repeat(48));
console.log(`${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);
