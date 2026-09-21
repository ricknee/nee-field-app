// Tier-1 regression harness for the All Charts calculators (index.html)
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

// ── Table 5A — compact stranded building wire ──────────────────────────────
// Compact wire is physically smaller than the same size in Table 5, so these
// rows are the whole reason the feature exists: on a big feeder they are worth
// a trade size of pipe. The failure mode to guard is picking the compact
// column for NON-compact wire, which undersizes the conduit and does not throw.

test("Table 5A: compact THHN is transcribed correctly at the sizes that matter", () => {
  const c = CF_INSUL.find(i => i.id === "THHN-C").area;
  const at = (sz) => c[CF_WIRE.indexOf(sz)];
  eq(at("#6"),   0.0452, "#6 compact THHN");
  eq(at("#2"),   0.1017, "#2");
  eq(at("#1/0"), 0.1590, "1/0");
  eq(at("#4/0"), 0.2733, "4/0");
  eq(at("250"),  0.3525, "250 kcmil");
  eq(at("500"),  0.6151, "500 kcmil");
  eq(at("1000"), 1.2370, "1000 kcmil");
  // Table 5A prints a DASH, not a number, for #8 THHN — it is not made.
  eq(at("#8"),   null,   "#8 compact THHN is a dash in the book");
  eq(at("#3"),   null,   "Table 5A has no #3 in any column");
  eq(CF_WIRE.slice(0, 3).every(sz => at(sz) === null), true, "and nothing below #8");
});

test("Table 5A: the other three columns are not copies of the THHN one", () => {
  const at = (id, sz) => CF_INSUL.find(i => i.id === id).area[CF_WIRE.indexOf(sz)];
  eq(at("XHHW-C", "#8"),  0.0394, "#8 compact XHHW — made, unlike THHN");
  eq(at("TW-C",   "#8"),  0.0510, "#8 compact THW/THHW");
  eq(at("RHWS-C", "#8"),  0.0531, "#8 compact RHH*");
  // 2/0 is where THHN and XHHW part company in Table 5A (.1924 vs .1885);
  // 4 sizes either side of it they are identical, so a lazy copy passes
  // everywhere except here.
  eq(at("THHN-C", "#2/0"), 0.1924, "2/0 compact THHN");
  eq(at("XHHW-C", "#2/0"), 0.1885, "2/0 compact XHHW — NOT the same number");
  eq(at("THHN-C", "#3/0"), at("XHHW-C", "#3/0"), "but 3/0 genuinely is the same");
});

// ⚠ ONE documented exception, and it is a flag on Table 5 rather than on 5A.
// In Table 5 the app groups "TW / THW / THHW / THW-2" as a single row, and at
// #8 those are NOT one number in the NEC: TW is thinner than THW. The app
// carries TW's 0.0437, and compact THW is 0.0510 — larger, which is physically
// impossible for the same insulation. That is evidence the standard row is
// holding the TW value where a THW pull needs the THW one. It is an
// UNDERSIZING error, so it is pinned here as open rather than guessed at;
// resolving it needs the book, not another website.
const KNOWN_INVERSION = new Set(["TW-C:#8"]);

test("compact is smaller than standard at every size it is made in", () => {
  const PAIRS = [["THHN-C","THHN"], ["XHHW-C","XHHW"], ["TW-C","TW"], ["RHWS-C","RHWS"]];
  for (const [cid, sid] of PAIRS) {
    const c = CF_INSUL.find(i => i.id === cid).area;
    const s = CF_INSUL.find(i => i.id === sid).area;
    CF_WIRE.forEach((sz, i) => {
      if (c[i] == null) return;
      if (KNOWN_INVERSION.has(`${cid}:${sz}`)) return;
      if (!(c[i] < s[i])) {
        throw new Error(`${cid} ${sz}: compact ${c[i]} is not below standard ${s[i]}`);
      }
    });
  }
});

test("the one known inversion is still exactly the one documented above", () => {
  const at = (id, sz) => CF_INSUL.find(i => i.id === id).area[CF_WIRE.indexOf(sz)];
  eq(at("TW", "#8"),   0.0437, "standard row still carries TW's #8 value");
  eq(at("TW-C", "#8"), 0.0510, "compact THW #8 is still larger — the open question");
  eq(KNOWN_INVERSION.size, 1, "if this grew, something was transcribed wrong");
});

// The payoff vector: the same eight conductors, one pipe size apart.
test("8 × 1/0 THHN in EMT: compact makes 2\", standard needs 2-1/2\"", () => {
  const c = run({ qty: { "#1/0": 8 }, insul: "THHN-C", conduit: "EMT" });
  eq(c.count, 8, "conductor count");
  eq(c.area.toFixed(4), "1.2720", "compact total area");
  eq(c.pct.map(pct1)[5], "37.9", '2" EMT');
  eq(CF_TRADE[c.minIdx], "2", "fits 2\" at 37.9%");

  const s = run({ qty: { "#1/0": 8 }, insul: "THHN", conduit: "EMT" });
  eq(s.area.toFixed(4), "1.4840", "standard total area");
  eq(s.pct.map(pct1)[5], "44.2", '2" EMT — over the 40% line');
  eq(CF_TRADE[s.minIdx], "2-1/2", "so standard THHN takes the next size up");
});

// ⚠ THE FAILURE THIS FEATURE COULD HAVE SHIPPED. A #12 in a compact pull has
// no Table 5A area. Skipping it would still return a number — a SMALLER one —
// and "1/2 in. is fine" would print with nothing marked wrong. So a filled-in
// size that is not made compact suppresses the whole answer.
test("a size not made compact refuses instead of quietly shrinking the pull", () => {
  const r = run({ qty: { "#12": 3, "#1/0": 8 }, insul: "THHN-C", conduit: "EMT" });
  eq(r.missing.join(","), "#12", "the offending size is named, not dropped");
  eq(r.minIdx, -1, "no minimum size is offered");
  eq(r.pct.every(p => p === null), true, "and no percentage at all is shown");
});

test("a compact pull of ONLY unmade sizes is a refusal, not an empty form", () => {
  const r = run({ qty: { "#14": 2, "#8": 1 }, insul: "THHN-C" });
  eq(r.count, 0, "nothing countable");
  eq(r.missing.join(","), "#14,#8", "both named, in grid order");
  const ok = run({ qty: { "#8": 1 }, insul: "XHHW-C" });
  eq(ok.missing.length, 0, "…but #8 IS made in compact XHHW, so that one computes");
  eq(ok.area.toFixed(4), "0.0394", "compact XHHW #8");
});

test("standard insulation types have no holes and never refuse", () => {
  for (const id of ["THHN", "XHHW", "TW", "RHWS", "RHW"]) {
    const r = run({ qty: { "#14": 1, "#3": 1, "1000": 1 }, insul: id });
    eq(r.missing.length, 0, `${id} should be made in every size in the grid`);
    eq(r.count, 3, `${id} counts all three`);
  }
});

test("every table is complete and aligned", () => {
  eq(CF_TRADE.length, 12, "trade sizes");
  eq(CF_WIRE.length, 21, "wire sizes");
  eq(CF_CONDUIT.length, 12, "conduit types");
  eq(CF_INSUL.length, 9, "insulation types — 5 standard (Table 5) + 4 compact (Table 5A)");
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
      // null is legal ONLY on a compact row — Table 5A is not made in every
      // size. A null on a standard row would be a deleted number, not a gap.
      if (v == null && i.compact) continue;
      if (!(v > 0)) throw new Error(`${i.id} has a non-positive or missing area`);
    }
    if (!i.area.some(v => v != null)) throw new Error(`${i.id} has no sizes at all`);
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
    // Compact rows have holes, so compare against the last REAL value rather
    // than the previous index — otherwise a null reads as 0 and every row after
    // a gap "ascends" trivially.
    let prev = null, prevSz = null;
    for (let k = 0; k < i.area.length; k++) {
      const v = i.area[k];
      if (v == null) continue;
      if (prev != null && v <= prev) {
        throw new Error(`${i.id} at ${CF_WIRE[k]}: ${v} follows ${prevSz}'s ${prev}`);
      }
      prev = v; prevSz = CF_WIRE[k];
    }
  }
});

test("an empty form computes nothing rather than dividing by zero", () => {
  const r = run({});
  eq(r.count, 0, "no conductors");
  eq(r.minIdx, -1, "no answer offered");
  eq(r.pct.every(p => p === null), true, "no percentages");
});

// ══════════════════════════════════════════════════════════════════════════
//  TAB 2 — GROUNDING (Tables 250.122 and 250.66)
// ══════════════════════════════════════════════════════════════════════════
const G = new Function(
  html.slice(html.indexOf("const GND_122 = ["), html.indexOf("function gndRender()")) +
  "\nreturn { GND_122, GND_66, GND_KCMIL, GND_66_CAPS, gnd122Row, gnd66Row };"
)();

test("Table 250.122: the rows match the 2023 NEC — no 30 A or 40 A row", () => {
  eq(G.GND_122.length, 19, "row count");
  eq(G.GND_122[0].amps, 15, "starts at 15 A");
  eq(G.GND_122[2].amps, 60, "⚠ the 2023 table jumps 20 → 60; no 30 A or 40 A row");
  eq(G.GND_122[G.GND_122.length - 1].amps, 6000, "ends at 6000 A");
  const at = (a) => G.GND_122.find(r => r.amps === a);
  eq(at(20).cu, "12", "20 A copper");   eq(at(20).al, "10", "20 A aluminum");
  eq(at(100).cu, "8", "100 A copper");  eq(at(200).cu, "6", "200 A copper");
  eq(at(400).cu, "3", "400 A copper — the one people guess wrong as 4");
  eq(at(1200).al, "250", "1200 A aluminum");
});

// The whole reason the missing 30 A / 40 A rows are harmless: "not exceeding"
// means you take the next row UP, so the answer is the same one the older
// table printed explicitly. If this ever regressed to "nearest" or "floor",
// a 30 A circuit would quietly be told 12 AWG.
test("250.122 lookup rounds UP to the next row — a 30 A breaker gets 10 AWG", () => {
  eq(G.gnd122Row(30).amps, 60, "30 A lands on the 60 A row");
  eq(G.gnd122Row(30).cu, "10", "…which is 10 AWG copper, as the old table said");
  eq(G.gnd122Row(40).cu, "10", "40 A likewise");
  eq(G.gnd122Row(15).cu, "14", "an exact row match still works");
  eq(G.gnd122Row(20).cu, "12", "exact");
  eq(G.gnd122Row(225).cu, "4", "225 A takes the 300 A row, not the 200");
  eq(G.gnd122Row(0), null, "nothing entered = no answer");
  eq(G.gnd122Row(7000), null, "past the table = no answer, not the last row");
});

test("Table 250.66: 7 rows, matching the utility standard and the app", () => {
  eq(G.GND_66.length, 7, "row count");
  eq(G.GND_66[0].gecCu, "8",   "2 or smaller → 8 AWG copper");
  eq(G.GND_66[0].gecAl, "6",   "…6 aluminum");
  eq(G.GND_66[3].gecCu, "2",   "over 3/0 through 350 → 2 AWG copper");
  eq(G.GND_66[3].gecAl, "1/0", "…1/0 aluminum");
  eq(G.GND_66[6].gecCu, "3/0", "over 1100 → 3/0 copper");
  eq(G.GND_66[6].gecAl, "250", "…250 kcmil aluminum");
});

test("250.66 lookup picks the row by conductor material, not one column", () => {
  // 4/0 is row 3 read as copper (over 3/0 through 350) but row 2 as aluminum
  // (4/0 or 250) — reading the wrong column is a one-size error in the answer.
  eq(G.gnd66Row(G.GND_KCMIL["4/0"], "cu").gecCu, "2", "4/0 copper service → 2 AWG");
  eq(G.gnd66Row(G.GND_KCMIL["4/0"], "al").gecCu, "4", "4/0 aluminum service → 4 AWG");
  eq(G.gnd66Row(G.GND_KCMIL["2"], "cu").gecCu, "8", "#2 copper → 8 AWG");
  eq(G.gnd66Row(500, "cu").gecCu, "1/0", "500 kcmil copper");
  eq(G.gnd66Row(2000, "cu").gecCu, "3/0", "past the last bound stays on the last row");
});

test("250.66 parallel sets are sized on combined area, not one conductor", () => {
  const one = G.gnd66Row(G.GND_KCMIL["350"], "cu").gecCu;
  const two = G.gnd66Row(G.GND_KCMIL["350"] * 2, "cu").gecCu;
  eq(one, "2",   "one 350 kcmil copper → 2 AWG");
  eq(two, "2/0", "two in parallel = 700 kcmil, which is the 'over 600' row → 2/0");
});

// ⚠ These caps live in the section text, not the table. Without them the app
// tells you to run 3/0 to a ground rod, which is legal but is money in a ditch.
test("250.66(A)-(C) electrode caps are present and correct", () => {
  const cap = (id) => G.GND_66_CAPS.find(c => c.id === id);
  eq(cap("rod").capCu, "6", "250.66(A) rod/pipe/plate → 6 AWG copper max");
  eq(cap("rod").capAl, "4", "…4 AWG aluminum max");
  eq(cap("cee").capCu, "4", "250.66(B) concrete-encased → 4 AWG copper max");
  eq(cap("ring").ring, true, "250.66(C) ground ring is capped by the ring itself");
  eq(G.GND_66_CAPS[0].capCu, null, "the default applies no cap");
  // The cap has to actually bite: a 1000 kcmil copper service to a ground rod.
  const table = G.gnd66Row(1000, "cu").gecCu;
  eq(table, "2/0", "the table alone says 2/0");
  eq(G.GND_KCMIL[table] > G.GND_KCMIL[cap("rod").capCu], true, "so 250.66(A) caps it to 6 AWG");
});

// ══════════════════════════════════════════════════════════════════════════
//  TAB 3 — AMPACITY (Table 310.16 + 310.15(B)(1) + 310.15(C)(1))
// ══════════════════════════════════════════════════════════════════════════
const A = new Function(
  html.slice(html.indexOf("const AMP_T310_16 = {"), html.indexOf("function ampRender()")) +
  "\nreturn { AMP_T310_16, AMP_SIZES, AMP_AMBIENT, AMP_CCC, ampDerated, ampSmallestFor, AMP_AMB_DEFAULT };"
)();

test("Table 310.16: copper columns match the printed table", () => {
  const cu = (s) => A.AMP_T310_16[s].cu;
  eq(cu("#14").join(), "15,20,25",    "#14 — a real row the reference app omits");
  eq(cu("#12").join(), "20,25,30",    "#12");
  eq(cu("#6").join(),  "55,65,75",    "#6");
  eq(cu("#4/0").join(),"195,230,260", "#4/0");
  eq(cu("500").join(), "320,380,430", "500 kcmil");
  eq(cu("1000").join(),"455,545,615", "1000 kcmil");
});

test("Table 310.16: aluminum columns match, and #14 has no aluminum row", () => {
  const al = (s) => A.AMP_T310_16[s].al;
  eq(al("#14"), null, "aluminum starts at 12 AWG");
  eq(al("#12").join(), "20,20,25",    "#12 — 60° and 75° are the SAME, easy to mistype");
  eq(al("#4/0").join(),"150,180,205", "#4/0");
  eq(al("500").join(), "260,310,350", "500 kcmil");
  eq(al("1000").join(),"375,445,500", "1000 kcmil");
});

test("310.15: the default row is the underated one", () => {
  eq(A.AMP_AMBIENT[A.AMP_AMB_DEFAULT].f.join(), "1,1,1", "26–30°C is the table's own basis");
  eq(A.AMP_CCC[0].f, 1, "1 to 3 conductors applies no adjustment");
  eq(A.ampDerated("#6", "cu", A.AMP_AMB_DEFAULT, 0).join(), "55,65,75", "so it reads straight off");
});

test("ampacity derating multiplies ambient AND conductor count", () => {
  // #6 copper 90° = 75 A. Six current-carrying conductors → ×0.80.
  eq(A.ampDerated("#6", "cu", A.AMP_AMB_DEFAULT, 1)[2], 60, "75 × 0.80");
  // …and at 41–45°C the 90° factor is 0.87, so 75 × 0.87 × 0.80.
  const both = A.ampDerated("#6", "cu", 7, 1)[2];
  eq(Math.abs(both - 75 * 0.87 * 0.80) < 1e-9, true, "both factors apply, not just one");
});

test("a hot ambient removes the columns that insulation cannot survive", () => {
  const hot = A.ampDerated("#6", "cu", 10, 0);   // 56–60°C
  eq(hot[0], null, "60°C wire is unusable at a 56–60°C ambient");
  eq(hot[1] != null, true, "75°C wire still is");
  const hotter = A.ampDerated("#6", "cu", 13, 0); // 71–75°C
  eq(hotter[1], null, "and 75°C wire runs out too");
  eq(hotter[2] != null, true, "only 90°C is left");
});

// ⚠ Sizing off the 90° column is the classic ampacity mistake — 110.14(C)
// normally limits terminations to 75°C. The picker must never reach for [2].
test("the load lookup sizes on the 75° column, per 110.14(C)", () => {
  eq(A.ampSmallestFor(100, "cu", A.AMP_AMB_DEFAULT, 0).size, "#3",
     "100 A copper → #3 (100 A at 75°), NOT #4 which only makes 100 A at 90°");
  eq(A.ampSmallestFor(200, "cu", A.AMP_AMB_DEFAULT, 0).size, "#3/0", "200 A copper");
  eq(A.ampSmallestFor(200, "al", A.AMP_AMB_DEFAULT, 0).size, "250",  "200 A aluminum");
  eq(A.ampSmallestFor(9999, "cu", A.AMP_AMB_DEFAULT, 0), null, "off the table = no answer");
  eq(A.ampSmallestFor(0, "cu", A.AMP_AMB_DEFAULT, 0), null, "nothing entered = no answer");
});

// ══════════════════════════════════════════════════════════════════════════
//  TAB 4 — VOLTAGE DROP (Chapter 9 Table 9)
// ══════════════════════════════════════════════════════════════════════════
const V = new Function(
  html.slice(html.indexOf("const VD_T9 = {"), html.indexOf("function vdRender()")) +
  "\nreturn { VD_T9, VD_SIZES, VD_CMIL, VD_RHO, vdCompute, vdSmallestUnder3 };"
)();
const vd = (o) => V.vdCompute({
  sys:"ac", ph:"1", mat:"cu", cond:"steel", size:"#12", sets:"1",
  len:"", amps:"", volts:"", pf:"1.0", ...o,
});

// ⚠⚠ THE BEST TEST IN THIS FILE. Table 9 prints an "effective Z at 0.85 PF"
// column that this app does NOT use — it computes Ze from R and X_L instead.
// So recomputing that column and checking it against what the NEC printed
// validates the R values, the X values, the conduit-material indexing AND the
// formula, all against a source the code never touches. If a single cell of
// Table 9 were mistyped, this is what would catch it.
test("Table 9: recomputing NEC's own 0.85 PF column reproduces it exactly", () => {
  // size: [ Ze@0.85 in PVC, in aluminum conduit, in steel ] — as printed.
  const PRINTED = {
    "#12":  [1.7,   1.7,   1.7  ],
    "#2":   [0.19,  0.19,  0.20 ],
    "#1/0": [0.13,  0.13,  0.13 ],
    "#4/0": [0.074, 0.078, 0.080],
    "250":  [0.066, 0.070, 0.073],
    "500":  [0.043, 0.048, 0.050],
    "1000": [0.032, 0.036, 0.040],
  };
  // Tolerance is ONE unit in the last printed digit, not exact equality. The
  // NEC computed that column from unrounded resistance and reactance, while
  // this app only has the rounded R and X it also printed — so a cell can land
  // one ulp away and still be right. 20 of the 21 checks below hit it exactly;
  // #4/0 in aluminum conduit computes .0785 against a printed .078. Anything
  // genuinely mistyped would be out by far more than one digit.
  for (const [size, want] of Object.entries(PRINTED)) {
    ["pvc", "al", "steel"].forEach((cond, i) => {
      const r = vd({ size, cond, pf:"0.85", len:"1000", amps:"1", volts:"100" });
      const dp = String(want[i]).split(".")[1]?.length || 0;
      const ulp = Math.pow(10, -dp);
      if (Math.abs(r.z - want[i]) > ulp * 1.0001) {
        throw new Error(`${size} in ${cond}: NEC prints ${want[i]}, computed ${r.z.toFixed(dp + 2)}`);
      }
    });
  }
});

// The guard for the bug above: if anyone "simplifies" these back to
// Object.keys(), the lists silently reorder to kcmil-first and every
// smallest-size search starts walking from 250 kcmil.
test("size lists are explicitly ordered smallest-first, not Object.keys()", () => {
  eq(A.AMP_SIZES[0], "#14", "ampacity list starts at the smallest conductor");
  eq(V.VD_SIZES[0],  "#14", "voltage-drop list likewise");
  eq(A.AMP_SIZES[13], "250", "…and kcmil sizes come after the AWG ones");
  const ordered = (list, cmil) => list.every((s, i) =>
    i === 0 || cmil(list[i - 1]) < cmil(s));
  const areaOf = (s) => V.VD_CMIL[s] ?? { "700":700000, "800":800000, "900":900000 }[s];
  eq(ordered(V.VD_SIZES, areaOf), true, "voltage-drop sizes ascend by area");
  eq(ordered(A.AMP_SIZES, areaOf), true, "ampacity sizes ascend by area");
});

test("voltage drop: a hand calculation, 1-phase", () => {
  // #12 copper, PVC, PF 1.0 → Ze is just R = 2.0 Ω/1000 ft.
  // VD = 2 × 2.0 × 20 A × 100 ft / 1000 = 8.00 V on 120 V = 6.67%.
  const r = vd({ size:"#12", cond:"pvc", len:"100", amps:"20", volts:"120" });
  eq(r.z, 2.0, "at unity power factor Ze collapses to R");
  eq(r.vd.toFixed(2), "8.00", "volts dropped");
  eq(r.pct.toFixed(2), "6.67", "percent");
  eq(r.atLoad.toFixed(1), "112.0", "volts at the load");
});

test("voltage drop: 3-phase uses √3, not 2", () => {
  const one = vd({ size:"#12", cond:"pvc", len:"100", amps:"20", volts:"208", ph:"1" });
  const three = vd({ size:"#12", cond:"pvc", len:"100", amps:"20", volts:"208", ph:"3" });
  eq((three.vd / one.vd).toFixed(4), (Math.sqrt(3) / 2).toFixed(4), "ratio is √3/2");
});

test("voltage drop: parallel sets divide the drop", () => {
  const one = vd({ size:"500", len:"200", amps:"400", volts:"480" });
  const two = vd({ size:"500", len:"200", amps:"400", volts:"480", sets:"2" });
  eq((one.vd / two.vd).toFixed(4), "2.0000", "two sets halve it");
});

// ⚠ This is why the form asks for conduit material at all. A calculator that
// ignores the raceway understates drop on every steel run, and understating
// is the direction that gets a motor started on low voltage.
test("steel conduit produces MORE drop than PVC — the reason the field exists", () => {
  const pvc   = vd({ size:"#4/0", cond:"pvc",   pf:"0.85", len:"300", amps:"150", volts:"480" });
  const steel = vd({ size:"#4/0", cond:"steel", pf:"0.85", len:"300", amps:"150", volts:"480" });
  if (!(steel.vd > pvc.vd)) throw new Error("steel must be worse than PVC");
  eq(((steel.vd / pvc.vd - 1) * 100).toFixed(0), "8", "about 8% more drop at 0.85 PF");
});

test("DC ignores reactance and uses Table 8 dc resistance", () => {
  // ρ × 1000 / cmil reproduces Table 8: #14 3.14, 1/0 0.122, 500 0.0258 Ω/kFT.
  const chk = (size, mat, want) => {
    const r = vd({ sys:"dc", mat, size, len:"100", amps:"10", volts:"120" });
    if (Math.abs(r.z - want) / want > 0.005) {
      throw new Error(`${size} ${mat}: expected ~${want} Ω/kFT, got ${r.z.toFixed(5)}`);
    }
  };
  chk("#14",  "cu", 3.14);
  chk("#1/0", "cu", 0.122);
  chk("500",  "cu", 0.0258);
  chk("500",  "al", 0.0424);
  // DC is a two-wire loop, so it uses 2 even though "3-phase" is still set.
  const a = vd({ sys:"dc", ph:"3", size:"500", len:"100", amps:"100", volts:"48" });
  const b = vd({ sys:"dc", ph:"1", size:"500", len:"100", amps:"100", volts:"48" });
  eq(a.vd, b.vd, "phase is meaningless on DC and must not change the answer");
});

test("Table 9 has no #14 aluminum, and the app says so instead of guessing", () => {
  eq(V.VD_T9["#14"][5], null, "aluminum block is null at #14");
  eq(vd({ size:"#14", mat:"al", len:"100", amps:"10", volts:"120" }).unavailable, true,
     "reported as unavailable, not computed from a null");
});

test("the 3% suggestion finds a real size, or admits there isn't one", () => {
  // 100 A at 120 V down a 200 ft run is a long way past 3% on #12.
  const s = { sys:"ac", ph:"1", mat:"cu", cond:"pvc", size:"#12", sets:"1",
              len:"200", amps:"100", volts:"120", pf:"1.0" };
  const bad = V.vdCompute(s);
  if (!(bad.pct > 3)) throw new Error("expected this run to be over 3%");
  const best = V.vdSmallestUnder3(s);
  if (!best) throw new Error("a size should exist");
  if (!(best.pct <= 3)) throw new Error("the suggestion must actually be under 3%");
  const check = V.vdCompute({ ...s, size: best.size });
  eq(check.pct <= 3, true, "and recomputing at that size agrees");
});

// ══════════════════════════════════════════════════════════════════════════
//  TAB 5 — MOTORS (Tables 430.248, 430.250, 430.251(A))
// ══════════════════════════════════════════════════════════════════════════
const M = new Function(
  html.slice(html.indexOf("const MOT_1P_V = ["), html.indexOf("function motRender()")) +
  "\nreturn { MOT_1P_FLC, MOT_3P_FLC, MOT_1P_LRA, MOT_1P_V, MOT_3P_V, MOT_1P_LRA_V," +
  " MOT_1P_HP, MOT_3P_HP, motLookup, motSizing };"
)();
const mot = (o) => M.motLookup({ ph:"1", mode:"flc", hp:"1/6", v:"115", ...o });

test("Table 430.248: single-phase full-load current", () => {
  eq(mot({ hp:"1/6", v:"115" }).amps, 4.4,  "1/6 HP at 115 V");
  eq(mot({ hp:"1/2", v:"115" }).amps, 9.8,  "1/2 HP at 115 V");
  eq(mot({ hp:"1/2", v:"230" }).amps, 4.9,  "…and half that at 230 V");
  eq(mot({ hp:"5",   v:"230" }).amps, 28,   "5 HP at 230 V");
  eq(mot({ hp:"10",  v:"115" }).amps, 100,  "10 HP at 115 V");
  eq(mot({ hp:"1/6", v:"115" }).table, "430.248", "cited table");
});

test("Table 430.250: three-phase full-load current", () => {
  const m3 = (hp, v) => mot({ ph:"3", hp, v });
  eq(m3("1/2", "115").amps, 4.4,  "1/2 HP at 115 V — same 4.4 as 1φ 1/6 HP, a real coincidence");
  eq(m3("5",   "230").amps, 15.2, "5 HP at 230 V");
  eq(m3("10",  "460").amps, 14,   "10 HP at 460 V — the everyday one");
  eq(m3("50",  "460").amps, 65,   "50 HP at 460 V");
  eq(m3("200", "460").amps, 240,  "200 HP at 460 V");
  eq(m3("100", "575").amps, 99,   "100 HP at 575 V");
});

// Table 430.250 stops listing 115 V above 2 HP. Reporting that honestly
// matters more than it looks: silently falling back to another column would
// hand over a current for a motor that does not exist at that voltage.
test("a horsepower/voltage pair the table does not list says so", () => {
  eq(mot({ ph:"3", hp:"5", v:"115" }).notListed, true, "no 5 HP at 115 V three-phase");
  eq(mot({ ph:"3", hp:"5", v:"115" }).amps, undefined, "and no number is invented");
  eq(mot({ ph:"1", hp:"10", v:"460" }).notListed, true, "430.248 has no 460 V column at all");
});

test("Table 430.251(A): single-phase locked rotor, and it has no 200 V column", () => {
  eq(M.MOT_1P_LRA_V.join(), "115,208,230", "115 / 208 / 230 only");
  eq(mot({ mode:"lra", hp:"1/2", v:"115" }).amps, 58.8, "1/2 HP at 115 V");
  eq(mot({ mode:"lra", hp:"10",  v:"230" }).amps, 300,  "10 HP at 230 V");
  eq(mot({ mode:"lra", hp:"1/6", v:"115" }).notListed, true, "430.251(A) starts at 1/2 HP");
});

// A cross-check, not the storage mechanism: 430.251(A) is exactly 6× 430.248.
// If a locked-rotor cell were mistyped this catches it, and if a full-load
// cell were mistyped it catches that too.
test("430.251(A) is exactly 6× 430.248 — cross-checks BOTH tables at once", () => {
  for (const hp of Object.keys(M.MOT_1P_LRA)) {
    [115, 230].forEach(v => {
      const flc = M.MOT_1P_FLC[hp][M.MOT_1P_V.indexOf(v)];
      const lra = M.MOT_1P_LRA[hp][M.MOT_1P_LRA_V.indexOf(v)];
      if (Math.abs(lra - flc * 6) > 0.05) {
        throw new Error(`${hp} HP at ${v} V: LRA ${lra} vs 6 × FLC ${flc} = ${flc * 6}`);
      }
    });
  }
});

// ⚠ Deliberately absent, not derived. The 6× identity above holds for the
// single-phase table; assuming it holds for 430.251(B) would be a guess
// presented as a code lookup.
test("three-phase locked rotor is reported missing, never estimated", () => {
  const r = mot({ ph:"3", mode:"lra", hp:"10", v:"460" });
  eq(r.missing, "430.251(B)", "named so the user knows what to go look up");
  eq(r.amps, undefined, "and no number is offered");
});

// ⚠ Regression guard for a real bug: the voltage list was keyed off MODE
// alone, so switching to Locked Rotor swapped in Table 430.251(A)'s short
// 115/208/230 list even on three phase — silently removing 460 V, which is
// most of the three-phase work. The list is per (mode × phase).
test("Locked Rotor on three phase keeps 460 V and 575 V in the list", () => {
  eq(M.MOT_1P_LRA_V.includes(460), false, "430.251(A) genuinely has no 460 V");
  eq(M.MOT_3P_V.includes(460), true,  "but the three-phase list must");
  eq(M.MOT_3P_V.includes(575), true,  "…and 575 V");
  eq(M.MOT_3P_V.includes(200), true,  "…and 200 V, which 430.251(A) also lacks");
  // The FLC path must keep answering at 460 V regardless of what LRA can do.
  eq(mot({ ph:"3", hp:"10", v:"460" }).amps, 14, "10 HP at 460 V still reads");
});

test("Article 430 sizing multipliers", () => {
  // 10 HP, 3-phase, 460 V → 14 A from Table 430.250.
  const z = M.motSizing(14);
  eq(z.conductor.toFixed(1),  "17.5", "430.22 — 125%");
  eq(z.disconnect.toFixed(1), "16.1", "430.110(A) — 115%");
  eq(z.breaker.toFixed(1),    "35.0", "430.52 — inverse-time breaker 250% max");
  eq(z.fuseDE.toFixed(1),     "24.5", "430.52 — dual-element fuse 175% max");
  eq(z.fuseNTD.toFixed(1),    "42.0", "430.52 — non-time-delay fuse 300% max");
  eq(M.motSizing(0), null, "nothing in, nothing out");
});

// ══════════════════════════════════════════════════════════════════════════
//  TAB 6 — GENERATORS (GeneratorJoe amp charts + manufacturer spec sheets)
// ══════════════════════════════════════════════════════════════════════════
const GN = new Function(
  html.slice(html.indexOf("const GEN_1P_V = ["), html.indexOf("// ── TAB 3: AMPACITY")) +
  "\nreturn { GEN_1P_V, GEN_3P_V, GEN_MODELS, GEN_MARELLI, GEN_AIR, genAmps, genRule, genModels, genMarelli, genAirSpec };"
)();
const a1 = (kw, v) => +GN.genAmps(kw, v, "1").toFixed(1);
const a3 = (kw, v) => +GN.genAmps(kw, v, "3").toFixed(1);

// Cells read off the supplied PDFs, spread across both ends of each table and
// across columns — so a changed constant (√3 for 1.73, a dropped 0.8) fails.
test("single-phase amps match the chart (100% pf)", () => {
  eq(a1(1, 120), 8.3, "1 kW at 120 V");
  eq(a1(41, 240), 170.8, "41 kW at 240 V");
  eq(a1(239, 240), 995.8, "239 kW at 240 V");
  eq(a1(280, 120), 2333.3, "280 kW at 120 V — last row");
});

test("three-phase amps match the chart (80% pf, 1.73)", () => {
  eq(a3(4, 480), 6.0, "4 kW at 480 V — first row");
  eq(a3(155, 120), 933.3, "155 kW at 120 V — this is the cell that proves 1.73, not √3 (932.2)");
  eq(a3(165, 450), 264.9, "165 kW at 450 V");
  eq(a3(500, 208), 1736.9, "500 kW at 208 V");
  eq(a3(1000, 480), 1505.3, "1000 kW at 480 V");
  eq(a3(2000, 4160), 347.4, "2000 kW at 4160 V");
  eq(a3(100, 11000), 6.6, "100 kW at 11 kV");
  eq(a3(4000, 13800), 209.4, "4000 kW at 13.8 kV — last cell");
  eq(GN.GEN_3P_V.length, 17, "every voltage column on the chart");
});

test("rule of thumb: prime is 90% of standby, both ways", () => {
  eq(GN.genRule(100, "standby").prime, 90, "100 standby → 90 prime");
  eq(+GN.genRule(90, "prime").standby.toFixed(6), 100, "90 prime → 100 standby");
  eq(GN.genRule(0, "standby"), null, "nothing in, nothing out");
});

// ⚠ The real-model list must never invent a prime rating. A standby-only set
// is skipped on a prime basis, and counted so the screen can say so.
test("real models: never a prime the spec sheet does not print", () => {
  for (const m of GN.GEN_MODELS) {
    if (m[4] != null && !(m[4] <= m[3])) throw new Error(`${m[0]} ${m[1]}: prime ${m[4]} > standby ${m[3]}`);
  }
  const r = GN.genModels({ ph:"1", kw:"22", basis:"prime", brand:"" });
  eq(r.rows.every(m => m[4] >= 22), true, "every prime-basis match has a published prime ≥ 22");
  eq(r.standbyOnly > 0, true, "and the air-cooled 22 kW units are counted as left out");
  const s = GN.genModels({ ph:"1", kw:"22", basis:"standby", brand:"Generac" });
  eq(s.rows.some(m => m[1].startsWith("Guardian 22") && m[2] === "LP"), true, "Guardian 22 LP carries 22 kW standby");
  eq(s.rows.some(m => m[1].startsWith("Guardian 22") && m[2] === "NG"), false,
     "…but NOT on natural gas (19.5 kW) — the NG pick is the next size up");
});

test("real models: nothing over 2× the ask", () => {
  const r = GN.genModels({ ph:"1", kw:"22", basis:"standby", brand:"" });
  eq(r.rows.every(m => m[3] >= 22 && m[3] <= 44), true, "22 kW → 22…44 kW only");
});

test("real models: a three-phase-only set never answers a single-phase job", () => {
  const r = GN.genModels({ ph:"1", kw:"300", basis:"standby", brand:"" });
  eq(r.rows.every(m => m[5] === "" || m[5].includes("1")), true, "only 1φ-capable or unlisted");
  // Cat's sheet reads "3-phase (1-phase not confirmed)" — it must not count as 1φ.
  const d40 = GN.GEN_MODELS.find(m => m[0] === "Cat" && m[1] === "D40 GC");
  if (d40) eq(d40[5], "3", "Cat D40 GC is 3φ until 1φ is confirmed");
});

test("the data block in index.html is what the JSON says", () => {
  const dir = path.join(ROOT, "docs", "generator-specs");
  let n = 0;
  for (const b of ["generac", "cummins", "cat", "kohler"]) {
    const f = path.join(dir, b + ".json");
    if (!fs.existsSync(f)) continue;
    const raw = JSON.parse(fs.readFileSync(f, "utf8"));
    n += (Array.isArray(raw) ? raw : raw.rows).filter(r => typeof r.standby_kw === "number").length;
  }
  eq(GN.GEN_MODELS.length, n, "re-run tools/build-generator-specs.mjs after editing the JSON");
});

test("Marelli: picks the smallest head, on the chart's pf basis", () => {
  // 100 kW standby 3φ 480 V → 125 kVA. MXB-E 225 XB4 standby is 105 (too small), SB4 is 131.
  const h = GN.genMarelli("3", 480, 100, 90);
  eq(h.standby[0], "MXB-E 225 SB4", "standby 150/40 ≥ 125 kVA");
  // Prime 90 kW → 112.5 kVA Class H. XB4 is 100 (too small), SB4 is 125.
  eq(h.prime[0], "MXB-E 225 SB4", "Class H continuous ≥ 112.5 kVA");
  eq(h.ref, false, "480 V is tabulated");
});

test("Marelli: an untabulated voltage is labelled, never derived", () => {
  const h = GN.genMarelli("3", 208, 100, 90);
  eq(h.ref, true, "208 V three phase answers AT 480 V, flagged");
  eq(h.key, "3:480", "…from the 480 V column");
  eq(GN.genMarelli("1", 120, 10, 9).none, true, "1φ 120 V: Marelli tabulates nothing — no stand-in");
  eq(GN.genMarelli("1", 240, 10, 9).standby[0].startsWith("MXB-E"), true, "1φ 240 V zig-zag is tabulated");
});

// A spec line the sheet does not give must be ABSENT, not "0" or "—": a blank
// 25%-load burn is not a sheet saying zero. And fuel picks its own columns.
test("air-cooled spec card: fuel-specific, blanks dropped, nothing converted", () => {
  const m = { brand:"X", model:"T1", standby_kw_ng:18, standby_kw_lp:20,
              ng_inlet_pressure_inwc_min:5, ng_inlet_pressure_inwc_max:7,
              lp_inlet_pressure_inwc_min:10, lp_inlet_pressure_inwc_max:12,
              ng_cfh_100:300, lp_cfh_100:120, lp_gph_100:3.3, ng_cfh_25:null };
  const flat = (f) => Object.fromEntries(GN.genAirSpec(m, f).flatMap(s => s.rows));
  const ng = flat("ng"), lp = flat("lp");
  eq(ng["Standby rating"], "18 kW", "NG rating");
  eq(lp["Standby rating"], "20 kW", "LP rating");
  eq(ng["Inlet pressure"], "5–7 in. W.C.", "NG pressure");
  eq(lp["Inlet pressure"], "10–12 in. W.C.", "LP pressure");
  eq(ng["Fuel use at 100% load"], "300 ft³/hr", "NG burn as printed");
  eq(lp["Fuel use at 100% load"], "120 ft³/hr · 3.3 gal/hr", "LP keeps both units");
  eq("Fuel use at 25% load" in ng, false, "a missing load point is not a row");
  eq(GN.genAirSpec(m, "ng").some(s => s.title === "Engine"), false, "an empty section is not shown");
});

test("air-cooled spec card: a printed gas-pipe table lands in the gas section, per fuel", () => {
  const m = { brand:"X", model:"T2", standby_kw_ng:18,
              other: { min_gas_pipe_npt: { ng: { "25ft":"1", "100ft":"1 1/4" }, lp: { "25ft":"3/4" } },
                       fuel_exercise_cfh: { ng:71, lp:35 }, derating: { temp:"2% per 10°F" } } };
  const gas = (f) => Object.fromEntries(GN.genAirSpec(m, f).find(s => s.title.startsWith("Gas")).rows);
  eq(gas("ng")["Min. gas pipe, 100 ft run"], "1 1/4 in. NPT", "NG table");
  eq(gas("lp")["Min. gas pipe, 25 ft run"], "3/4 in. NPT", "LP reads the LP column, not NG's");
  eq("Min. gas pipe, 100 ft run" in gas("lp"), false, "…and only the lengths LP lists");
  eq(gas("ng")["Fuel use while exercising"], "71 ft³/hr", "exercise burn per fuel");
  const other = Object.fromEntries(GN.genAirSpec(m, "ng").find(s => s.title === "Other").rows);
  eq("min gas pipe npt" in other, false, "not repeated as a blob under Other");
  eq(other["derating"], "temp: 2% per 10°F", "nested fields read as text, not JSON");
});

test("air-cooled data: every model names a brand, a model and a source", () => {
  for (const m of GN.GEN_AIR) {
    if (!m.brand || !m.model) throw new Error("row without brand/model");
    if (!m.source_url) throw new Error(`${m.brand} ${m.model}: no spec-sheet source`);
    if (m.standby_kw_ng == null && m.standby_kw_lp == null) throw new Error(`${m.brand} ${m.model}: no rating`);
  }
});

// ── report ──
console.log("\nAll Charts — Conduit Fill + Grounding (NEC) tests\n" + "-".repeat(48));
for (const [mark, name] of log) console.log(` ${mark} ${name}`);
console.log("-".repeat(48));
console.log(`${pass} passed, ${fail} failed\n`);
process.exit(fail ? 1 : 0);
