// Tier-1 tests for netlify/functions/_ticket-lines.js — the supplier ticket reader.
// ---------------------------------------------------------------------------
// Fully offline: the fixtures are the positioned words pdf.js pulled out of real
// CED and Wolff PDFs (tests/fixtures/ticket-lines/*.json), so the parsers are
// exercised on real layouts without the PDF library or the network.
//
// Run:  & "C:\Users\irick\nodejs\node.exe" tests/ticket-lines.test.mjs
// Exit code 0 on all-pass, 1 on any failure.
//
// The cases that matter most are the money traps, not the happy path:
//   • CED's PER column — "275.25 C" is $2.7525 EACH; a misread must not pass.
//   • A credit memo prints qty AND price negative.
//   • Wolff's trailing minus ("773.85-") and missing leading zero (".9885").
//   • The wrong reader on the wrong vendor must never come back 'ok'.
//   • A PDF whose total ≠ the stored invoice amount must not come back 'ok'.
// ---------------------------------------------------------------------------
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { num, parseCedPages, parseWolffPages, judgeTicket, readPendingTicketLines }
  from "../netlify/functions/_ticket-lines.js";

const here = dirname(fileURLToPath(import.meta.url));
const fx = name => JSON.parse(readFileSync(join(here, "fixtures", "ticket-lines", `${name}.json`), "utf8")).pages;

let pass = 0, fail = 0;
function t(name, fn) {
  try { fn(); pass++; console.log(`  ✓ ${name}`); }
  catch (e) { fail++; console.log(`  ✗ ${name}\n      ${e.message}`); }
}
function eq(a, b, msg = "") {
  if (JSON.stringify(a) !== JSON.stringify(b)) throw new Error(`${msg} expected ${JSON.stringify(b)}, got ${JSON.stringify(a)}`);
}
const line = (p, part) => p.lines.find(l => l.vendorPart === part);

console.log("num()");
t("plain, thousands, leading-dot", () => { eq(num("1,063.08"), 1063.08); eq(num(".9885"), 0.9885); eq(num("85.00"), 85); });
t("leading minus", () => eq(num("-23.50"), -23.5));
t("Wolff trailing minus is a credit, not NaN", () => eq(num("773.85-"), -773.85));
t("words are not numbers", () => { eq(num("C"), null); eq(num("EMT-1/2"), null); eq(num(""), null); });

console.log("CED invoice 0171-1063885");
const ced = parseCedPages(fx("ced-invoice"));
t("header", () => { eq(ced.invoiceNo, "0171-1063885"); eq(ced.invoiceDate, "08/28/2026"); eq(ced.po, "CLT 256"); eq(ced.isCredit, false); });
t("9 lines, sum = MERCHANDISE 998.20", () => { eq(ced.lines.length, 9); eq(ced.lineSum, 998.2); eq(ced.merchandise, 998.2); eq(ced.total, 1063.08); });
t("PER C: '85.00 C' on 1/2 EMT is $0.85 per FOOT", () => {
  const l = line(ced, "EMT-1/2"); eq(l.unitPrice, 0.85); eq(l.unit, "ft"); eq(l.mfr, "COND"); eq(l.extension, 340);
});
t("PER M: '175.00 M' stat wire is $0.175 per foot", () => { const l = line(ced, "18/2"); eq(l.unitPrice, 0.175); eq(l.unit, "ft"); });
t("PER E stays each", () => { const l = line(ced, "H30POUCH"); eq(l.unitPrice, 0.11); eq(l.unit, "ea"); });
t("description and mfr land on the right line", () => {
  const l = line(ced, "CLB75AS"); eq(l.mfr, "ORBIT"); eq(l.description, "3/4'' ALUMINUM LB+COVER+GASKET"); eq(l.unitPrice, 5.47);
});
t("verdict ok against the stored amount", () => eq(judgeTicket(ced, "1063.08").status, "ok"));

console.log("CED credit memo 0171-1064388");
const cm = parseCedPages(fx("ced-credit-memo"));
t("flagged as a credit; 'ORIGINAL INVOICE(S)' note is not a line", () => { eq(cm.isCredit, true); eq(cm.lines.length, 2); });
t("negative qty, POSITIVE unit price", () => { const l = line(cm, "EK4036S"); eq(l.qty, -12); eq(l.unitPrice, 23.5); eq(l.extension, -282); });
t("ok against a NEGATIVE stored amount", () => eq(judgeTicket(cm, "-486.71").status, "ok"));

console.log("Wolff 22006698-000 (wire)");
const ww = parseWolffPages(fx("wolff-wire"));
t("header", () => { eq(ww.invoiceNo, "22006698-000"); eq(ww.invoiceDate, "10/20/2026"); eq(ww.po, "CLT 256"); });
t("lines + tax = total (Wolff prints no subtotal)", () => { eq(ww.lineSum, 1329.37); eq(ww.tax, 86.39); eq(ww.total, 1415.76); eq(ww.sumOk, true); });
t("leading-dot price per foot", () => { const l = line(ww, "WATHHN2/0BRCT"); eq(l.unitPrice, 0.9885); eq(l.unit, "ft"); eq(l.qty, 60); });
t("cut charges kept for the sum but marked as charges", () => {
  eq(line(ww, "LABWATHHN2/0BR").isCharge, true); eq(line(ww, "WCTHHN4GRCT").isCharge, false);
});
t("coil notes and reel reminders are not lines", () => eq(ww.lines.some(l => /coil|reels/i.test(l.description)), false));
t("verdict ok", () => eq(judgeTicket(ww, "1415.76").status, "ok"));

console.log("Wolff 22000944-000 (mixed)");
const wm = parseWolffPages(fx("wolff-mixed"));
t("26 lines incl. a $608 disconnect", () => { eq(wm.lines.length, 26); eq(line(wm, "QOVH362RB").unitPrice, 608.23); });
t("verdict ok", () => eq(judgeTicket(wm, "2677.66").status, "ok"));

console.log("The guards");
t("CED reader on a Wolff PDF reads nothing → unreadable, never ok", () =>
  eq(judgeTicket(parseCedPages(fx("wolff-wire")), "1415.76").status, "unreadable"));
t("Wolff reader on a CED PDF is never ok", () =>
  eq(judgeTicket(parseWolffPages(fx("ced-invoice")), "1063.08").status === "ok", false));
t("PDF total ≠ stored amount → mismatch (wrong PDF on the row)", () => {
  const v = judgeTicket(ced, "999.99"); eq(v.status, "mismatch"); eq(/right PDF/.test(v.note), true);
});
t("a misread PER column fails its own line check", () => {
  // Simulate "85.00 C" read as each: the extension can no longer match.
  const broken = parseCedPages(fx("ced-invoice"));
  const l = line(broken, "EMT-1/2"); l.unitPrice = 85; l.check = Math.abs(400 * 85 - 340) <= 0.015;
  eq(judgeTicket(broken, "1063.08").status, "mismatch");
});
t("a dropped line breaks the sum → mismatch", () => {
  const short = parseCedPages(fx("ced-invoice"));
  short.lines.pop(); short.lineSum = Math.round(short.lines.reduce((s, l) => s + l.extension, 0) * 100) / 100;
  short.sumOk = Math.abs(short.lineSum - short.merchandise) <= 0.015;
  eq(judgeTicket(short, "1063.08").status, "mismatch");
});

console.log("Runner");
await (async () => {
  // One unsupported vendor, one PDF that fails to open, one good one.
  const writes = [];
  const q = async (text, params) => {
    if (/^\s*SELECT/.test(text)) return [
      { id: "11111111-1111-1111-1111-111111111111", vendor_name: "Lowe's", invoice_no: "L1", amount: "10", pdf_key: "a" },
      { id: "22222222-2222-2222-2222-222222222222", vendor_name: "CED", invoice_no: "C1", amount: "1", pdf_key: "bad" },
      { id: "33333333-3333-3333-3333-333333333333", vendor_name: "CED", invoice_no: "C2", amount: "1063.08", pdf_key: "good" },
    ];
    writes.push(params); return [];
  };
  // getBytes throws for "bad"; "good" returns bytes that are not a PDF, so it
  // lands 'unreadable' whether or not pdf.js is installed — either way the
  // runner must record it and move on.
  const getBytes = async key => { if (key === "bad") throw new Error("HTTP_404"); return new Uint8Array([1]); };
  let r;
  try { r = await readPendingTicketLines(q, getBytes, { limit: 3 }); } catch (e) { r = { error: e.message }; }
  t("never throws; every ticket gets a status written", () => { eq(r.error, undefined); eq(writes.length, 3); });
  t("unsupported vendor recorded as unsupported, not skipped forever", () => eq(writes[0][12], "unsupported"));
  t("a PDF that can't be fetched is 'unreadable' with the reason", () => {
    eq(writes[1][12], "unreadable"); eq(/HTTP_404/.test(writes[1][13]), true);
  });
  t("full batch reports more waiting", () => eq(r.moreWaiting, true));
  const r0 = await readPendingTicketLines(q, getBytes, { limit: 3, budgetMs: -1 });
  t("a spent time budget stops before reading and says so", () => {
    eq(r0.read, 0); eq(r0.stoppedForTime, true); eq(r0.moreWaiting, true);
  });
})();

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
