// Offline tests for netlify/functions/_order-checklists.js — the pure half:
// how an order line becomes a list line, how lines are keyed, and the diff an
// order edit applies to its list. The SQL half was PREPARE-checked against Neon.
//
// Run:  & "C:\Users\irick\nodejs\node.exe" tests/order-checklists.test.mjs

import { fmtOrderQty, orderChecklistName, orderLinesToMap, diffOrderLines }
  from "../netlify/functions/_order-checklists.js";

let pass = 0, fail = 0;
function test(name, fn) {
  try { fn(); console.log(" ✓ " + name); pass++; }
  catch (e) { console.log(" ✗ " + name + "\n     " + e.message); fail++; }
}
function eq(actual, expected, msg) {
  const a = JSON.stringify(actual), b = JSON.stringify(expected);
  if (a !== b) throw new Error(`${msg}: expected ${b}, got ${a}`);
}

const EMT  = "17e11111-1111-4111-8111-111111111111";
const PVC  = "17e11111-2222-4222-8222-222222222222";
const line = (o) => ({ item_id: null, item_airtable_id: null, description: null,
                       quantity_ordered: "1.0000", item_name: null, ...o });

test("numeric padding from Postgres does not reach the list", () => {
  eq(fmtOrderQty("500.0000"), "500", "whole");
  eq(fmtOrderQty("2.5000"), "2.5", "half");
  eq(fmtOrderQty("0.3333"), "0.3333", "four places kept");
  eq(fmtOrderQty(null), "0", "null");
});

test("a catalog line reads 'qty — item name', like the printed order", () => {
  const m = orderLinesToMap([line({ item_id: EMT, description: '1/2" EMT PIPE', quantity_ordered: "500.0000", item_name: '1/2" EMT PIPE' })]);
  eq([...m.entries()], [[`item:${EMT}`, '500 — 1/2" EMT PIPE']], "body + key");
});

test("the [BOX] marker becomes BOX in the text and never leaks as a marker", () => {
  const m = orderLinesToMap([
    line({ item_id: EMT, description: "EMT CONNECTOR [BOX]", quantity_ordered: "2", item_name: "EMT CONNECTOR" }),
    line({ description: "Wire nuts [BOX]", quantity_ordered: "3" }),
  ]);
  eq([...m.values()], ["2 BOX — EMT CONNECTOR", "3 BOX — Wire nuts"], "bodies");
  eq([...m.keys()][1], "misc:wire nuts", "misc key has no marker");
});

test("the item's current name wins over the description stored at order time", () => {
  const m = orderLinesToMap([line({ item_id: PVC, description: "old name", item_name: '2" PVC (SCH 40)' })]);
  eq([...m.values()], ['1 — 2" PVC (SCH 40)'], "body");
});

test("repeated Misc lines get distinct keys, in order", () => {
  const m = orderLinesToMap([
    line({ description: "Anchors", quantity_ordered: "10" }),
    line({ description: " anchors ", quantity_ordered: "20" }),
  ]);
  eq([...m.keys()], ["misc:anchors", "misc:anchors#2"], "keys");
});

test("an unresolved item falls back to its handle, not to a misc key", () => {
  const m = orderLinesToMap([line({ item_airtable_id: "recItemA", description: "PART" })]);
  eq([...m.keys()], ["item:recItemA"], "key");
});

test("a qty change on the order is a CHANGE, not a remove + add", () => {
  // The whole point of keying on the item: add+remove would drop the crew's
  // position and any edit, and re-append the line at the bottom.
  const before = orderLinesToMap([line({ item_id: EMT, quantity_ordered: "200", item_name: "EMT" })]);
  const after  = orderLinesToMap([line({ item_id: EMT, quantity_ordered: "300", item_name: "EMT" })]);
  eq(diffOrderLines(before, after), { added: [], changed: [{ key: `item:${EMT}`, body: "300 — EMT" }], removed: [] }, "diff");
});

test("flipping each -> box is a change to the same line", () => {
  const before = orderLinesToMap([line({ item_id: EMT, description: "C", item_name: "C", quantity_ordered: "2" })]);
  const after  = orderLinesToMap([line({ item_id: EMT, description: "C [BOX]", item_name: "C", quantity_ordered: "2" })]);
  const d = diffOrderLines(before, after);
  eq([d.added.length, d.changed.length, d.removed.length], [0, 1, 0], "counts");
});

test("added lines keep order-line order; unchanged lines are left alone", () => {
  const before = orderLinesToMap([line({ item_id: EMT, item_name: "EMT", quantity_ordered: "5" })]);
  const after  = orderLinesToMap([
    line({ item_id: EMT, item_name: "EMT", quantity_ordered: "5" }),
    line({ item_id: PVC, item_name: "PVC", quantity_ordered: "7" }),
    line({ description: "Strut", quantity_ordered: "9" }),
  ]);
  eq(diffOrderLines(before, after),
     { added: [{ key: `item:${PVC}`, body: "7 — PVC" }, { key: "misc:strut", body: "9 — Strut" }], changed: [], removed: [] },
     "diff");
});

test("a line dropped from the order is reported removed", () => {
  const before = orderLinesToMap([line({ item_id: EMT, item_name: "EMT" }), line({ item_id: PVC, item_name: "PVC" })]);
  const after  = orderLinesToMap([line({ item_id: PVC, item_name: "PVC" })]);
  eq(diffOrderLines(before, after).removed, [`item:${EMT}`], "removed");
});

test("a new order diffs from empty: every line is added", () => {
  const after = orderLinesToMap([line({ item_id: EMT, item_name: "EMT" }), line({ item_id: PVC, item_name: "PVC" })]);
  eq(diffOrderLines(new Map(), after).added.length, 2, "added");
});

test("the list is named after the order and its vendor notes", () => {
  eq(orderChecklistName(47, "Wolff Deliver to shop."), "Order #47 — Wolff Deliver to shop.", "with notes");
  eq(orderChecklistName(48, "  "), "Order #48", "blank notes");
  eq(orderChecklistName(49, "x".repeat(300)).length, 120, "fits the 120-char list name");
});

console.log("----------------------------------------------");
console.log(`${pass} passed, ${fail} failed`);
if (fail) process.exit(1);
