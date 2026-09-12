// netlify/functions/_order-checklists.js
// An inventory ORDER mirrored as a LIST on its job (db/schema/074).
//
// Saving an order creates "Order #47 — <vendor notes>" on the job, one line per
// order line: "500 — 1/2" EMT PIPE". Editing the order re-syncs the list.
// Called from inventory.js (create / update / delete) and from
// tools/backfill-order-checklists.mjs, which is why it lives in its own module:
// the backfill and the live path MUST key lines identically, or the first edit
// after a backfill would duplicate every line.
//
// ⚠ THE SYNC IS A DIFF OF THE ORDER, NOT OF THE LIST. It compares the order's
// lines before an edit to its lines after, and applies only that difference to
// the list. So a line the crew deleted from the list is not resurrected by an
// unrelated qty change elsewhere on the order, and a line whose text the crew
// edited keeps that edit until the ORDER changes that line.
//
// ⚠ A TICKED LINE IS NEVER TOUCHED. It records what arrived. Rewriting its qty
// under the tick would claim the new qty arrived; deleting it would lose the
// record that something did.
//
// Pure functions first (tests/order-checklists.test.mjs), then the SQL.

import { neonWrite } from "./_neon.js";

// Mirrors handleOrderGet: " [BOX]" on the description is a marker meaning the
// line was ordered by the box, not text.
const BOX_MARKER = " [BOX]";

// numeric(14,4) arrives as the string "500.0000". Number() drops the padding;
// the rounding keeps a float artefact out of a line a person reads.
export function fmtOrderQty(q) {
  const n = Number(q);
  if (!Number.isFinite(n)) return "0";
  return String(Math.round(n * 10000) / 10000);
}

export function orderChecklistName(orderNumber, vendorNotes) {
  const v = String(vendorNotes || "").trim();
  return (`Order #${orderNumber}` + (v ? ` — ${v}` : "")).slice(0, 120);
}

// rows: { item_id, item_airtable_id, description, quantity_ordered, item_name },
// in line order. Returns an insertion-ordered Map of key -> list-line body.
export function orderLinesToMap(rows) {
  const map = new Map();
  const seen = Object.create(null);
  for (const r of rows || []) {
    let desc = String(r.description || "");
    let isBox = false;
    if (desc.endsWith(BOX_MARKER)) { isBox = true; desc = desc.slice(0, -BOX_MARKER.length); }
    const name = String(r.item_name || desc || "Item").trim();

    // item_id is resolved from the handle at insert, so it is the same uuid
    // before and after an edit. The handle is the fallback for a line whose
    // item did not resolve.
    const base = r.item_id          ? `item:${r.item_id}`
               : r.item_airtable_id ? `item:${r.item_airtable_id}`
               : `misc:${desc.trim().toLowerCase()}`;
    seen[base] = (seen[base] || 0) + 1;
    const key = seen[base] === 1 ? base : `${base}#${seen[base]}`;

    map.set(key, `${fmtOrderQty(r.quantity_ordered)}${isBox ? " BOX" : ""} — ${name}`.slice(0, 300));
  }
  return map;
}

export function diffOrderLines(before, after) {
  const added = [], changed = [], removed = [];
  for (const [key, body] of after) {
    if (!before.has(key))             added.push({ key, body });
    else if (before.get(key) !== body) changed.push({ key, body });
  }
  for (const key of before.keys()) if (!after.has(key)) removed.push(key);
  return { added, changed, removed };
}

// ── SQL ──────────────────────────────────────────────────────────────────────

export async function readOrderLineMap(orderId) {
  const rows = await neonWrite("orderChecklist.lines",
    `SELECT l.item_id, l.item_airtable_id, l.description, l.quantity_ordered, i.name AS item_name
       FROM material_order_lines l
       LEFT JOIN inventory_items i ON i.id = l.item_id
      WHERE l.order_id = $1::uuid
      ORDER BY l.line_number ASC NULLS LAST, l.id`,
    [String(orderId)]);
  return orderLinesToMap(rows);
}

// `before` = the order's line map from BEFORE this change (readOrderLineMap), an
// empty Map for a brand-new order, or null when only the header changed.
// Never throws for "nothing to do" — returns { skipped } — but DOES throw on a
// database error; inventory.js decides what a failure means for the request.
export async function syncOrderChecklist(orderId, before, actor) {
  const head = (await neonWrite("orderChecklist.head",
    `SELECT o.order_number, o.vendor_notes, o.job_id, j.airtable_id AS job_rec,
            c.id AS list_id, c.name AS list_name
       FROM material_orders o
       LEFT JOIN jobs j           ON j.id = o.job_id
       LEFT JOIN job_checklists c ON c.material_order_id = o.id
      WHERE o.id = $1::uuid`,
    [String(orderId)]))[0];
  if (!head)        return { skipped: "order-not-found" };
  if (!head.job_id) return { skipped: "no-job" };   // restock order, or an unmatched old one

  const after = await readOrderLineMap(orderId);
  const name  = orderChecklistName(head.order_number, head.vendor_notes);
  let listId  = head.list_id;
  let base    = before;

  if (!listId) {
    if (!after.size) return { skipped: "no-lines" };
    const made = await neonWrite("orderChecklist.create",
      `INSERT INTO job_checklists (job_airtable_id, job_id, name, created_by, material_order_id)
       VALUES ($1, $2::uuid, $3, $4, $5::uuid)
       ON CONFLICT (material_order_id) WHERE material_order_id IS NOT NULL DO NOTHING
       RETURNING id`,
      [head.job_rec || null, String(head.job_id), name, actor || null, String(orderId)]);
    listId = made[0]?.id;
    if (!listId) {
      // Lost a race with a concurrent save of the same order — use its list.
      const again = await neonWrite("orderChecklist.find",
        `SELECT id FROM job_checklists WHERE material_order_id = $1::uuid`, [String(orderId)]);
      listId = again[0]?.id;
      if (!listId) throw new Error("orderChecklist: list neither created nor found");
    }
    // A list that did not exist gets every line, whatever `before` said.
    base = new Map();
  } else if (head.list_name !== name) {
    await neonWrite("orderChecklist.rename",
      `UPDATE job_checklists SET name = $2, updated_at = now() WHERE id = $1::uuid`,
      [String(listId), name]);
  }

  if (!base) return { listId, added: 0, changed: 0, removed: 0 };

  const d = diffOrderLines(base, after);

  if (d.removed.length) {
    await neonWrite("orderChecklist.remove",
      `DELETE FROM checklist_items
        WHERE checklist_id = $1::uuid AND order_line_key = ANY($2::text[]) AND NOT done`,
      [String(listId), d.removed]);
  }
  if (d.changed.length) {
    await neonWrite("orderChecklist.change",
      `UPDATE checklist_items c
          SET body = t.body
         FROM unnest($2::text[], $3::text[]) AS t(key, body)
        WHERE c.checklist_id = $1::uuid AND c.order_line_key = t.key AND NOT c.done`,
      [String(listId), d.changed.map(x => x.key), d.changed.map(x => x.body)]);
  }
  if (d.added.length) {
    // NOT EXISTS makes a retried or raced sync harmless: a key already on the
    // list is not added twice.
    await neonWrite("orderChecklist.add",
      `INSERT INTO checklist_items (checklist_id, body, position, created_by, order_line_key)
       SELECT $1::uuid, t.body,
              (SELECT COALESCE(MAX(position), 0) FROM checklist_items WHERE checklist_id = $1::uuid) + t.ord,
              $4, t.key
         FROM unnest($2::text[], $3::text[]) WITH ORDINALITY AS t(key, body, ord)
        WHERE NOT EXISTS (SELECT 1 FROM checklist_items x
                           WHERE x.checklist_id = $1::uuid AND x.order_line_key = t.key)`,
      [String(listId), d.added.map(x => x.key), d.added.map(x => x.body), actor || null]);
  }
  if (d.removed.length || d.changed.length || d.added.length) {
    await neonWrite("orderChecklist.touch",
      `UPDATE job_checklists SET updated_at = now() WHERE id = $1::uuid`, [String(listId)]);
  }
  return { listId, added: d.added.length, changed: d.changed.length, removed: d.removed.length };
}

// ── COMPLETE: the two places move together ───────────────────────────────────
// Owner's ask (2026-09-12): an order picked up or delivered is marked complete
// ONCE, from either app, and both sides follow — the order goes to Complete and
// every line on its list is ticked.
//
// Deliberately ONE-WAY. Reactivating an order does NOT untick its list: those
// ticks may be real (half the order did arrive), and wiping them would destroy
// the record of what came. A wrong tick is one tap to undo on the list.

// Ticks every open line on an order's list. Returns how many it ticked (0 when
// the order has no list — a restock order, or one older than 074 unmatched).
export async function tickOrderChecklist(orderId, actor) {
  const rows = await neonWrite("orderChecklist.tickAll",
    `UPDATE checklist_items i
        SET done = true, done_at = now(), done_by = $2
       FROM job_checklists c
      WHERE c.material_order_id = $1::uuid
        AND i.checklist_id = c.id
        AND NOT i.done
      RETURNING i.checklist_id`,
    [String(orderId), actor || null]);
  if (rows.length) {
    await neonWrite("orderChecklist.tickTouch",
      `UPDATE job_checklists SET updated_at = now() WHERE material_order_id = $1::uuid`, [String(orderId)]);
  }
  return rows.length;
}

// From the LIST side: mark the list's order Complete and tick every line.
// Returns null when the list is not an order list.
export async function completeOrderFromChecklist(listId, actor) {
  const rows = await neonWrite("orderChecklist.completeOrder",
    `UPDATE material_orders o
        SET status = 'Complete', synced_at = now()
       FROM job_checklists c
      WHERE c.id = $1::uuid AND o.id = c.material_order_id
      RETURNING o.id, o.order_number`,
    [String(listId)]);
  const order = rows[0];
  if (!order) return null;
  const ticked = await tickOrderChecklist(order.id, actor);
  return { orderId: order.id, orderNumber: Number(order.order_number), ticked };
}

// Deleting an order takes its list with it — unless someone has ticked a line,
// in which case the list is a delivery record and stays (the FK nulls out).
export async function dropOrderChecklistIfUntouched(orderId) {
  const gone = await neonWrite("orderChecklist.drop",
    `DELETE FROM job_checklists c
      WHERE c.material_order_id = $1::uuid
        AND NOT EXISTS (SELECT 1 FROM checklist_items i WHERE i.checklist_id = c.id AND i.done)
      RETURNING c.id`,
    [String(orderId)]);
  return { dropped: gone.length };
}
