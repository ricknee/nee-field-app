// Warn on an unpaid invoice when the estimate it billed has moved under it
// (owner 2026-11-20, after Sullivan Pullet: a Sent progress invoice billed an
// estimate that was then cut from two generators to one).
//
// The link: a contract invoice's snapshot stores one line per estimate, with
// line id "contract_<estimate handle>" and the dollars billed on that line.
// There is no FK column, so this reads the snapshot. A snapshot that won't parse
// is skipped, never thrown on: a warning must not break the invoice list.
//
// Three checks, per estimate billed on the invoice:
//   1. the estimate no longer exists (deleted);
//   2. it is Rejected;
//   3. everything billed against it on non-Voided invoices adds up to MORE
//      than its price today (it was cut, or billed past 100%).
// Warnings are attached only to invoices still open (not Paid, not Voided).
// The over-billed total still counts Paid invoices, because that's where the
// money already went.

export const CONTRACT_LINE_PREFIX = "contract_";

export function contractLines(snapshot) {
  let s = snapshot;
  if (typeof s === "string") {
    if (!/^\s*\{/.test(s)) return [];
    try { s = JSON.parse(s); } catch { return []; }
  }
  const lines = Array.isArray(s?.lines) ? s.lines : [];
  return lines
    .filter(l => typeof l?.id === "string" && l.id.startsWith(CONTRACT_LINE_PREFIX))
    .map(l => ({ handle: l.id.slice(CONTRACT_LINE_PREFIX.length), amount: Number(l.amount) || 0 }));
}

// invoices: [{ id, status, snapshot }]
// estimates: Map(handle -> { status, price, label })  (handle = airtable_id or uuid)
// returns Map(invoiceId -> [warning string])
export function invoiceEstimateWarnings(invoices, estimates) {
  const billed = new Map();          // handle -> $ billed on non-Voided invoices
  const linesByInv = new Map();
  for (const inv of invoices || []) {
    const lines = contractLines(inv.snapshot);
    linesByInv.set(inv.id, lines);
    if (String(inv.status) === "Voided") continue;
    for (const l of lines) billed.set(l.handle, (billed.get(l.handle) || 0) + l.amount);
  }
  const money = n => "$" + Number(n).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const out = new Map();
  for (const inv of invoices || []) {
    const st = String(inv.status || "");
    if (st === "Paid" || st === "Voided") continue;
    const warns = [];
    const seen = new Set();
    for (const l of linesByInv.get(inv.id) || []) {
      if (seen.has(l.handle)) continue;
      seen.add(l.handle);
      const e = estimates.get(l.handle);
      if (!e) { warns.push("The estimate this invoice billed has been deleted."); continue; }
      const name = e.label || "the estimate";
      if (e.status === "Rejected") warns.push(`${name} is now Rejected, but this invoice still bills it (${money(l.amount)}).`);
      const total = billed.get(l.handle) || 0;
      if (e.price != null && total - Number(e.price) > 0.005) {
        warns.push(`${money(total)} has been billed against ${name}, which is now priced at ${money(e.price)}: over by ${money(total - Number(e.price))}.`);
      }
    }
    if (warns.length) out.set(inv.id, warns);
  }
  return out;
}
