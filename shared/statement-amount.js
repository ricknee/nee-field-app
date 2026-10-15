// Read a supplier statement's amount out of its PDF text (db/schema/084).
//
// Input: the PDF's text, already grouped into lines (top to bottom). The field
// app builds those lines from pdf.js getTextContent(). Output:
// { amount, evidence } or null.
//
// ⛔ Display only. The number goes on the Statements screen and nowhere else.
//
// ⚠ IT RETURNS null RATHER THAN GUESS. A statement it can't read stays blank,
// and the vendor's total says how many are blank. A wrong number would be summed
// without anyone noticing. The line it used is returned as `evidence` and shown,
// so a wrong pick can be seen and corrected by hand.
//
// Labels are tried in order. The first label that has an amount on its own line,
// or failing that on the next line, wins. "Previous balance", "past due" and the
// aging buckets are never labels, because they are not this month's statement
// amount.
(function (root) {
  var MONEY = /\(?-?\$?\s?\d{1,3}(?:,\d{3})*\.\d{2}\)?-?|\(?-?\$?\s?\d+\.\d{2}\)?-?/g;

  var LABELS = [
    // Wolff: "Total Balance Due: 40,346.28" at the foot of the last page. It
    // also prints a "Balance Due <date>" subtotal per due date ABOVE that —
    // those are pieces of the total, and the first one is often a small credit.
    // Owner confirmed 2026-10-15 against the Apr 2 statement.
    /total\s+balance\s+due/i,
    // CED: "TOTAL DUE" heads the first column of the aging footer.
    /total\s+amount\s+due/i,
    /amount\s+due/i,
    /total\s+due/i,
    /balance\s+due/i,
    /please\s+pay(\s+this\s+amount)?/i,
    /pay\s+this\s+amount/i,
    /statement\s+total/i,
    /total\s+balance/i,
    /new\s+balance/i,
    /account\s+balance/i,
    /^\s*balance\b/i,
    /^\s*total\b/i,
  ];
  // A line carrying one of these is never the answer, even if it also says "total".
  var NOT = /previous|prior|past\s+due|credit\s+limit|last\s+payment|payment\s+received/i;

  function parseMoney(s) {
    var t = String(s).trim(), neg = false;
    if (/^\(.*\)$/.test(t)) { neg = true; t = t.slice(1, -1); }
    if (/-$/.test(t)) { neg = true; t = t.slice(0, -1); }
    if (/^-/.test(t)) { neg = true; t = t.slice(1); }
    t = t.replace(/[$,\s]/g, "");
    if (!/^\d*\.\d{2}$/.test(t)) return null;
    var n = Number(t);
    return isFinite(n) ? (neg ? -n : n) : null;
  }

  function moneyIn(line) {
    var m = String(line).match(MONEY) || [];
    var out = [];
    for (var i = 0; i < m.length; i++) {
      var n = parseMoney(m[i]);
      if (n !== null) out.push(n);
    }
    return out;
  }

  function readStatementAmount(lines) {
    if (!lines || !lines.length) return null;
    var L = lines.map(function (s) { return String(s || "").replace(/\s+/g, " ").trim(); });
    for (var k = 0; k < LABELS.length; k++) {
      for (var i = 0; i < L.length; i++) {
        var line = L[i];
        if (!LABELS[k].test(line)) continue;
        var idx = line.search(LABELS[k]);
        var labelText = line.slice(idx).match(LABELS[k])[0];
        // ⚠ The skip-list is checked against the LABEL and the words just before
        // it ("Previous Balance"), NOT the whole line. CED's footer is a row of
        // column headings — TOTAL DUE | CURRENT / 1 - 30 | PAST DUE … — and a
        // whole-line check threw the real answer away because "PAST DUE" sits
        // two columns over.
        if (NOT.test(line.slice(Math.max(0, idx - 20), idx) + labelText)) continue;
        // The amount after the label on the same line. Text before the label
        // (an account number, a date) is not it.
        var after = moneyIn(line.slice(idx));
        if (after.length) return { amount: after[after.length - 1], evidence: line };
        // A header with its figures underneath. One amount below: that's it.
        // Several (an aging row): take the column the label heads — the LAST
        // figure when the label is the last heading (Current | 30 | 60 | Total
        // Due), the FIRST when it is the first (CED: TOTAL DUE | CURRENT / 1 -
        // 30 | PAST DUE …). A label in the middle is ambiguous and reads nothing.
        var next = L[i + 1];
        if (next) {
          var nx = moneyIn(next);
          var labelIsLast  = !/[A-Za-z]/.test(line.slice(idx).replace(LABELS[k], ""));
          var labelIsFirst = !/[A-Za-z]/.test(line.slice(0, idx));
          if (nx.length === 1 || (nx.length > 1 && labelIsLast)) {
            return { amount: nx[nx.length - 1], evidence: line + " / " + next };
          }
          if (nx.length > 1 && labelIsFirst) {
            return { amount: nx[0], evidence: line + " / " + next };
          }
        }
      }
    }
    return null;
  }

  // pdf.js text items to lines: group by baseline (y), order left to right.
  function linesFromTextItems(items) {
    var rows = [];
    (items || []).forEach(function (it) {
      if (!it || typeof it.str !== "string" || !it.transform) return;
      var y = it.transform[5], x = it.transform[4];
      var row = null;
      for (var i = 0; i < rows.length; i++) if (Math.abs(rows[i].y - y) < 2.5) { row = rows[i]; break; }
      if (!row) { row = { y: y, parts: [] }; rows.push(row); }
      row.parts.push({ x: x, s: it.str });
    });
    rows.sort(function (a, b) { return b.y - a.y; });   // PDF y grows upward
    return rows.map(function (r) {
      return r.parts.sort(function (a, b) { return a.x - b.x; }).map(function (p) { return p.s; }).join(" ");
    });
  }

  root.neeReadStatementAmount = readStatementAmount;
  root.neeStatementLines = linesFromTextItems;
})(typeof window !== "undefined" ? window : globalThis);
