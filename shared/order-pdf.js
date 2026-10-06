/* Materials-order PDF — ONE copy, loaded by BOTH apps.
 *
 * The inventory app prints and emails it; the field app previews and emails it
 * from the order's list on the job (owner 2026-10-06). Two copies of a layout
 * drift, and a supplier would then get a different sheet depending on which app
 * sent it — so the layout lives here and nowhere else.
 *
 * ⚠ sw.js serves /shared/ NETWORK-FIRST. Every other asset is cache-first, which
 * would pin a phone to whatever version of this file it saw first.
 *
 * Plain ES5 on window, no module: inventory.html is ES5 throughout.
 * Needs jsPDF loaded (window.jspdf). Returns { doc, filename }; the caller saves,
 * shares or previews it.
 */
(function () {
  // Gray NEE logo — the same image inventory.html embeds as ESTIMATE_LOGO_GRAY_B64.
  var LOGO = "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";

  function fmtQty(n) {
    return Number(n).toLocaleString("en-US", { maximumFractionDigits: 2 });
  }

  function sortLines(a, b) {
    var catA = (a && a.category ? String(a.category).trim() : "");
    var catB = (b && b.category ? String(b.category).trim() : "");
    if (!catA && catB) return 1;
    if (catA && !catB) return -1;
    if (catA !== catB) {
      var c = catA.localeCompare(catB, undefined, { sensitivity: "base", numeric: true });
      if (c !== 0) return c;
    }
    var nameA = (a && a.itemName ? String(a.itemName) : "");
    var nameB = (b && b.itemName ? String(b.itemName) : "");
    return nameA.localeCompare(nameB, undefined, { sensitivity: "base", numeric: true });
  }

  window.neeBuildOrderPdf = function (order) {
    var jsPDF   = window.jspdf.jsPDF;
    var doc     = new jsPDF({ unit: "mm", format: "letter", orientation: "portrait" });
    var pW      = 215.9;
    var pH      = 279.4;
    var mg      = 15;
    var cW      = pW - mg * 2;
    var today   = new Date();
    var dateStr = today.toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric" });

    // Header — gray logo + company name
    try {
      doc.addImage("data:image/png;base64," + LOGO, "PNG", mg, 6, 22, 22);
    } catch(err) { console.warn("Logo embed failed:", err); }

    doc.setFont("helvetica", "bold");
    doc.setFontSize(14);
    doc.setTextColor(40, 40, 40);
    doc.text("NORTHEASTERN ELECTRIC, INC.", mg + 26, 14);

    doc.setFont("helvetica", "normal");
    doc.setFontSize(9);
    doc.setTextColor(80, 80, 80);
    doc.text("MATERIALS ORDER", mg + 26, 21);

    doc.setTextColor(120, 120, 120);
    doc.setFontSize(8);
    doc.text(dateStr, pW - mg, 21, { align: "right" });

    doc.setDrawColor(120, 120, 120);
    doc.setLineWidth(0.6);
    doc.line(mg, 32, pW - mg, 32);

    // Job info
    var y = 42;
    doc.setFont("helvetica", "bold");
    doc.setFontSize(13);
    doc.setTextColor(30, 30, 30);
    doc.text(order.jobName || "Order", mg, y);

    y += 6;
    doc.setFont("helvetica", "normal");
    doc.setFontSize(9);
    doc.setTextColor(120, 120, 120);
    var info = [];
    if (order.orderId) info.push("Order #" + order.orderId);
    if (order.createdBy) info.push("Prepared by " + order.createdBy);
    if (info.length) doc.text(info.join("  ·  "), mg, y);

    if (order.vendor) {
      y += 7;
      doc.setFont("helvetica", "bold");
      doc.setFontSize(10);
      doc.setTextColor(60, 60, 60);
      doc.text("Vendor / Notes:", mg, y);
      doc.setFont("helvetica", "normal");
      doc.setTextColor(40, 40, 40);
      doc.text(order.vendor, mg + 32, y);
    }

    // Table header
    y += 14;
    doc.setFillColor(240, 240, 240);
    doc.rect(mg, y - 5, cW, 8, "F");
    doc.setDrawColor(180, 180, 180);
    doc.setLineWidth(0.2);
    doc.rect(mg, y - 5, cW, 8, "S");

    doc.setFont("helvetica", "bold");
    doc.setFontSize(8);
    doc.setTextColor(80, 80, 80);

    var c1 = mg + 2;
    var c2 = pW - mg - 2;

    doc.text("ITEM", c1, y);
    doc.text("QTY",  c2, y, { align: "right" });

    // Rows
    y += 4;
    doc.setFont("helvetica", "normal");
    doc.setFontSize(10);

    order.lines.slice().sort(sortLines).forEach(function(l, idx) {
      var rowH = 8;
      y += rowH;

      if (y > pH - 35) {
        doc.addPage();
        y = mg + 10;
      }

      if (idx % 2 === 0) {
        doc.setFillColor(250, 250, 250);
        doc.rect(mg, y - rowH + 1, cW, rowH, "F");
      }
      doc.setDrawColor(225, 225, 225);
      doc.setLineWidth(0.1);
      doc.line(mg, y + 2, pW - mg, y + 2);

      doc.setTextColor(40, 40, 40);
      var name = l.itemName || "Item";
      var displayName = name.length > 60 ? name.substring(0, 58) + "..." : name;
      doc.text(displayName, c1, y);

      doc.setFont("helvetica", "bold");
      doc.setTextColor(30, 30, 30);
      var qtyText = l.isBox ? (fmtQty(l.qty) + " box") : fmtQty(l.qty);
      doc.text(qtyText, c2, y, { align: "right" });
      doc.setFont("helvetica", "normal");

      // Leader dots between item name and qty for paper readability.
      // Reset dash pattern after drawing — otherwise downstream lines (row
      // hairlines, footer divider) inherit the dashed style.
      var nameW  = doc.getTextWidth(displayName);
      var qtyW   = doc.getTextWidth(qtyText);
      var dotsX1 = c1 + nameW + 1.5;
      var dotsX2 = c2 - qtyW - 1.5;
      if (dotsX2 - dotsX1 > 2) {
        doc.setDrawColor(160, 160, 160);
        doc.setLineWidth(0.4);
        doc.setLineDashPattern([0.4, 1.2], 0);
        doc.line(dotsX1, y - 0.5, dotsX2, y - 0.5);
        doc.setLineDashPattern([], 0);
      }
    });

    // Footer with item count
    y += 14;
    if (y > pH - 25) { doc.addPage(); y = pH - 25; }
    doc.setDrawColor(150, 150, 150);
    doc.setLineWidth(0.3);
    doc.line(mg, y, pW - mg, y);
    y += 6;
    doc.setFont("helvetica", "bold");
    doc.setFontSize(11);
    doc.setTextColor(40, 40, 40);
    doc.text("Total items: " + order.lines.length, pW - mg, y, { align: "right" });

    // Page footer
    var footY = pH - 12;
    doc.setDrawColor(200, 200, 200);
    doc.setLineWidth(0.2);
    doc.line(mg, footY - 4, pW - mg, footY - 4);
    doc.setFont("helvetica", "normal");
    doc.setFontSize(8);
    doc.setTextColor(140, 140, 140);
    doc.text("Generated by NEE Inventory  ·  " + dateStr, mg, footY);
    doc.text("Northeastern Electric, Inc.", pW - mg, footY, { align: "right" });

    var safeJob  = (order.jobName || "Order").replace(/[^a-z0-9]/gi, "_").substring(0, 40);
    var datePart = today.toISOString().split("T")[0];
    var filename = "NEE_Order_" + safeJob + "_" + datePart + ".pdf";
    return { doc: doc, filename: filename };
  };
})();
