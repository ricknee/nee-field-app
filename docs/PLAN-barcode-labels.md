# PLAN — 🏷 Print barcode labels (inventory app)

**Status:** PLANNED, not built. Owner asked 2026-09-25: "click print barcode and be able to select
which barcodes and how many and be able to print em." Printer: **Brother QL-820NWB**
(Wi-Fi / Bluetooth / USB, DK rolls, 300 dpi).

## Facts measured before planning (production, 2026-09-25)

- `inventory_items`: **872 items, 860 with a barcode**, 12 without. `alternate_barcodes` is empty on
  every row.
- Barcode shapes: most are **11 digits** (`78618910494`), **203 are non-numeric**, 36 are 12-digit,
  longest is 21 characters.
- The scanner (`inventory.html`, `handleScanResult`) matches **by exact string**:
  `i.barcode.toString() === val.toString()`. `BarcodeDetector` already accepts `code_128`; the iOS
  ZXing fallback reads it too.
- jsPDF is already lazy-loaded from cdnjs in `inventory.html`.

## ⚠⚠ Print CODE 128, never UPC/EAN

An 11-digit value printed as UPC-A gets a **check digit appended**. The scanner then reads
`786189104943`, the exact-match lookup finds nothing, and the item looks like it doesn't exist. Nothing
throws. Code 128 encodes the stored string verbatim: digits, letters, any length. It is also the
only symbology that fits the 203 non-numeric values. Numeric strings pack into Code 128 set C
automatically, so the bars stay short.

**Test to add:** a scan of a printed label round-trips to the stored string.

## How printing works (the constraint that shapes everything)

A web page **cannot** reach the printer directly:
- Raw TCP 9100 over Wi-Fi: browsers can't open sockets.
- Web Bluetooth: the QL-820NWB is Bluetooth **Classic**, not BLE.
- b-PAC SDK: Windows desktop only.
- WebUSB: needs a driver swap, desktop Chrome only. Too fragile for the crew.

**So the app builds a PDF sized exactly to the label roll, one page per label, and hands it to the
OS print dialog:**
- **Android:** Brother **Print Service Plugin** (Play Store). The printer then appears in Chrome's
  print dialog over Wi-Fi.
- **Windows:** the Brother QL driver, paper size set to the roll.
- **Fallback:** Web Share the PDF to Brother **iPrint&Label**. Same mechanism as jobsite photo
  sharing (`project_photo_sharing`); never send `text` alongside `files`.

**The one real risk:** Android's print path scaling the page or adding margins. That is why step 1
is a spike, not a screen.

## What the user sees

- **☰ → 🏷 Print labels:** the batch screen.
  1. Search by name or barcode (reuse the existing `searchNorm` search); tick items.
  2. Per-item quantity stepper (− / +).
  3. Shortcuts: **by location** ("everything on Shelf B"), **by receiving / push**, **select all shown**.
  4. Preview of the first label + total ("14 labels") → **Print** / **Share**.
- **🏷 Print label** on each item's screen: qty, then print.
- **On the label:** item name (auto-shrunk; names run long, e.g.
  `LIGHT ALMOND CABLE PLATE (TPCATVLA)`), the Code 128 barcode, and the human-readable value under it.
- Remembered per device (`localStorage`, try/catch): label size, last quantities.

## Build order

1. **Spike:** one hard-coded label PDF, printed from the owner's Android phone **and** a PC, then
   scanned back with the app's own scanner. Proves size, margins, and round-trip. Stop here if
   Android scales it; fall back to the iPrint&Label share path.
2. **Single label** from an item's screen.
3. **Batch screen**: picker + quantities.
4. **Shortcuts**: by location, by push/receipt, plus an offer to print labels after receiving.
5. **Items with no barcode (12)**: "Generate barcode" creates an internal one (e.g. `NEE-00123`) and
   saves it. **The only write in the whole feature.** It needs a uniqueness check against both
   `barcode` and `alternate_barcodes`, or two items can share a code and the scanner picks
   whichever comes first.

Steps 1–4 are read-only: inventory-app code plus one barcode library (JsBarcode from cdnjs or
jsdelivr, rendered to canvas then into jsPDF). No schema change until step 5.

⛔ Per CLAUDE.md, `inventory.js` has **no Airtable client**. Nothing here needs one; don't add one.

## Open questions for the owner (answer before step 1)

1. **Which DK roll?** DK-1201 (29 × 90 mm die-cut address label, fits bins) or DK-2205 (62 mm
   continuous, cut to length, roomier for 21-character codes). The page size and layout come
   from this.
2. **Where do the labels go:** bins/shelves or the items themselves? Decides whether the location
   prints on the label.
3. **Phone, office PC, or both?** Decides which gets tested first in the spike.
