# PLAN — 🏷 Print QR labels for bins (inventory app)

**Status:** Step 1 (the spike) BUILT 2026-09-28, `091f0e2`: inventory home → 🏷 Label Print Test
(admin). Waiting on the owner's results from Android and iPhone. Steps 2–5 not built. Owner asked 2026-09-25: "click print barcode and be able to select
which barcodes and how many and be able to print em."
**Decided with the owner, 2026-09-28:**
- **Printer:** Brother **QL-820NWB** (Wi-Fi / Bluetooth / USB, 300 dpi).
- **Roll:** **DK-1208**, already loaded.
- **Labels go on bins.**
- **QR code + description only, no barcode lines.**
- **Phones:** **Android and iPhone** both.

## Facts measured before planning (production)

- `inventory_items`: **872 items, 860 with a barcode**, 12 without. `alternate_barcodes` is empty on
  every row. Values: mostly 11 digits (`78618910494`), 203 non-numeric, longest 21 characters.
- The scanner (`inventory.html`, `handleScanResult`) matches **by exact string**:
  `i.barcode.toString() === val.toString()`. `BarcodeDetector` already accepts `qr_code`; the iOS
  ZXing fallback reads QR too. The crew **already scans QR codes and it "works well"**.
- jsPDF is already lazy-loaded from cdnjs in `inventory.html`.
- Locations (`locations`) are the **shop and trucks** (Shop #1, #4 Transit, #6 Box Trailer…), not
  bins. So a location line on a bin label would say "Shop #1" and tell nobody which bin, and it's
  left off.

## What the QR holds: the item's `barcode`, verbatim

A QR code encodes any string exactly: digits, letters, leading zeros, 21 characters. There is no
check-digit trap (the thing that ruled out UPC: an 11-digit value printed as UPC gains a 12th digit
and stops matching). The scan lands in `handleScanResult` like any other.
⚠ Assumed to match the QR stickers already in use, since those scan to the right item through the
same exact-string match. **Confirm on one existing sticker during the spike**: scan it and compare
to the item's barcode.

## The label: DK-1208, 38 × 90.3 mm die-cut

Die-cut, so every label is a fixed size and **one PDF page = one label**. Page 90.3 × 38 mm,
landscape; keep content ~1.5–2 mm in from every edge (the QL series doesn't print to the edge of
a die-cut label). The spike confirms the real margin.

```
┌──────────────────────────────────────────────────────┐
│ ┌──────────┐  1-1/2" EMT SET SCREW                   │
│ │   QR     │  COUPLING                               │
│ │  ~30 mm  │                                         │
│ │  square  │                                         │
│ └──────────┘  78429720024                  (small)   │
└──────────────────────────────────────────────────────┘
```
- **QR:** ~30 mm square on the left, with its quiet zone. Big enough to scan a bin from arm's length.
- **Description:** the item name, large, up to 3 lines, auto-shrunk to fit. Names run long, e.g.
  `LIGHT ALMOND CABLE PLATE (TPCATVLA)`.
- **The value in small print** at the bottom, so a scuffed QR can still be typed in by hand.
- Library: a small QR generator (e.g. `qrcode` from jsdelivr, allowed by the CDN rule) → canvas →
  jsPDF image. Error-correction level **M or Q**, because bin labels get scuffed.

## How printing works (the constraint that shapes everything)

A web page **cannot** reach the printer directly:
- Raw TCP 9100 over Wi-Fi: browsers can't open sockets.
- Web Bluetooth: the QL-820NWB is Bluetooth **Classic**, not BLE.
- b-PAC SDK: Windows desktop only.

**So the app builds a label-sized PDF and hands it to the phone's print screen:**
- **Android:** install Brother **Print Service Plugin** (Play Store). The printer then appears in
  Chrome's print dialog over Wi-Fi.
- **iPhone:** AirPrint, if the QL-820NWB supports it. Believed so, **not verified**; the spike
  settles it. iOS picks the paper size itself, which is the likeliest thing to go wrong.
- **Both, fallback:** **📤 Share** the PDF to Brother **iPrint&Label**. Same Web Share mechanism as
  jobsite photo sharing (`project_photo_sharing`); never send `text` alongside `files`.

**One-time setup (owner):** printer on the shop Wi-Fi (printer Menu → WLAN, or iPrint&Label's setup);
phones on the same network; the plugin on Android phones.

## What the user sees

- **☰ → 🏷 Print labels:** the batch screen.
  1. Search by name or barcode (reuse the existing `searchNorm` search); tick items.
  2. A quantity stepper (− / +) for each item, default 1.
  3. Shortcuts: **select all shown**, **everything in a category** (e.g. all "EMT Fittings" for a
     bin rack), **everything from a receiving/push**.
  4. Preview of the first label + total ("14 labels") → **🖨 Print** / **📤 Share**.
- **🏷 Print label** on each item's screen: qty, then print.
- Remembered per device (`localStorage`, try/catch): last quantities.

## Build order

1. **Spike:** a hidden test button that prints one real item's label. Owner prints it from the
   **Android** phone and an **iPhone**, then scans it back with the app, and scans one **existing**
   QR sticker to confirm both hold the same kind of value. Proves size, margins, iOS behaviour,
   round-trip. If a phone scales or crops, that phone uses 📤 Share → iPrint&Label.
2. **Single label** from an item's screen.
3. **Batch screen**: picker + quantities.
4. **Shortcuts**: by category and by push/receipt, plus an offer to print labels after receiving.
5. **Items with no barcode (12)**: "Generate code" creates an internal one (e.g. `NEE-00123`) and
   saves it. **The only write in the whole feature.** Needs a uniqueness check against both
   `barcode` and `alternate_barcodes`, or two items share a code and the scanner picks whichever
   comes first.

Steps 1–4 are read-only: inventory-app code plus one QR library. No schema change until step 5.

⛔ Per CLAUDE.md, `inventory.js` has **no Airtable client**. Nothing here needs one; don't add one.

## Parked ideas (owner: "leave it for now")

Identifying an item from a **photo** via Claude vision: suggest the top 3, never auto-pick, because
118 PVC and 90 EMT fittings differ only by size. Filling a **new** item's form from a photo of its
box is the more reliable variant.
