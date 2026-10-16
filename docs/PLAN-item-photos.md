# PLAN — Inventory item photos (+ maybe spec sheets)

**Status: PLAN ONLY — do not build until the owner says so.** Written 2026-10-16.

## Owner's decisions (2026-10-16)
- **Admin only** can add, replace or remove a photo. Everyone sees them.
- **No photos on estimate/order PDFs.**
- Spec sheets (PDF) — **undecided**, see the last section.

## Where the files live
- **Cloudflare R2, not Neon.** Neon holds only `items.photo_key`.
  - Bytes in Neon would pass through the function (≈4.5 MB payload ceiling, an invocation per
    image), bloat the database and its backups, and defeat the phone's cache.
  - R2 is already wired (`_r2.js`, presigned PUT/GET; `inventory.js` already imports it). The
    fleet/lift photo path (`presignEquipThumbPut`, `thumbKeyFor`) is the pattern to copy.
- Key: `items/<itemId>/photo-<ts>.jpg` + its `thumbKeyFor()` twin. One photo per item.

## Loading cost
- App start: unchanged apart from one short string per item.
- Lists show the thumbnail (~400 px, 20–30 KB) with `loading="lazy"`; the full image (~1600 px,
  200–300 KB) loads only on tap. A 40-item category ≈ 1 MB the first time, ~0 after.
- The browser resizes before upload (as fleet photos do), so a 5 MB phone photo lands ≈ 300 KB.
- ⚠ **Presigned GET URLs change every request, which defeats the browser cache.** Sign with a
  fixed time window (e.g. per calendar day) so a thumbnail's URL is stable for the day.
- Storage: ~1,000 photographed items ≈ 300 MB.

## Build steps
1. **Schema** — `items.photo_key text NULL`; include it in the item-list SELECT.
   ⚠ Grep every explicit item column list (see the "three column lists" lesson).
2. **`inventory.js`** — `itemPhotoUploadUrl` (admin), `setItemPhoto {itemId, key|null}` (admin),
   and the item list returns `photoThumbUrl`. The full URL is fetched on tap.
3. **`inventory.html`**
   - Thumbnails in the Use on Job category list and search results, the selected-item box, and
     the Check Stock browse. A 💡 placeholder when there is no photo.
   - Tap the thumbnail to open the full image.
   - The item edit/new-item form gets 📷 Add photo (camera or file), Replace and Remove — shown
     to admin only.
4. **Tests** — admin-only authz, the key must sit under `items/<itemId>/`, a null key clears it.

Nothing here touches stock, cost, the ledger or expenses.

## Spec sheets (PDF) — undecided
If wanted, they fit the same design:
- Multiple PDFs per item, under `items/<itemId>/_specs/`, listed from R2 (no column needed).
- Admin uploads; everyone opens or prints. Print through pdf.js page rendering, as on the
  Statements screen and job prints (Android won't print a PDF in an iframe).
- Soft delete, matching the job-prints `_deleted/` pattern.
- Possible later step: "add this item's spec sheet to the job's 📄 Specs" when the item is used on
  a job. It would be a copy into the job's `_prints/_specs/`, never a link.
