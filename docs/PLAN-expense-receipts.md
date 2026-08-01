# Plan: Receipt photos on expenses

**Status:** Planned, not started. Original design 2026-07-27; **rewritten 2026-08-01** now that
the jobsite-photo system has shipped on R2 and most of the machinery already exists.

**One-line:** Let whoever enters an expense attach a photo of the receipt, visible to admin and
office, stored in R2 next to the jobsite photos.

**The change since July:** the July plan was a from-scratch build — pick a store, stand up a
bucket, design an upload path, write a gallery. All of that now exists and is in production.
This is mostly **assembly**.

---

## 1. What it gets you

- **Receipts attached to the expense**, so approving one doesn't mean hunting through a glovebox,
  a truck console, or a text thread.
- **Captured at the moment of spend**, on the phone, by the person who spent it — not reconstructed
  at month-end.
- **Visible where the decision is made:** the admin/office Expenses view, next to the amount being
  approved.
- **A real audit trail** for tax and for any customer who disputes a billed cost.
- Removes the standing "where's the receipt for this?" loop between the field and the office.

Non-goals for v1: OCR, auto-filling the amount, matching against vendor invoices. Those are
plausible later and all depend on having the images first.

## 2. What already exists and gets reused

| Piece | Where | Reuse |
|---|---|---|
| R2 client (presign, list, move, soft delete, purge) | `netlify/functions/_r2.js` | **As-is** |
| Presigned upload flow | `handleJobPhotoUploadUrls` | Copy, change the key prefix |
| Client-side compression (2048px + thumb, HEIC→JPEG, EXIF orientation) | `compressImage` in `index.html` | **As-is** |
| Chunked upload with progress + per-item retry | `uploadJobPhotos` | Copy, simplify (1-3 files, not 100) |
| Lightbox / full-size viewer | `showLightbox` | **As-is** |
| Key-ownership validation | `assertKeyInJob` pattern | Copy as `assertKeyInExpense` |
| Bucket, CORS, credentials, backup | already live | **Nothing to do** |

The backup picks receipts up **for free** — `tools/backup-photos.ps1` copies the whole bucket, so
receipts land on F: and P: on the next run with no change.

## 3. The simplification worth taking

The July plan specified an `expense_receipts` child table (Airtable now, Neon later). **Don't
build it for v1.**

The photo system proved the folder *is* the record: key objects by the owning record id and list
by prefix. No table, no schema change, nothing to migrate, and — importantly — **nothing new to
port to Neon**, which matters given the migration is mid-flight and Airtable is being retired.

```
expenses/<expense record id>/<stamp>-<n>-<rand>.jpg
expenses/<expense record id>/<stamp>-<n>-<rand>_thumb.jpg
expenses/<expense record id>/_deleted/...
```

Add the table later only if captions, OCR text, or per-receipt metadata are actually wanted.

**The one catch:** an expense record must exist before its receipts can be keyed to it, so the
upload happens *after* save, not during. See §4.

## 3a. Two capture routes — phone photo AND scanner PDF

Added 2026-08-01. Receipts arrive two ways and they are **not** the same kind of file:

| Route | Produces | Handling |
|---|---|---|
| Phone camera / camera roll | JPEG | compress to 2048px q0.75 + thumbnail, as photos do |
| **ScanSnap** (desktop or mobile) | **PDF**, often multi-page | **upload untouched** — no compression, no thumbnail |

Consequences worth stating up front, because they are easy to get wrong:

- **Never compress a PDF.** The canvas resize path is image-only. Running a scan through it would
  either fail or silently rasterise a crisp 300dpi scan into a worse JPEG. Branch on file type at
  the picker, not deeper in.
- **PDFs get no thumbnail.** Generating one needs pdf.js, which is a heavy dependency for a tile.
  Show a document icon instead. The existing gallery already tolerates a missing thumbnail (falls
  back to the full image), but a PDF is not an image at all, so it needs its own tile treatment
  rather than that fallback.
- **A multi-page scan is ONE receipt**, not N. Page count is irrelevant to the data model — the
  file is the attachment.
- **Size is a non-issue.** Uploads are presigned PUTs straight to R2, so the 4.5 MB Netlify
  payload ceiling never applies. A 10 MB scan is fine.
- **The file picker accepts both:** `accept="image/*,application/pdf"`, no `capture` attribute
  (which would force the camera and rule out picking a scan — the same trap that broke
  multi-select on the photo upload).
- **Desktop matters here.** ScanSnap Desktop drops the PDF on a PC, so the attach flow has to work
  from the office browser, not just the phone. It does, but it means the UI can't assume a camera.

The stored object keeps its real content type so the viewer can branch: images open in the
existing lightbox, PDFs open in the browser's own PDF viewer (already proven by the Documents
strip in §11).

## 4. Flow

```
Add expense  ->  save (existing handleAddGeneralExpense / handleAddLiftExpense)
                   -> returns the new expense record id
                        -> "Add receipt" appears
                             -> camera or camera roll
                                  -> compress (existing compressImage)
                                       -> POST expenseReceiptUploadUrls -> presigned PUTs
                                            -> phone PUTs straight to R2

View expense -> GET expenseReceipts -> presigned GET urls -> thumbnail row -> tap for full size
```

Saving first is deliberate: it means an abandoned upload can never leave orphaned objects under an
expense that was never created, and the existing save path stays untouched.

## 5. Slices

**Slice 1 — attach and view (~4-5 h; was 3-4 before PDFs)**
- `_r2.js`: `expensePrefix(expenseId)`, reuse everything else
- `airtable.js`: `expenseReceiptUploadUrls` (POST, accepts image **or** `application/pdf`),
  `expenseReceipts` (GET, returns `contentType` so the client can branch)
- `index.html`: an "Add receipt" button on the expense row + an attachment strip
  - images → `compressImage` → thumbnail tile → existing lightbox
  - PDFs → upload untouched → document tile → open in a new tab
- Tests mirroring the photo ones: validation, authorization, key-ownership, **and that a PDF is
  never routed through the image compressor**

**Slice 2 — the approval view (~1-2 h)**
- Receipt thumbnails in the admin/office Expenses list, next to the amount
- Tap to enlarge before approving

**Slice 3 — delete + polish (~1-2 h)**
- Soft delete reusing the recycle-bin helpers
- Multiple receipts per expense (a fuel stop plus a parts counter slip)
- "No receipt" indicator so gaps are visible at a glance

## 6. Authorization

Follows the existing expense rules rather than inventing new ones:

| Action | Who |
|---|---|
| Attach a receipt | whoever may edit that expense — `guardExpenseMutation` already encodes this (owner while unreviewed, admin/office always) |
| View receipts | same scoping as `handleExpenses`: admin/office see all, an employee sees only their own |
| Delete a receipt | admin/office |

`handleExpenses` (airtable.js:2829) already enforces per-employee scoping server-side, so receipts
inherit a boundary that's been in production for weeks.

## 7. Touch points

- `handleAddGeneralExpense` (:2950), `handleAddLiftExpense` (:2937) — return the new record id if
  they don't already
- `guardExpenseMutation` (:2139) — reuse verbatim for attach/delete
- `handleExpenses` (:2829) — attach receipt urls to each expense in the response
- `saveGeneralExpense` (index.html:8657) — hand off to the receipt uploader after a successful save
- The 🧾 My Expenses tab and the admin Expenses view — thumbnail strips

## 8. Risks

| Risk | Mitigation |
|---|---|
| Receipts are small text on crumpled paper; 2048px q0.75 may not stay legible | Spike first: photograph one real receipt at the current settings and read it back on a phone. Bump to 2560px / q0.85 for receipts if needed — they are rarer than jobsite photos, so the size costs little. |
| Upload after save means a failed upload leaves an expense with no receipt | Show it plainly in the list ("no receipt") rather than silently; retry is just tapping Add receipt again |
| Storage growth | Negligible — receipts are far rarer than jobsite photos, and 10 GB is ~25,000 images |
| Airtable retirement | Nothing new is stored in Airtable, so this adds **zero** new migration work |

## 9. Decisions (settled 2026-08-01)

**A receipt is never required to save an expense.** Cash spends, lost slips and vendors who
don't hand one over are normal. A hard requirement would just be satisfied with a blurry photo
of a countertop, which is worse than an honest blank. Instead show a **"no receipt" marker** on
those expenses in the admin list — visibility, not a gate, so the few worth chasing are obvious.

**Receipts are exempt from auto-purge.** A jobsite photo deleted by mistake is a nuisance; a
receipt is a financial record that may be wanted years later for tax or a disputed customer
charge. They are rare and small, so keeping them costs effectively nothing. When the R2 lifecycle
rule for the photo recycle bin is added, it must **exclude `expenses/`**.

> Note: the backup already covers this independently. `tools/backup-photos.ps1` uses `rclone
> copy`, never `sync`, so anything ever written to R2 stays on F: and P: permanently — even after
> it is purged from Cloudflare. That was chosen to survive accidental deletion and happens to
> satisfy record retention too.

## 10. Scope — settled 2026-08-01

Three ways money gets logged, and they split into two different jobs:

| Source | What a "receipt" means | Work |
|---|---|---|
| General expenses (field app) | photograph the paper slip | Slices 1-3 above |
| Lift / rental expenses (field app) | photograph the paper slip | same |
| **Material push (inventory app)** | **archive the PDF the app already generates** | §11 |

## 11. Archiving the materials PDF (inventory app)

The inventory app **already builds a PDF** on every push — `generateReceiptPDF()` (inventory.html
:2451) returns base64, and `downloadReceiptPDF()` (:2425) turns it into a blob and triggers a
browser download named `NEE_Materials_<job>_<date>.pdf`.

**It is never stored.** It goes to whatever machine did the push, into that browser's Downloads
folder, and that is the only copy. Close the tab and there is no record beyond the Airtable
expense rows themselves.

So this is not a camera feature — the file already exists in memory at push time. It is the same
presigned upload as a photo, without the camera or the compression:

```
downloadReceiptPDF(g)          <- keep, people still want the local copy
  └─ also: POST jobDocUploadUrl -> presigned PUT -> R2
```

**Key layout.** PDFs must not land in the photo gallery — `listJobPhotos` returns every non-thumb
object under the job prefix, so a PDF would render as a broken image tile. Give them their own
segment, excluded from the gallery the same way `_deleted/` is:

```
jobs/<job record id>/_docs/NEE_Materials_<date>-<push id>.pdf
```

The push already mints a **Push ID** for idempotency (see the expense-push work), which makes a
natural unique filename and means a retried push can't produce a second copy.

**Viewing.** A "Documents" row in the job's Photos view — filename, date, tap to open. PDFs open
in the browser's viewer; no gallery work needed.

**Effort:** ~2 h, and it is independent of slices 1-3 — it could ship first, since it needs no
camera work and no decisions about compression.

**Worth noting:** this closes a real gap. Right now the materials list backing a job's costs
exists only as Airtable rows plus a PDF on one person's laptop. Archiving it puts the document
that justifies the numbers next to the job, and the nightly backup then copies it to F: and P:
with no extra work.
