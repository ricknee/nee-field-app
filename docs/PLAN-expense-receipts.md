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

**Slice 1 — attach and view (~3-4 h)**
- `_r2.js`: `expensePrefix(expenseId)`, reuse everything else
- `airtable.js`: `expenseReceiptUploadUrls` (POST), `expenseReceipts` (GET)
- `index.html`: an "Add receipt" button on the expense row + a thumbnail strip, reusing
  `compressImage` and the existing lightbox
- Tests mirroring the photo ones: validation, authorization, key-ownership

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

## 10. Still open

1. **Scope: which expense types?** The field app has general expenses and lift expenses; the
   inventory app pushes material expenses across from the other base. Receipts obviously fit the
   first two — unclear whether the inventory push path needs them.
