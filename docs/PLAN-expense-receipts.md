# Plan: Receipt photos on expenses

**Status:** Planned, not started — **deferred** (bigger build than we want to tackle now).
Design decided 2026-07-27; picks up cleanly as a later slice, ideally alongside the Neon move.

**One-line:** Let an employee (or admin) attach a photo of a receipt when entering an expense,
visible to admin. Store the **image bytes in object storage** and keep only a **reference**
(URL + storage key) in the record, so the receipt data migrates Airtable → Neon as a plain
string instead of a binary attachment.

**Backend decision: Cloudflare R2** (presigned upload). Rejected alternatives below.

---

## The one principle (why R2, not the "easy" option)

Never put image bytes in the primary database — not an Airtable attachment, not a Neon `bytea`.
Photos live in object storage; the DB holds a reference. This keeps the receipt column a plain
string that migrates with zero binary wrangling. **Airtable attachments are specifically the
worst data type to migrate later** (proxied, rotating URLs), so the easy option is a trap given
the Neon goal.

### Backend options considered

| Option | Effort | Neon fit | Verdict |
|---|---|---|---|
| **Cloudflare R2 (presigned)** | Medium — bucket + presign fn + CORS | ✅ Best — fully decoupled from Airtable *and* Make; DB holds key+URL only | **CHOSEN** |
| Airtable attachment field | Lowest | ❌ Doubles down on the DB we're leaving; worst-case migration | Rejected |
| pCloud via existing Make webhook | Low — reuse `uploadPDFToPCloud` | OK (text link) | Rejected — deepens Make, against the shrink-Make trend (Drop-mirror, JotForm-retire) |
| Google Drive | Med-high | OK | Rejected — app would need its own service-account OAuth; more surface than R2 for no win |

---

## Data model (Neon-forward; one-to-many from day one)

Itemized expenses often have several receipts, so model it as a child table, not a single field.
Today = a linked Airtable "Expense Receipts" table; later = a straight Neon table.

```
expense_receipts
  id            uuid / recXXX
  expense_id    → Expenses            (FK / linked record)
  url           text                  (viewable link)
  storage_key   text                  (R2 bucket key — the durable id)
  content_type  text                  (image/jpeg…)
  uploaded_by   → Employees           (reuse the Submitted By identity)
  uploaded_at   timestamptz
  client_ref    text UNIQUE           (idempotency — reuse the "Push ID" pattern)
```

Dead-simple fallback if we want a smaller Phase 1: one `Receipt URL` text field on Expenses,
upgrade to the child table in Phase 2.

## Capture UX (mobile field techs)

- **Camera:** `<input type="file" accept="image/*" capture="environment">` → rear camera, still
  allows gallery pick.
- **Client-side compress before upload:** draw to `<canvas>`, resize ~1600px long edge, re-encode
  JPEG ~0.7. Turns 3–5 MB phone photos into <500 KB **and** converts iPhone HEIC → JPEG for free
  (Safari draws HEIC to canvas, exports JPEG). HEIC left raw is a viewing headache — convert on
  the client.
- **Where it shows:** thumbnail row on each expense in both the employee **🧾 My Expenses** tab
  and the admin Expenses view; tap for full view. Admin sees all; employees add/remove receipts
  only while the expense is still unreviewed — reuse the existing `guardExpenseMutation` window.
- **Offline:** field = spotty signal. Phase 1 requires online, blocks submit behind an upload
  spinner. Phase 2 queues in IndexedDB and retries.

## End-to-end flow (R2)

1. Employee snaps photo → client compresses.
2. Browser calls new function action `getReceiptUploadUrl` → returns a presigned PUT + storage key.
3. Browser PUTs the JPEG straight to the R2 bucket (bypasses the functions, like the pCloud PDFs
   do today via `uploadPDFToPCloud`).
4. Browser calls `attachReceipt` → function writes an `expense_receipts` row (url, key,
   `uploaded_by` from token, `client_ref` for idempotency).
5. Admin/employee views read receipts alongside the expense.

## Touch points in the current code

- Expense create: `handleAddGeneralExpense` (airtable.js:2518), `handleAddLiftExpense` (:2505) —
  Submitted By already stamped from token (`fldRWV0eIKwBrXwHV`).
- Ownership gate: `handleDeleteExpense` / `handleUpdateExpense` / `guardExpenseMutation`.
- Existing browser→cloud upload precedent: `uploadPDFToPCloud` (index.html:3264).
- Frontend expense form: `saveGeneralExpense` (~index.html:7624); My Expenses + admin Expenses
  render sites.
- New backend actions to register in the dispatch chain (~airtable.js:3831): `getReceiptUploadUrl`,
  `attachReceipt` (+ `deleteReceipt`). New env vars for R2 creds/bucket/endpoint.

## Phasing

- **Phase 1:** R2 bucket + presign fn + child table + capture UI + client compression, single
  receipt, admin-visible, online-only.
- **Phase 2:** multiple receipts, thumbnail/lightbox, offline queue, idempotency hardening.
- **Neon:** define `expense_receipts` in Neon so the Airtable shape mirrors it — then it's a
  straight ETL slice after time-entries.

## Open items before build

- Stand up R2 bucket + credentials; decide public-read vs. signed-URL-only (lean signed/private).
- CORS config on the bucket for direct browser PUT.
- Confirm signed-URL TTL / re-sign path for admin viewing months later.
