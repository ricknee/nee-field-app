# Change request for the MiniBee invoice bot

**Copy this file into the MiniBee project.** It is written to be handed straight to
whoever — or whatever — works on that code. It describes one change.

The app side is already built, deployed and live. Nothing here needs anything else
built; it needs one HTTP call added.

---

## The change in one sentence

Where the bot decides an invoice **needs reviewing** — the branch that prints it —
also POST that invoice to the field app, so it lands on the app's review screen
instead of only on paper.

## What must NOT change

- Invoices whose PO **does** match a job keep going onto that job exactly as they do
  now, through the existing `addGeneralExpense` call. Do not touch that path.
- Keep the printing. This adds a step; it does not replace one.
- Keep the existing login. The same session token is used for the new call.

---

## Why it's needed

The MiniBee is the only thing that ever sees these invoices. They arrive by email, the
bot reads them, and when the PO doesn't match it prints the paper and stops — nothing
tells the app.

The app cannot go looking for them: it has no access to that mailbox, and it cannot
read pCloud. So the review screen stays empty unless the MiniBee sends to it.

---

## The token

The bot already logs in and holds a token — **reuse it, don't add a second login.**
For reference, that call is:

```
POST https://hub.northeasternelec.com/.netlify/functions/airtable
Content-Type: application/json

{ "action": "login", "identifier": "<the bot's username>", "pin": "<its PIN>" }
```

It answers `{ "ok": true, "user": {...}, "token": "…" }`. The token lasts 30 days and
goes on every later call as `Authorization: Bearer <token>`.

⚠ **The credentials are already in the bot's own config. Do not copy them into a chat,
a prompt, or this file.** Read them from wherever the existing code reads them.

## The call

Same host, same bearer token the bot already holds from its login.

```
POST https://hub.northeasternelec.com/.netlify/functions/airtable
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{
  "action":      "vendorInvoiceIntake",
  "vendor":      "CED",
  "invoiceNo":   "0171-1063885",
  "po":          "CAJ 436",
  "invoiceDate": "2026-08-28",
  "amount":      "1063.08",
  "taxAmount":   "0.00",
  "pdfBase64":   "JVBERi0xLjQK…"
}
```

| Field | Required | Notes |
|---|---|---|
| `action` | yes | exactly `vendorInvoiceIntake` |
| `vendor` | yes | must read as CED or Wolff. The full letterhead name works — `"WOLFF BROS. SUPPLY, INC."` and `"CED CONSOLIDATED ELECTRICAL DISTRIBUTORS, INC."` both resolve. Anything else is a 400. |
| `invoiceNo` | yes | the supplier's invoice number |
| `po` | no | the PO **exactly as read off the paper**. Omit or send `null` when the parser found none. |
| `invoiceDate` | no | `YYYY-MM-DD` |
| `amount` | no | string or number, **signed** — see below |
| `taxAmount` | no | same |
| `pdfBase64` | no | the invoice PDF, base64, under 8 MB decoded. Strongly recommended: without it the review screen shows figures but no invoice. Observed sizes are 15–26 KB. |

These are the same values the bot already extracts in order to create an expense.
Nothing new has to be parsed.

---

## ⚠ Send the values raw. Do not tidy them.

This is the part that is easy to get wrong, because cleaning the data up looks like
being helpful. The app handles all three of these itself, and pre-cleaning destroys
what it needs.

1. **The trailing minus is real and must survive.** Wolff prints credit memos as
   `773.85-`. Pass the string through untouched — the app parses trailing, leading and
   parenthesised minus. **Strip it and a credit becomes a charge**, and the job gets
   billed for gear that went back.

2. **Do not normalise the PO.** CED's parse reads `CAJ 436`; Wolff's reads `CAJ436`;
   the job record says `Joe Yoder (CAJ 436)`. The app reduces all of them to
   letters-and-digits before comparing, so it reconciles them by itself. Send whatever
   the parser saw, spaces and all — **and send it even when it looks wrong**, because a
   bad PO visible on the screen is how somebody spots a mistyped one.

3. **"No PO" is a valid message, not an error.** Omit the field. That records the
   invoice as *no PO on the invoice*, which is a different problem from *no job has
   this PO* and is handled differently at the other end.

---

## Responses

All are HTTP 200 unless noted. None of them should stop the bot processing the rest of
the batch.

| Response | Meaning | What the bot should do |
|---|---|---|
| `{ok:true, status:"needs_review", reason, id}` | Parked for a person. `reason` is `no-po-on-invoice`, `no-job-match` or `ambiguous-po`. | Normal outcome. Log and continue. |
| `{ok:true, status:"matched", jobId, expenseId}` | The app matched a job the bot didn't, and created the expense itself. Its PO matching is more forgiving about spacing. | Success. **Do not also call `addGeneralExpense`** — the expense already exists. |
| `{ok:true, duplicate:true, id, status}` | Already had this invoice. Nothing written. | Success. Continue. |
| `400 {ok:false, error}` | Vendor wasn't CED or Wolff, or `invoiceNo` was empty. | Log loudly — something upstream is wrong. |
| `401` | Token expired or missing. | Log in again and retry. |
| `502 {ok:false, id, error}` | Recorded, but something failed afterwards. The invoice is on the review screen. | Safe to retry. |

### Retrying is always safe

Duplicates are rejected on the vendor plus the invoice number, **ignoring case and
punctuation** — `0171-1055250` and `0171 1055250` are treated as the same invoice. If
the bot re-reads a folder on a timer, re-sending costs one no-op call and cannot charge
anything twice.

---

## Testing it

There is no sandbox — this hits production. To test safely:

1. Send one call with an obviously fake `invoiceNo` (e.g. `TEST-001`) and a `po` that
   matches nothing. Expect `{"ok":true,"status":"needs_review","reason":"no-job-match"}`.
2. Confirm it appears in the app under **☰ → 🧾 Invoice Review**.
3. Dismiss it from that screen. Test rows are cleared that way, not by deleting.

Do **not** test with a `po` that matches a real job — that creates a real expense on
that job.

---

## How to know it worked in production

The call returning 200 is not the proof. What landed is.

After the next real batch, open **☰ → 🧾 Invoice Review** in the field app. Everything
the MiniBee printed should also be sitting there, with its PDF, a job picker and a
Dismiss button. The ☰ button carries a red dot while any are waiting.

- **Nothing there** — the call isn't landing. Check the bearer token first; a 401 may be
  getting swallowed.
- **Many more than the printed pile** — worth investigating. It would mean the bot has
  been dropping more invoices than the paper suggested.

---

## Check the finished code against this list

These are the mistakes most likely to be made here, in order of how much they cost and
how quietly they do it. Every one of them produces code that runs fine and looks right.

- [ ] **The amount is passed through as-is.** Search the diff for `parseFloat`,
      `Number(`, `.replace('-','')`, `Math.abs`, `.trim()` on the amount. A Wolff credit
      arrives as `773.85-`; anything that "fixes" that string turns a refund into a
      charge. The app wants the raw value. **This is the expensive one.**
- [ ] **The PO is passed through as-is** — spaces, case and all. No `.replace(/\s/g,'')`,
      no uppercasing, no stripping. The app does that itself, on both sides of the
      comparison.
- [ ] **A missing PO omits the field or sends `null`** — not `""`, not `"N/A"`, not
      `"unknown"`. Those are strings that match no job and read as a real PO on screen.
- [ ] **The matched path is untouched.** `addGeneralExpense` still fires for invoices
      whose PO matched, and the new call did not get added to that branch as well. An
      invoice must not go down both paths.
- [ ] **`status:"matched"` from the new call does NOT then also call
      `addGeneralExpense`.** The expense already exists; calling again bills the job twice.
- [ ] **Errors don't halt the batch.** One bad invoice should be logged and skipped, not
      stop the other nine.
- [ ] **No credentials were hardcoded** into the new code, and none ended up in a log line.
- [ ] **The test used a fake invoice number and a PO matching no job** — not a real one.

## Optional, later — not part of this change

The bot could eventually send **every** invoice, matched ones included, and let the app
do all the PO matching. That puts the matching rule in one place instead of two that can
drift, and gives a record of every invoice that ever arrived rather than only the
failures. The endpoint already accepts this.

It is a bigger change and is not needed. Make the small one above first.

---

*App-side reference: `docs/PLAN-vendor-invoice-review.md` §4 and
`netlify/functions/_vendor-invoices.js` in the `nee-field-app` repo.*
