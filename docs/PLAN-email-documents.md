# PLAN — Email an estimate or invoice from the app

## ⏸ STATUS — read this first

**NOT STARTED.** Written 2026-09-03 from the owner's ask: *"when I create an
estimate or invoice, click email and it opens my Gmail with the PDF attached,
to the address supplied if there is one, with a short note."*

Nothing here is built. The slices below are independently shippable and ordered
cheapest-first — **Slice 1 is about an hour and is not thrown away by Slice 3.**

⚠⚠ **Read "The address book is the bottleneck" before estimating any of this.**
The button is the easy part. Today the app can resolve a recipient for roughly
a third of jobs and **never** from a contractor.

---

## The constraint that shapes everything

**A link cannot carry an attachment.** Neither `mailto:` nor Gmail's compose URL
(`https://mail.google.com/mail/?view=cm&to=…&su=…&body=…`) supports attachments.
That is an email/browser security rule, not a gap in this app, and no amount of
URL cleverness gets around it.

So "click a button → Gmail opens with the PDF attached" is only reachable two
ways:

1. **Put the file on the clipboard/share sheet** — `navigator.share({ files })`.
   Real attachment, but mobile-only and it cannot carry body text (see traps).
2. **Create the message server-side as a Gmail DRAFT**, then open Gmail on that
   draft. Real attachment, real body, desktop and mobile, and the owner still
   reviews before sending. This is the one that matches the ask.

Everything else is a two-step (download, then attach by hand).

---

## What already exists — more than you'd think

| Piece | Where | State |
|---|---|---|
| The PDF, in the browser, as bytes | `buildInvoicePDFDoc(snap, theme)` — one builder serves **both** estimate and invoice (the estimate passes `ESTIMATE_THEME`) | ✅ works |
| `pdfDoc.output('datauristring')` → base64 | already used by `uploadPDFToPCloud` | ✅ works |
| Recipient lookup, half-built | `window._invContractorEmail` (~14787) and `window._estContractorEmail` (~17021) | ⚠ **set and never read** — the comment says they are "for the Email button's recipient address"; the button was planned and never built |
| `navigator.share` with files | shipped 2026-08-06 for jobsite photos | ✅ working precedent to copy |
| Google OAuth: client id/secret, refresh tokens for `rick@` and `nee@`, token refresh helper | `_google-contacts.js` — `getAccessToken(subject)` (~218), `oauthTokenRequest(subject)` (~251) | ✅ **the expensive part is already paid for** |

The OAuth groundwork is the reason Slice 3 is worth doing at all. Without it this
would be a from-scratch Google integration.

---

## ⚠⚠ The address book is the bottleneck, not the button

Measured 2026-09-03:

| Source | Has an email |
|---|---|
| `companies.primary_email` (contractors) | **0 of 35** |
| `jobs.customer_email` | 42 of 122 |
| `contacts.primary_email` | 22 of 241 |

**Zero contractors have an email on file.** `_invContractorEmail` — the hook
written specifically to feed this button — resolves to `""` on every job in the
system, and always has.

So a recipient can be resolved for **about a third of jobs**, and never from the
contractor. Building the button without fixing this ships a feature that opens a
blank To: field most of the time and feels broken.

**Slice 0 exists because of this and should probably go first.**

---

## Slice 0 — make the address exist *(~1 h)*

Not glamorous, and it is the difference between the button working and not.

- Surface `Primary Email` on the company edit path so contractors can have one.
  ⚠ There is **no company edit path at all today** — `createCompany` exists,
  nothing updates one. Same write-once gap as the customer-name one found on
  2026-09-03; consider fixing both together.
- Show the resolved recipient on the estimate/invoice screen *before* the button
  is built, so it is obvious which jobs are missing one.
- Resolution order, once and shared: **job customer email → primary contact on
  the company → company primary email → blank**.

**Do not skip this to get to the fun part.** Slices 1–3 all consume the same
resolver; building it once here means the later slices are only about delivery.

---

## Slice 1 — download + prefilled Gmail compose *(~1 h)*

The honest two-step. Works on every device today, needs no new permission.

1. Build the PDF (already happens), `doc.save(filename)` — it lands in Downloads.
2. Open `https://mail.google.com/mail/?view=cm&fs=1&to=…&su=…&body=…` in a new tab.
3. Owner drags the file in.

Subject/body template lives in one constant so Slice 3 reuses it verbatim:

```
Subject: Northeastern Electric — Estimate 2220 — Phil McGuire
Body:    Hi Phil,
         Attached is the estimate for <job>. Let me know if you have questions.
         — Rick, Northeastern Electric  330-428-0480
```

⚠ Popup blockers: the `window.open` must happen **in the click handler**, not
after an `await`. Same class of bug as the iOS share-gesture trap below.

**Ships value immediately and is not wasted work** — Slices 2 and 3 reuse the
resolver, the filename and the template.

---

## Slice 2 — Web Share on mobile *(~1–2 h)*

Copy the photo-sharing implementation. Tap Email → pick Gmail → PDF genuinely
attached.

⚠⚠ **Two traps already learned the hard way on the photo feature — see
`project_photo_sharing` memory:**
- **Never send `text` together with `files`.** iOS silently drops the whole
  share. So on mobile you get the attachment **or** the note, not both.
- **iOS spends the user gesture on the fetch.** The file must be ready *before*
  `navigator.share` is called, or the share sheet never opens.
- Desktop support is patchy. Feature-detect `navigator.canShare({ files })` and
  fall back to Slice 1 rather than showing a button that does nothing.

Because of the text/files conflict this is a *worse* fit for "with a note" than
Slice 1 — it trades the note for a real attachment.

---

## Slice 3 — Gmail draft via the API *(the actual ask, ~4–6 h)*

Backend action `emailDocumentDraft`:

1. Take `{ type, jobId, to, subject, body, filename, pdfBase64 }`.
2. `getAccessToken(subject)` — **already written**, in `_google-contacts.js`.
3. Build a MIME `multipart/mixed`: `text/plain` part + `application/pdf`
   base64 part.
4. `POST https://gmail.googleapis.com/gmail/v1/users/me/drafts` with
   `{ message: { raw: base64url(mime) } }`.
5. Return the draft id; the client opens
   `https://mail.google.com/mail/u/0/#drafts?compose=<id>`.

Owner sees his own Gmail, PDF attached, note written, and presses Send himself.

### Traps

⚠⚠ **New scope, new consent.** `gmail.compose` must be added and both accounts
re-consented. The existing refresh tokens do **not** carry it.

⚠⚠ **The consent screen must stay `Internal`.** External + Testing refresh
tokens expire after **7 days** — this exact trap is documented in
`PLAN-google-contacts.md` and would silently kill the feature a week after it
ships.

⚠⚠ **`GOOGLE_REFRESH_TOKEN_1` is `rick@`, `_2` is `nee@`.** Nothing in a token
says whose it is. A draft created on the wrong account lands in an inbox nobody
is looking at. `googleStatus` already asks Google who each token belongs to —
reuse it, and decide deliberately which account sends.

⚠ **Payload size.** Netlify functions cap at ~4.5 MB and base64 inflates by ~33%.
A large estimate PDF could exceed it. Estimates are small today; if this ever
bites, the fix is the same one the photo path uses — presigned upload, never
through the function.

⚠ **Ships INERT.** Follow the house pattern: a `GMAIL_DRAFTS` env switch,
unset = the button does not render. `dry` = build the MIME and log its size
without calling Google.

---

## Recommended order

**Slice 0 → Slice 1**, then stop and use it. That is ~2 hours and covers the
common case.

Slice 3 when there is appetite for a real build; it turns a two-step into one
click and reuses everything from 0 and 1.

Slice 2 is optional — genuinely nice on a phone, but the no-text-with-files rule
means it cannot do the "with a note" half of the ask.

---

## Open questions for the owner

1. **Which account should send** — `rick@` or `nee@`? `nee@` is becoming the
   office address book; a shared sending identity may age better.
2. **Estimates and invoices both, or start with estimates?** Estimates go out
   more often and are lower-stakes to get slightly wrong.
3. **Is a draft the right stopping point**, or would you rather it just sent?
   A draft is recoverable; a send is not. Recommend draft.
