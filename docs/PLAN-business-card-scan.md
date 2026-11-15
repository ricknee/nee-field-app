# PLAN — Fill a contact from a business card (NOT BUILT)

Owner, 2026-11-15: "keep the idea as a plan". The 📇 Contacts screen shipped
first (`_contacts-directory.js`); this adds a way to fill its add/edit form
from a card instead of typing. **Do not build until the owner asks.**

## Why it waits
The one-tap version needs an AI API key, which is billed **separately** from
the owner's Claude subscription (~$1–3/month for cards alone). The owner
didn't want a new monthly cost yet. ChatGPT vs Claude was asked about: either
reads cards about equally well; Claude was suggested so one key also covers
the panel-schedule AI fill (`docs/PLAN-panel-ai-import.md`).

## Options, cheapest first
1. **📋 Paste text (free, recommended first step).** Phone camera / Google
   Lens → "Copy text" → paste into a box on the contact form. A parser fills:
   phones (labelled `Office:` / `Mobile:` / `Cell:` / `Fax:`; unlabelled →
   first = phone), email, website, `Street, City, ST 12345`. Name and title
   are guessed (first two short lines without digits/@) and shown for a check.
   Nothing saves until the user taps Save.
2. **On-device OCR (free).** Tesseract.js lazy-loaded from a CDN like jsPDF,
   then the same parser. Slower (5–10 s) and worse on glossy/curved cards.
3. **📷 AI scan (paid).** Photo → new `scanBusinessCard` action → vision model
   returns JSON {firstName, lastName, title, company, officePhone, cellPhone,
   email, street, city, state, zip, website}. Same "check, then Save" flow.
   Needs `ANTHROPIC_API_KEY` (or OpenAI) in Netlify. Photo is not stored.

## Fit with the Contacts screen
- Button sits at the top of the person form in `ctOpenEdit` (index.html).
- Map onto the existing field keys (`CT_FORMS`): power-co contacts have
  `phone` = cell and `officePhone`; contacts have one `phone` — prefer mobile.
- The `company` on the card is only a hint for the org picker (match by name);
  never create a company from a card automatically.

## Same key would also unlock (owner was shown these)
Receipts → expense, generator nameplates → generator record, panel schedules,
utility paperwork numbers, unreadable supplier tickets, invoice ↔ job
suggestions, service-report wording, estimate drafts, print counts, questions
about the business's own data, NEC lookups.
