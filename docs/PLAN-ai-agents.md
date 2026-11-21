# PLAN — AI helpers in the field app

**Status (2026-11-20):** owner picked #1 and #2, both built. Everything else below is **plan only. Don't build it until the owner asks.**

## Ground rules (all of them)

- **AI reads and suggests. A person approves.** No AI path creates a job (that burns a PO number for good), creates or changes an expense, invoice or estimate price, or sends anything to a customer.
- **An AI answer is untrusted input.** Validate every id it returns against the list it was given, and refuse anything that doesn't fit.
- **Inert without the key.** Everything checks `ANTHROPIC_API_KEY` and does nothing when it's unset.
- **Slow work runs in a `*-background` function.** Normal functions time out at 10 s.
- **The SDK is `@anthropic-ai/sdk`.** It's lazy-imported (the test suite stays offline) and listed in `external_node_modules`.
- **Model:** `claude-opus-5-5` with `fallbacks: "default"`.
- **Cost:** check volume × tokens before anything runs on a schedule.

## ✅ Built

### #1 ☀ This morning (Overview card)
- `GET morningDigest` (admin + office) feeds a card at the top of the Overview. It shows app errors from the last 24 h (`client_errors`), open invoices whose estimate changed, supplier invoices waiting (plus how many have an AI suggestion), hourly sync health, and the 12 data checks.
- No AI here. The numbers come straight from the database.
- Owner said **no** to "hours with no job" and the hours-target nudge on this card.

### #2 🤖 Supplier invoice reader
- Schema 102 adds `vendor_invoices.ai_*`. The code lives in `_ai-invoice-match.js` and `ai-invoice-suggest-background.js`.
- It's triggered at intake when an invoice parks, by the hourly sweep in `qb-time-pull`, and by the 🤖 button (`vendorInvoiceSuggest`).
- Review screen: suggested job + reason, then **Use this**. That runs the normal `vendorInvoiceAssign`, confirm dialog included.
- ⬜ **Check after the first real parked invoice:** was the suggestion right? Compare `ai_job_id` against the job a person actually picked:
  ```sql
  SELECT invoice_no, ai_confidence, ai_reason, ai_job_id = job_id AS agreed
    FROM vendor_invoices
   WHERE ai_read_at IS NOT NULL AND status = 'matched';
  ```

## ⏸ Planned (not built)

### #3 Panel schedule from a photo
- The full plan is already written: `docs/PLAN-panel-ai-import.md`.
- It now only needs the owner's go-ahead, since the API key will exist.

### #4 Business card → Contacts
- The full plan is already written: `docs/PLAN-business-card-scan.md`. Same status as #3.

### #5 Draft proposal text
- In Est GP's new ✏ title-and-text editor, add a **"Draft it for me"** button.
- You type rough notes ("1 Cummins 150 kW indoor, set by others, exhaust thru wall, no fuel"). It returns proposal text in the house format: `##` heading, `*` bullets, `---`, plus an Exclusions section.
- Feed it 3–5 past proposals from `job_estimates.notes` as examples.
- It fills the textarea only. Nothing saves until you press Save.

### #6 "Ask the app" (office)
- A question box: "hours on Seneca last week?", "what's unbilled on WatersEdge?"
- Safest shape: Claude picks from a fixed set of existing read actions (`hoursByJob`, `getJobInvoices`, `workload`, …) as tools. **No free-form SQL.**
- Admin + office only.

### #7 Crew voice notes
- Record on the phone, transcribe, then Claude tidies it into a job note or daily log entry.
- Needs a speech-to-text step (the browser's own, or an API).
- Review before saving.

### #8 Error investigator (cloud routine)
- A Claude Code cloud routine that reads `client_errors` each morning, finds the line in `index.html`, and writes up the likely cause.
- Needs GitHub connected to claude.ai (`/web-setup`) and Neon added at claude.ai/customize/connectors.
- The ☀ card already shows the errors; this would add the "why".
