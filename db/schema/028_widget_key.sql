-- Neon slice — a revocable key for the home-screen clock widget.
--
-- Applied BARE via the Neon MCP; this file is the annotated source of truth.
--
-- A home-screen widget cannot log in. It is a script or a widget host (Scriptable
-- on iOS, KWGT/Tasker on Android) doing a plain GET every so often, with no
-- session and often no ability to set request headers at all. So it needs a
-- credential it can carry in a URL.
--
-- ── WHY THIS IS SAFE ENOUGH TO PUT IN A URL ──────────────────────────────────
-- A token in a query string leaks: into browser history, screenshots, and any log
-- that records full URLs. That is acceptable ONLY because of how narrow this one
-- is, and each of these is load-bearing:
--
--   • It authorises exactly ONE read action, `clockWidget`, and nothing else.
--   • That action returns only that person's own clock state — am I on, since
--     when, which job, how long today. No pay, no rates, no roster, no money.
--   • It is per-person, so a leak exposes one person's shift, not the company.
--   • It is REVOCABLE without touching their login: regenerate the key below and
--     every widget URL ever issued to them stops working, while their phone stays
--     signed in. That separation is the whole reason for a dedicated column
--     rather than reusing token_valid_from.
--
-- The signature itself is signScope() from _auth.js over
-- ('clockwidget', airtable_id, widget_key) — so changing the key invalidates the
-- signature, and a token minted for one person cannot be replayed for another.
ALTER TABLE employees ADD COLUMN IF NOT EXISTS widget_key uuid;

COMMENT ON COLUMN employees.widget_key IS
  'Secret component of this person''s home-screen widget URL. Regenerating it '
  'revokes every widget link previously issued to them, without affecting their '
  'app session. NULL means they have never created one.';
