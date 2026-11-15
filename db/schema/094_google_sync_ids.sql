-- 094 — Google contact sync: per-person ids for power-co and inspection people,
-- and a "what Google last got" fingerprint on all three people tables.
-- Owner 2026-11-15: sync to rick@northeasternelec.com ONLY, labelled by tab
-- (NEE Customers / Contractors / Power Co. / Inspection) and by company.
-- See netlify/functions/_google-contacts-sync.js.
--
-- ⚠⚠ google_person_id_1 IS THE ONLY THING STOPPING A DUPLICATE. Schema 049
-- deliberately gave power_contacts no per-person id (the old Make sync filed
-- them by a constant LABEL id, not a person id). Syncing them per person needs
-- one, or every run would create every power contact again. Same for
-- inspection_contacts, which were never synced at all.
--
-- Bare ids ("c1234…"), same form contacts.google_person_id_1 already holds —
-- see toResourceName()/toStoredId() in _google-contacts.js.
--
-- google_sync_hash: a fingerprint of the person + labels last written to
-- Google. Unchanged fingerprint → no Google call at all, which is what keeps an
-- hourly pass over ~280 people inside a few seconds. NULL = never synced (or
-- changed since), so the next pass looks at it.

ALTER TABLE power_contacts      ADD COLUMN IF NOT EXISTS google_person_id_1 text;
ALTER TABLE power_contacts      ADD COLUMN IF NOT EXISTS google_sync_hash   text;
ALTER TABLE inspection_contacts ADD COLUMN IF NOT EXISTS google_person_id_1 text;
ALTER TABLE inspection_contacts ADD COLUMN IF NOT EXISTS google_sync_hash   text;
ALTER TABLE contacts            ADD COLUMN IF NOT EXISTS google_sync_hash   text;
