-- 049_contact_google_ids.sql — capture the Google person ids before they are lost
--
-- Groundwork for item 07 (Neon → Google People API direct, replacing the five
-- retired Make sync scenarios). NOTHING READS THESE COLUMNS YET.
--
-- ⚠⚠ WHY THIS EXISTS AT ALL. 192 of the 239 contacts are ALREADY in Google, and
-- the only record of which Google person each one is lives in two Airtable
-- fields. A direct sync that starts without them does not update 192 contacts —
-- it CREATES 192 duplicates, and twice over, because there are two destinations.
-- In a live address book on people's phones. Capturing them now costs nothing;
-- recovering from that would cost an afternoon of manual merging.
--
-- ⚠ THERE ARE TWO DESTINATIONS, not one. Both fields are per-person and fully
-- distinct — measured across 200 records: 192 present, 192 distinct, in EACH
-- column. The retired "needs sync" formula flagged a record when EITHER was
-- blank, which is what gives away that two separate Google accounts were being
-- written to. Which account is which is NOT recorded anywhere; the suffixes
-- below are positional, matching the Airtable field ids, and whoever builds the
-- sync has to establish the mapping before writing.
--
--   google_person_id_1  <- fld7baYOGRf3mmdl1
--   google_person_id_2  <- fldZ4H2ob1lcOmZDp
--
-- ⚠⚠ POWER CONTACTS HAVE NO EQUIVALENT, and that is a design input, not an
-- oversight. Their two sync fields hold contact GROUP ids — verified identical
-- on all 25 rows (contactGroups/36e512d0097f117f and
-- contactGroups/593386b00fa9ca08) — so they say which Google label to file
-- under, not which person a row became. There is therefore NO id to match on:
-- a direct sync must either match power contacts by name/phone/email or accept
-- creating them fresh. No column is added there, because storing the same
-- constant 25 times would imply a per-row identity that does not exist.

ALTER TABLE contacts
  ADD COLUMN IF NOT EXISTS google_person_id_1 text,
  ADD COLUMN IF NOT EXISTS google_person_id_2 text;

COMMENT ON COLUMN contacts.google_person_id_1 IS
  'Google People person id, destination 1 (Airtable fld7baYOGRf3mmdl1). Read-only capture for item 07; nothing reads it yet.';
COMMENT ON COLUMN contacts.google_person_id_2 IS
  'Google People person id, destination 2 (Airtable fldZ4H2ob1lcOmZDp). Which account is which is not recorded anywhere.';
