-- ── Scissor Lifts → Neon (roadmap Step 4b, lifts half) ─────────────────────
-- Added 2026-08-05. APPLIED BARE to the default branch of Neon project
-- damp-silence-99074350 (the Neon MCP mangles inline SQL comments), reasoning
-- kept here — same convention as 001/002/003/007/008.
--
-- The data is trivial: 9 Airtable fields, 10 rows, no money, no formulas, no
-- rollups, no Make. THE PHOTOS ARE THE REAL WORK — see the warning below.
CREATE TABLE IF NOT EXISTS scissor_lifts (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  -- Nullable by design: lifts added after the write flip have no Airtable twin,
  -- and none at all once the Airtable table is retired.
  airtable_id   text UNIQUE,
  name          text NOT NULL,
  status        text NOT NULL DEFAULT 'Available',
  -- `current_job` is a job NAME string, not an FK. It mirrors Airtable, where
  -- Current Job is singleLineText — handleScissorLiftsByJob matches on the name.
  -- Left as text deliberately: making it a real FK is a behaviour change (a
  -- rename would move lifts between jobs), and belongs in its own decision.
  current_job   text,
  -- Also a plain string. In Airtable this is a singleSelect, NOT a link to
  -- Employees, so there is no employee FK to resolve here.
  assigned_to   text,
  date_deployed date,
  notes         text,
  hooks_left    boolean NOT NULL DEFAULT false,
  box_left      boolean NOT NULL DEFAULT false,
  created_at    timestamptz NOT NULL DEFAULT now(),
  synced_at     timestamptz
);

-- ⚠ PHOTOS LIVE IN R2, NEVER IN THIS TABLE, AND THAT IS NOT A STYLE CHOICE.
--
-- Airtable serves attachments from v5.airtableusercontent.com on SIGNED URLs
-- THAT EXPIRE (~2 hours). The old handler worked only because it re-fetched them
-- from Airtable on every single request. Storing `photo_url` here would look
-- correct in testing and then break every lift photo the same afternoon — a
-- silent, delayed failure of exactly the kind this migration keeps meeting.
--
-- So there is no photo column. R2 is the source of truth, keyed:
--
--   lifts/<lift uuid>/<airtable attachment id>.jpg
--
-- Keyed on the ATTACHMENT id rather than the filename because two lifts can both
-- have "photo.jpg", and a rename in Airtable must not orphan the copy. One
-- listByPrefix('lifts/') call serves the whole lifts page; presigning is local
-- and needs no network. Existing prefixes are jobs/, expenses/ and _deleted/, so
-- lifts/ collides with nothing.
--
-- Backfill + the one-pass photo copy: db/etl/scissor-lifts.mjs
-- Phase 1 run 2026-08-05: 10 rows. Phase 2 pending R2 credentials locally.

-- ── Natural sort, computed at read time ────────────────────────────────────
-- Airtable sorted by Lift Name as TEXT, so "Lift #10" landed between #1 and #2.
-- Owner asked for natural order. Done in the query rather than a stored sort
-- column so a rename cannot leave a stale key behind:
--
--   ORDER BY NULLIF(regexp_replace(name, '\D', '', 'g'), '')::int NULLS LAST, name
--
-- A lift with no digits in its name falls back to alphabetical instead of
-- erroring on the int cast. Verified: #1 … #9, #10.

-- ── Deleting a sold lift ───────────────────────────────────────────────────
-- Owner's decision 2026-08-05: selling a lift removes EVERYTHING, photos included.
-- Safe to hard-delete — handleAddLiftExpense links its Expense to the JOB, with a
-- hardcoded vendor and type "Scissor Lift"; it never references the lift record,
-- so no financial history depends on this row. The handler must also delete the
-- lifts/<id>/ prefix from R2, since nothing else will.
