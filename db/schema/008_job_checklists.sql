-- Neon slice 8 — JOB CHECKLISTS (docs/PLAN-job-checklists.md).
-- APPLIED to the default branch of Neon project damp-silence-99074350 on 2026-08-05.
--
-- Same convention as 001/002/003/007: applied BARE via the Neon MCP (which
-- mangles inline SQL comments), reasoning kept here. Keep the two in sync by hand.
--
-- Replaces the Trello checklist a crew keeps per job ("Supplies from shop",
-- "Punch list"): name a list, type items one per line, tick them off while
-- loading the truck. Second domain born in Neon rather than migrated to it —
-- there is no Airtable table for this and never was.
--
-- ── SAME KEYING TRAP AS PANEL SCHEDULES ────────────────────────────────────
-- job_airtable_id is the key; job_id is a nullable FK that backfills. The jobs
-- table refreshes hourly, so a job created ten minutes ago is not in Neon yet
-- and a NOT NULL FK would refuse the first list anyone makes on it. See
-- 007_panel_schedules.sql.

CREATE TABLE job_checklists (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_airtable_id text NOT NULL,
  job_id          uuid REFERENCES jobs(id),
  name            text NOT NULL,          -- "Supplies from shop"
  created_at      timestamptz DEFAULT now(),
  updated_at      timestamptz DEFAULT now(),
  created_by      text                    -- a NAME, not a link; see 007
);

CREATE INDEX job_checklists_job ON job_checklists (job_airtable_id);

CREATE TABLE checklist_items (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  checklist_id  uuid NOT NULL REFERENCES job_checklists(id) ON DELETE CASCADE,
  body          text NOT NULL,            -- '200ft 2" PVC'

  -- Ticked is NOT deleted. A tick moves the item into the collapsed "Loaded"
  -- section, where one tap puts it back. On a supply list a mis-tap that
  -- deleted the row would mean arriving without the pipe and never knowing
  -- which line went missing.
  done          boolean NOT NULL DEFAULT false,
  done_at       timestamptz,
  done_by       text,

  -- Typed order is the order things get loaded, so it is preserved explicitly
  -- rather than left to created_at — two items added in the same millisecond
  -- from a fast typist would otherwise sort arbitrarily.
  position      int NOT NULL DEFAULT 0,

  created_at    timestamptz DEFAULT now(),
  created_by    text
);

CREATE INDEX checklist_items_list ON checklist_items (checklist_id, position);
