-- Neon slice 7 — PANEL SCHEDULES (docs/PLAN-panel-schedules.md).
-- APPLIED to the default branch of Neon project damp-silence-99074350 on 2026-08-05.
--
-- Same convention as 001/002/003: applied BARE via the Neon MCP (which mangles
-- inline SQL comments), with the reasoning kept here. Keep the two in sync by hand.
--
-- ── WHY THIS IS IN NEON AND NOT AIRTABLE ───────────────────────────────────
-- Panel schedules have no Airtable table today. That makes this the first domain
-- in the app BORN in Neon rather than migrated to it. Building it in Airtable
-- would mean building it twice — once now, once at roadmap Step 4 — for a domain
-- with no legacy to preserve. It is also a safe place to prove the Neon-native
-- pattern: no money, no payroll, nothing on the Make critical path.
--
-- ── THE KEY IS THE AIRTABLE RECORD ID, NOT A FOREIGN KEY ───────────────────
-- job_airtable_id is text and NOT NULL; job_id is a nullable FK that backfills.
-- The jobs table refreshes HOURLY, so a job created ten minutes ago does not
-- exist in Neon yet. A NOT NULL FK to jobs(id) would reject the first panel
-- anyone adds to a brand-new job, with a foreign-key error the user cannot act
-- on. This is the same trap that produces "new job = empty Time Entries tab for
-- ~1 h", and the same fix time_entries already uses.

CREATE TABLE panel_schedules (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  job_airtable_id text NOT NULL,
  job_id          uuid REFERENCES jobs(id),

  -- The label as an electrician says it: "Panel A", "MOP", "Classroom Panel".
  name            text NOT NULL,
  -- Free text rather than an enum. The UI offers the four common systems as
  -- presets, but a real building will eventually hold something not on that
  -- list, and a CHECK constraint would turn that into a 500 in a panel room.
  voltage         text,          -- "120/240V 1-Phase" | "120/208V 3-Phase" | ...

  -- Even only: the odd-left/even-right layout is not defined for an odd count.
  -- 84 is the largest panelboard anyone actually specifies; the cap is a
  -- typo-catcher (a fat-fingered 420 would render 210 rows), not a code limit.
  circuits        int NOT NULL CHECK (circuits BETWEEN 2 AND 84 AND circuits % 2 = 0),

  -- Header metadata off the finished-schedule example. All optional: they print
  -- if set and are simply absent if not. Slice 1's form asks only for name,
  -- voltage and circuits.
  feed            text,          -- "MLO" | "MAIN BREAKER"
  mounting        text,          -- "SURFACE MOUNT" | "FLUSH"
  enclosure       text,          -- "NEMA1"
  location        text,          -- "Boiler room, north wall"
  fed_from        text,          -- "Panel D"
  notes           text,

  created_at      timestamptz DEFAULT now(),
  updated_at      timestamptz DEFAULT now(),
  -- A NAME, not a link — same reasoning as time_entries.job_name. The text
  -- snapshot is the history; an employee record edited in 2027 must not rewrite
  -- who filled in a panel schedule today.
  updated_by      text
);

CREATE INDEX panel_schedules_job ON panel_schedules (job_airtable_id);

CREATE TABLE panel_circuits (
  panel_id    uuid NOT NULL REFERENCES panel_schedules(id) ON DELETE CASCADE,
  number      int  NOT NULL CHECK (number > 0),
  description text NOT NULL DEFAULT '',

  -- Slice 3 columns, in the schema from day one deliberately. Empty they cost
  -- nothing; added later they are a migration plus a deploy-ordering problem on
  -- a live table. The UI stays description-only until slice 3 — that is a UI
  -- decision, not a schema one.
  watts       int,
  amps        int,
  poles       int,

  -- The composite PK is what makes the save idempotent: the client sends the
  -- whole panel every time and ON CONFLICT (panel_id, number) DO UPDATE turns a
  -- double-tap on Save into a no-op instead of a duplicate row.
  PRIMARY KEY (panel_id, number)
);
