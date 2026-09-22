-- JOB FILE ORDER — the hand-set order of a job's Prints and Specs lists.
-- APPLIED to the default branch of Neon project damp-silence-99074350 on 2026-09-22.
--
-- Same convention as the other files here: applied BARE via the Neon MCP
-- (which mangles inline SQL comments), with the reasoning kept here.
--
-- ── WHY THIS IS THE ONLY TABLE PRINTS HAVE ─────────────────────────────────
-- Prints (and specs) are "the folder is the record": R2 answers what files a
-- job has, and nothing in Postgres lists them. That stays true. This table
-- holds ONE thing R2 cannot — the order someone dragged them into — as a list
-- of FILENAMES, which is what a print's key already is (the key IS the name).
--
--  * A name here with no file in R2 is ignored on read (deleted, binned).
--  * A file in R2 with no name here is shown ABOVE the ordered ones, newest
--    first — a fresh upload lands where it will be seen and dragged into place.
--  * Re-uploading "E-1.pdf" replaces the file and KEEPS its place, because
--    the name did not change.
-- So the list can go stale in either direction and nothing breaks; the worst
-- case is the order a list had before anyone dragged it.
--
-- ── KEYED ON THE R2 JOB SEGMENT, NOT jobs.id ───────────────────────────────
-- `job_key` is the same text that sits in `jobs/<job_key>/_prints/` — whatever
-- id the client holds (rec… for older jobs, uuid for native ones). Keying on
-- the string R2 uses means an order row can never disagree with the folder it
-- orders. No FK for the same reason: R2 has none either.

CREATE TABLE job_file_order (
  job_key     text NOT NULL,
  kind        text NOT NULL CHECK (kind IN ('prints', 'specs')),
  names       text[] NOT NULL DEFAULT '{}',
  updated_at  timestamptz NOT NULL DEFAULT now(),
  -- A NAME, not a link — same reasoning as panel_schedules.updated_by.
  updated_by  text,
  PRIMARY KEY (job_key, kind)
);
