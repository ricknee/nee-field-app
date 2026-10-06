-- Panel mounted UPSIDE DOWN — print the schedule the way the panel reads.
-- APPLIED to the default branch of Neon project damp-silence-99074350 on 2026-10-06.
--
-- Owner 2026-10-06, Equiprents: a panel deliberately mounted inverted has
-- circuit 1 at the BOTTOM, and turned 180° its odd bus is on the RIGHT. The
-- printed schedule started 1/2 at the top, odd on the left, so it read upside
-- down against the breakers it describes.
--
-- A display flag only. Circuit numbers, gangs and descriptions are unchanged —
-- the sheet, PDF and tape strips reverse their rows and swap sides when it is set.
-- Not `mounting`: that column is SURFACE / FLUSH, a different fact.

ALTER TABLE panel_schedules ADD COLUMN IF NOT EXISTS upside_down boolean NOT NULL DEFAULT false;
