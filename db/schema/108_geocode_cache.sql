-- 108 — 🗺 All-lifts map (owner 2026-11-26).
-- Job-site coordinates, looked up once per ADDRESS and kept. Keyed by the
-- address text itself, so editing a job's address simply misses the cache and
-- the new one gets looked up — nothing to invalidate.
-- lat/lng NULL = looked up and not found (don't retry on every map open).
CREATE TABLE IF NOT EXISTS geocode_cache (
  address     text PRIMARY KEY,
  lat         double precision,
  lng         double precision,
  source      text,
  created_at  timestamptz NOT NULL DEFAULT now()
);
