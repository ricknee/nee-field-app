-- 035_material_estimating.sql — Step D, the last inventory slice.
-- Estimates, their lines, templates, template lines, material orders and order
-- lines. Depends on 029 (inventory_items) and 032 (the ledger).
-- ---------------------------------------------------------------------------
-- ⚠⚠ EVERYTHING HERE IS PREFIXED `material_`, AND THAT IS NOT DECORATION.
-- Neon already has `job_estimates` — the MAIN base's estimates, which feed the
-- GP views and decide what a job is expected to earn. The inventory base's
-- `Estimates` is a completely different thing: a materials take-off. Letting
-- the two share a name would corrupt the GP layer with take-off numbers, and it
-- would do it quietly. The prefix makes the namespace obvious at a glance.
--
-- ⚠ Conduit assemblies are NOT here. Owner decision 2026-08-11: the three
-- Airtable tables (Labor Units, Conduit Assemblies, Assembly Components) were
-- built but never wired into the app, so there is no behaviour to preserve.
-- They get built natively in Neon when they are actually wanted — the same call
-- that was made for panel schedules and job checklists.
--
-- ⚠ Airtable's rollups, counts and formulas are VIEWS at the bottom, not
-- columns: `Total` on Estimates, `Line Total` on both line tables, `Total Items`
-- on Material Orders, and `$ Current Line Total` on template lines. Storing a
-- derived total is how the Stock Levels cache drifted (Step C) — a number that
-- can be recomputed should be.
--
-- ⚠ Money is numeric(14,4). Unit costs carry sub-cent precision; rounding at
-- rest threw four expense rows off by a cent at Step 4d.
--
-- Airtable stays the identity authority: `airtable_id` is UNIQUE everywhere and
-- the loader upserts on it.

BEGIN;

-- ── Estimates — a materials take-off for a job (14 rows) ───────────────────
CREATE TABLE IF NOT EXISTS material_estimates (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id      text UNIQUE NOT NULL,
  job_name         text,        -- "Name (PO)" display, captured at creation
  job_airtable_id  text,        -- main-base job rec id; text, not a link
  status           text,
  created_by       text,
  created_at       timestamptz,
  notes            text,
  synced_at        timestamptz
);
CREATE INDEX IF NOT EXISTS mat_est_job_idx ON material_estimates (job_airtable_id) WHERE job_airtable_id IS NOT NULL;

-- ── Estimate line items (591 rows) ────────────────────────────────────────
-- `Unit Cost at Time of Estimate` is a SNAPSHOT and stays one: re-deriving it
-- from the item's current cost would silently re-price every historical
-- estimate the moment a vendor moved.
CREATE TABLE IF NOT EXISTS material_estimate_lines (
  id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id           text UNIQUE NOT NULL,
  line_number           integer,      -- Airtable autoNumber, kept for reference
  estimate_airtable_id  text,
  estimate_id           uuid REFERENCES material_estimates(id) ON DELETE CASCADE,
  item_airtable_id      text,
  item_id               uuid REFERENCES inventory_items(id) ON DELETE SET NULL,
  quantity              numeric(14,4) NOT NULL DEFAULT 0,
  unit_cost_at_estimate numeric(14,4),
  description           text,         -- free-text "Misc" lines carry no item link
  synced_at             timestamptz
);
CREATE INDEX IF NOT EXISTS mat_est_line_est_idx  ON material_estimate_lines (estimate_id);
CREATE INDEX IF NOT EXISTS mat_est_line_item_idx ON material_estimate_lines (item_id);

-- ── Estimate templates (3 rows) ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS material_estimate_templates (
  id                       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id              text UNIQUE NOT NULL,
  name                     text NOT NULL,
  description              text,
  contractor               text,       -- free text, used to filter the picker
  source_estimate_ref      text,       -- a LABEL, deliberately not a link
  total_at_save            numeric(14,4),
  active                   boolean NOT NULL DEFAULT true,
  created_by               text,
  created_at               timestamptz,
  synced_at                timestamptz
);

-- ── Template lines (149 rows) ─────────────────────────────────────────────
-- Both a snapshot AND live pricing matter here, which is the point of the
-- table: quantities clone to a new estimate, but unit costs are pulled fresh.
CREATE TABLE IF NOT EXISTS material_estimate_template_lines (
  id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id           text UNIQUE NOT NULL,
  line_title            text,
  template_airtable_id  text,
  template_id           uuid REFERENCES material_estimate_templates(id) ON DELETE CASCADE,
  item_airtable_id      text,
  item_id               uuid REFERENCES inventory_items(id) ON DELETE SET NULL,
  quantity              numeric(14,4) NOT NULL DEFAULT 0,
  unit_cost_at_save     numeric(14,4),
  notes                 text,
  synced_at             timestamptz
);
CREATE INDEX IF NOT EXISTS mat_tmpl_line_tmpl_idx ON material_estimate_template_lines (template_id);
CREATE INDEX IF NOT EXISTS mat_tmpl_line_item_idx ON material_estimate_template_lines (item_id);

-- ── Material orders (10 rows) ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS material_orders (
  id                    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id           text UNIQUE NOT NULL,
  order_number          integer,       -- Airtable autoNumber
  estimate_airtable_id  text,
  estimate_id           uuid REFERENCES material_estimates(id) ON DELETE SET NULL,
  job_name              text,
  vendor_notes          text,
  status                text,
  order_type            text,
  created_by            text,
  created_at            timestamptz,
  synced_at             timestamptz
);
CREATE INDEX IF NOT EXISTS mat_order_est_idx ON material_orders (estimate_id);

-- ── Material order lines (248 rows) ───────────────────────────────────────
-- `Line Total` is a stored currency field in Airtable rather than a formula, so
-- it is carried AND recomputed in the view; see the note there.
CREATE TABLE IF NOT EXISTS material_order_lines (
  id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  airtable_id         text UNIQUE NOT NULL,
  line_number         integer,
  order_airtable_id   text,
  order_id            uuid REFERENCES material_orders(id) ON DELETE CASCADE,
  item_airtable_id    text,
  item_id             uuid REFERENCES inventory_items(id) ON DELETE SET NULL,
  description         text,
  quantity_ordered    numeric(14,4) NOT NULL DEFAULT 0,
  unit_cost_at_order  numeric(14,4),
  line_total_stored   numeric(14,4),   -- Airtable's stored value, kept to diff against
  received            boolean NOT NULL DEFAULT false,
  notes               text,
  synced_at           timestamptz
);
CREATE INDEX IF NOT EXISTS mat_order_line_order_idx ON material_order_lines (order_id);
CREATE INDEX IF NOT EXISTS mat_order_line_item_idx  ON material_order_lines (item_id);

-- ── Views replacing Airtable's rollups, counts and formulas ───────────────

-- `Line Total` on Estimate Line Items, and `Total` on Estimates.
CREATE OR REPLACE VIEW v_material_estimate_totals AS
SELECT e.id                                     AS estimate_id,
       e.airtable_id                            AS estimate_airtable_id,
       COUNT(l.id)                              AS line_count,
       COALESCE(SUM(l.quantity * l.unit_cost_at_estimate), 0)::numeric(14,4) AS total
FROM material_estimates e
LEFT JOIN material_estimate_lines l ON l.estimate_id = e.id
GROUP BY e.id, e.airtable_id;

-- `Total at Save` versus what the same list would cost TODAY. The second number
-- is the whole reason templates exist — quantities clone, prices refresh — and
-- in Airtable it was `$ Current Line Total`, a formula over a lookup.
CREATE OR REPLACE VIEW v_material_template_totals AS
SELECT t.id                                     AS template_id,
       t.airtable_id                            AS template_airtable_id,
       COUNT(l.id)                              AS line_count,
       COALESCE(SUM(l.quantity * l.unit_cost_at_save), 0)::numeric(14,4)  AS total_at_save,
       COALESCE(SUM(l.quantity * i.default_unit_cost), 0)::numeric(14,4)  AS total_current
FROM material_estimate_templates t
LEFT JOIN material_estimate_template_lines l ON l.template_id = t.id
LEFT JOIN inventory_items i                   ON i.id = l.item_id
GROUP BY t.id, t.airtable_id;

-- `Total Items` (a count) and the order's money.
--
-- ⚠ `line_total` is RECOMPUTED from quantity × unit cost, while
-- `line_total_stored` carries whatever Airtable held. They should agree; where
-- they don't, Airtable's stored value was written once and never revisited
-- after an edit. Keeping both means the disagreement is visible rather than
-- inherited — the same reasoning that kept the Stock Levels cache out of Neon.
CREATE OR REPLACE VIEW v_material_order_totals AS
SELECT o.id                                     AS order_id,
       o.airtable_id                            AS order_airtable_id,
       COUNT(l.id)                              AS line_count,
       COUNT(l.id) FILTER (WHERE l.received)    AS received_count,
       COALESCE(SUM(l.quantity_ordered * l.unit_cost_at_order), 0)::numeric(14,4) AS total,
       COALESCE(SUM(l.line_total_stored), 0)::numeric(14,4)                       AS total_stored
FROM material_orders o
LEFT JOIN material_order_lines l ON l.order_id = o.id
GROUP BY o.id, o.airtable_id;

COMMIT;
