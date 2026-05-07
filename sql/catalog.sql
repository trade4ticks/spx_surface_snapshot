-- =============================================================================
-- surface_metrics_catalog — one row per column of surface_metrics_core.
-- Drives dashboard dropdowns, is the source-of-truth column inventory for
-- LLM exploration, and documents formulas/units in-database.
-- Populated by scripts/build_catalog.py — re-run any time the schema changes.
-- =============================================================================

CREATE TABLE IF NOT EXISTS surface_metrics_catalog (
    column_name   TEXT PRIMARY KEY,
    family        TEXT NOT NULL,            -- iv, vix, skew, rr, rv, vrp, ...
    tenor         TEXT,                     -- 30d, 1m, 30d_90d, ...
    wing          TEXT,                     -- atm, 25p_25c, 10p_atm_10c, ...
    form          TEXT NOT NULL,            -- level, chg_d, chg_1w, z
    base_column   TEXT,                     -- for transforms: source column
    units         TEXT,                     -- vol_decimal, log_return, corr, ratio, ...
    description   TEXT,
    formula       TEXT
);

CREATE INDEX IF NOT EXISTS idx_catalog_family ON surface_metrics_catalog (family);
CREATE INDEX IF NOT EXISTS idx_catalog_form   ON surface_metrics_catalog (form);
CREATE INDEX IF NOT EXISTS idx_catalog_tenor  ON surface_metrics_catalog (tenor);
