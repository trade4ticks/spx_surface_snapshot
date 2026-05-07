-- =============================================================================
-- Wave 2 — Cross-column derived metrics (24 columns)
-- All columns are computed from per-day spx_atm + spx_surface fetches.
-- No parquet, no CBOE variance — cheap to backfill via surgical UPDATE.
-- Idempotent: ADD COLUMN IF NOT EXISTS — safe to re-run.
-- =============================================================================

-- ─── 10-delta wing IVs (extracted from spx_surface put_delta=10/90) ──────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS iv_1d_10p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS iv_1d_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS iv_7d_10p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS iv_7d_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS iv_30d_10p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS iv_30d_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS iv_90d_10p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS iv_90d_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS iv_180d_10p DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS iv_180d_10c DOUBLE PRECISION;

-- ─── Symmetric 10-delta convexity ────────────────────────────────────────────
-- convex = 0.5*iv_10p + 0.5*iv_10c - iv_atm
-- Adds ("10p","atm","10c") to the existing CONVEXITY_TRIPLES.
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS convex_1d_10p_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS convex_7d_10p_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS convex_30d_10p_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS convex_90d_10p_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS convex_180d_10p_atm_10c DOUBLE PRECISION;

-- ─── Risk reversal (call IV − put IV at the same delta) ─────────────────────
-- rr_<dte>d_25 = iv_<dte>d_25c - iv_<dte>d_25p
-- rr_<dte>d_10 = iv_<dte>d_10c - iv_<dte>d_10p
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS rr_1d_25   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS rr_7d_25   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS rr_30d_25  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS rr_90d_25  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS rr_180d_25 DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS rr_30d_10  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS rr_90d_10  DOUBLE PRECISION;

-- ─── VIX basis (term-structure absolute spread) ──────────────────────────────
-- vix_basis_<a>_<b> = vix_<a>d - vix_<b>d
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS vix_basis_1d_30d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS vix_basis_30d_90d DOUBLE PRECISION;
