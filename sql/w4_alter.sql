-- =============================================================================
-- Wave 4 — Transforms (336 columns)
--
-- Three transformations applied to eligible W1/W2/W3 base metrics:
--   chg_d_<base>  = base(today, T) - base(prior trading day, T)
--   chg_1w_<base> = base(today, T) - base(5 trading days back, T)
--   z_<base>      = (base(today, T) - μ) / σ over trailing 63 trading days
--                   at the same quote_time (no pooling across times of day)
--
-- Snapshot-aligned: lookbacks always pull the same quote_time.
-- z_ has no window suffix — there's only one window (63td / ~3m).
--
-- Skip rules:
--   * No transforms on calendar/PK fields, spot, forward_*d
--     (non-stationary; log_ret_* covers the spot dynamics)
--   * Skip chg_d/chg_1w on log_ret_*, vov_*, spot_vol_* (already changes/MAs)
--     — keep z_ on those
-- =============================================================================


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║ chg_d — daily change (1 trading day, snapshot-aligned)                    ║
-- ║ 110 columns                                                                ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ── W1: IV matrix (15) ──────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_1d_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_1d_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_1d_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_7d_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_7d_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_7d_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_30d_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_30d_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_30d_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_90d_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_90d_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_90d_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_180d_25p DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_180d_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_180d_25c DOUBLE PRECISION;

-- ── W1: VIX (5) ─────────────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_vix_1d   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_vix_7d   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_vix_30d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_vix_90d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_vix_180d DOUBLE PRECISION;

-- ── W1: term ratios (3) ─────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_term_ratio_1d_7d   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_term_ratio_7d_30d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_term_ratio_30d_90d DOUBLE PRECISION;

-- ── W1: skew slopes (30) ────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_1d_10p_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_1d_25p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_1d_10p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_1d_atm_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_1d_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_1d_25p_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_7d_10p_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_7d_25p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_7d_10p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_7d_atm_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_7d_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_7d_25p_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_30d_10p_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_30d_25p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_30d_10p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_30d_atm_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_30d_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_30d_25p_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_90d_10p_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_90d_25p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_90d_10p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_90d_atm_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_90d_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_90d_25p_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_180d_10p_25p DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_180d_25p_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_180d_10p_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_180d_atm_25c DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_180d_atm_10c DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_skew_180d_25p_25c DOUBLE PRECISION;

-- ── W1: term slopes (9) ─────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_term_slope_1_7_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_term_slope_1_7_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_term_slope_1_7_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_term_slope_7_30_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_term_slope_7_30_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_term_slope_7_30_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_term_slope_30_90_25p DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_term_slope_30_90_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_term_slope_30_90_25c DOUBLE PRECISION;

-- ── W1: convexity (15) ──────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_1d_10p_25p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_1d_atm_25c_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_1d_25p_atm_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_7d_10p_25p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_7d_atm_25c_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_7d_25p_atm_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_30d_10p_25p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_30d_atm_25c_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_30d_25p_atm_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_90d_10p_25p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_90d_atm_25c_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_90d_25p_atm_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_180d_10p_25p_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_180d_atm_25c_10c DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_180d_25p_atm_25c DOUBLE PRECISION;

-- ── W2: 10-delta wing IVs (10) ──────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_1d_10p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_1d_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_7d_10p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_7d_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_30d_10p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_30d_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_90d_10p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_90d_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_180d_10p DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_iv_180d_10c DOUBLE PRECISION;

-- ── W2: 10-delta convexity (5) ──────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_1d_10p_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_7d_10p_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_30d_10p_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_90d_10p_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_convex_180d_10p_atm_10c DOUBLE PRECISION;

-- ── W2: risk reversal (7) ───────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_rr_1d_25   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_rr_7d_25   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_rr_30d_25  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_rr_90d_25  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_rr_180d_25 DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_rr_30d_10  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_rr_90d_10  DOUBLE PRECISION;

-- ── W2: VIX basis (2) ───────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_vix_basis_1d_30d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_vix_basis_30d_90d DOUBLE PRECISION;

-- ── W3: RV / VRP / VRP ratio (9) ────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_rv_1w        DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_rv_1m        DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_rv_3m        DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_vrp_1w       DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_vrp_1m       DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_vrp_3m       DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_vrp_ratio_1w DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_vrp_ratio_1m DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_d_vrp_ratio_3m DOUBLE PRECISION;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║ chg_1w — weekly change (5 trading days, snapshot-aligned)                 ║
-- ║ 110 columns                                                                ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ── W1: IV matrix (15) ──────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_1d_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_1d_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_1d_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_7d_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_7d_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_7d_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_30d_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_30d_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_30d_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_90d_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_90d_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_90d_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_180d_25p DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_180d_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_180d_25c DOUBLE PRECISION;

-- ── W1: VIX (5) ─────────────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_vix_1d   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_vix_7d   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_vix_30d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_vix_90d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_vix_180d DOUBLE PRECISION;

-- ── W1: term ratios (3) ─────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_term_ratio_1d_7d   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_term_ratio_7d_30d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_term_ratio_30d_90d DOUBLE PRECISION;

-- ── W1: skew slopes (30) ────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_1d_10p_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_1d_25p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_1d_10p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_1d_atm_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_1d_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_1d_25p_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_7d_10p_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_7d_25p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_7d_10p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_7d_atm_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_7d_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_7d_25p_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_30d_10p_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_30d_25p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_30d_10p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_30d_atm_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_30d_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_30d_25p_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_90d_10p_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_90d_25p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_90d_10p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_90d_atm_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_90d_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_90d_25p_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_180d_10p_25p DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_180d_25p_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_180d_10p_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_180d_atm_25c DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_180d_atm_10c DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_skew_180d_25p_25c DOUBLE PRECISION;

-- ── W1: term slopes (9) ─────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_term_slope_1_7_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_term_slope_1_7_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_term_slope_1_7_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_term_slope_7_30_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_term_slope_7_30_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_term_slope_7_30_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_term_slope_30_90_25p DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_term_slope_30_90_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_term_slope_30_90_25c DOUBLE PRECISION;

-- ── W1: convexity (15) ──────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_1d_10p_25p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_1d_atm_25c_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_1d_25p_atm_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_7d_10p_25p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_7d_atm_25c_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_7d_25p_atm_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_30d_10p_25p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_30d_atm_25c_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_30d_25p_atm_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_90d_10p_25p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_90d_atm_25c_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_90d_25p_atm_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_180d_10p_25p_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_180d_atm_25c_10c DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_180d_25p_atm_25c DOUBLE PRECISION;

-- ── W2: 10-delta wing IVs (10) ──────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_1d_10p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_1d_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_7d_10p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_7d_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_30d_10p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_30d_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_90d_10p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_90d_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_180d_10p DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_iv_180d_10c DOUBLE PRECISION;

-- ── W2: 10-delta convexity (5) ──────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_1d_10p_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_7d_10p_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_30d_10p_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_90d_10p_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_convex_180d_10p_atm_10c DOUBLE PRECISION;

-- ── W2: risk reversal (7) ───────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_rr_1d_25   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_rr_7d_25   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_rr_30d_25  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_rr_90d_25  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_rr_180d_25 DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_rr_30d_10  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_rr_90d_10  DOUBLE PRECISION;

-- ── W2: VIX basis (2) ───────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_vix_basis_1d_30d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_vix_basis_30d_90d DOUBLE PRECISION;

-- ── W3: RV / VRP / VRP ratio (9) ────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_rv_1w        DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_rv_1m        DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_rv_3m        DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_vrp_1w       DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_vrp_1m       DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_vrp_3m       DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_vrp_ratio_1w DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_vrp_ratio_1m DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS chg_1w_vrp_ratio_3m DOUBLE PRECISION;


-- ╔═══════════════════════════════════════════════════════════════════════════╗
-- ║ z — 63-trading-day rolling z-score, snapshot-aligned                      ║
-- ║ 116 columns                                                                ║
-- ╚═══════════════════════════════════════════════════════════════════════════╝

-- ── W1: IV matrix (15) ──────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_1d_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_1d_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_1d_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_7d_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_7d_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_7d_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_30d_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_30d_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_30d_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_90d_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_90d_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_90d_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_180d_25p DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_180d_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_180d_25c DOUBLE PRECISION;

-- ── W1: VIX (5) ─────────────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vix_1d   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vix_7d   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vix_30d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vix_90d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vix_180d DOUBLE PRECISION;

-- ── W1: term ratios (3) ─────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_term_ratio_1d_7d   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_term_ratio_7d_30d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_term_ratio_30d_90d DOUBLE PRECISION;

-- ── W1: skew slopes (30) ────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_1d_10p_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_1d_25p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_1d_10p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_1d_atm_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_1d_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_1d_25p_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_7d_10p_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_7d_25p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_7d_10p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_7d_atm_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_7d_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_7d_25p_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_30d_10p_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_30d_25p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_30d_10p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_30d_atm_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_30d_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_30d_25p_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_90d_10p_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_90d_25p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_90d_10p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_90d_atm_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_90d_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_90d_25p_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_180d_10p_25p DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_180d_25p_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_180d_10p_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_180d_atm_25c DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_180d_atm_10c DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_skew_180d_25p_25c DOUBLE PRECISION;

-- ── W1: term slopes (9) ─────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_term_slope_1_7_25p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_term_slope_1_7_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_term_slope_1_7_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_term_slope_7_30_25p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_term_slope_7_30_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_term_slope_7_30_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_term_slope_30_90_25p DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_term_slope_30_90_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_term_slope_30_90_25c DOUBLE PRECISION;

-- ── W1: convexity (15) ──────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_1d_10p_25p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_1d_atm_25c_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_1d_25p_atm_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_7d_10p_25p_atm   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_7d_atm_25c_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_7d_25p_atm_25c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_30d_10p_25p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_30d_atm_25c_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_30d_25p_atm_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_90d_10p_25p_atm  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_90d_atm_25c_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_90d_25p_atm_25c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_180d_10p_25p_atm DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_180d_atm_25c_10c DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_180d_25p_atm_25c DOUBLE PRECISION;

-- ── W2: 10-delta wing IVs (10) ──────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_1d_10p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_1d_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_7d_10p   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_7d_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_30d_10p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_30d_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_90d_10p  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_90d_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_180d_10p DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_iv_180d_10c DOUBLE PRECISION;

-- ── W2: 10-delta convexity (5) ──────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_1d_10p_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_7d_10p_atm_10c   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_30d_10p_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_90d_10p_atm_10c  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_convex_180d_10p_atm_10c DOUBLE PRECISION;

-- ── W2: risk reversal (7) ───────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_rr_1d_25   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_rr_7d_25   DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_rr_30d_25  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_rr_90d_25  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_rr_180d_25 DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_rr_30d_10  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_rr_90d_10  DOUBLE PRECISION;

-- ── W2: VIX basis (2) ───────────────────────────────────────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vix_basis_1d_30d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vix_basis_30d_90d DOUBLE PRECISION;

-- ── W3: RV / VRP / VRP ratio (9) — same bases as chg ───────────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_rv_1w        DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_rv_1m        DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_rv_3m        DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vrp_1w       DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vrp_1m       DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vrp_3m       DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vrp_ratio_1w DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vrp_ratio_1m DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vrp_ratio_3m DOUBLE PRECISION;

-- ── W3 z-only: log_ret / vov / spot_vol (6) ─────────────────────────────────
-- These ARE already changes/MAs — chg_d/chg_1w skipped, z is the only transform.
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_log_ret_d       DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_log_ret_1w      DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_log_ret_1m      DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_vov_30d_1m      DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_spot_vol_30d_1m DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS z_spot_vol_30d_3m DOUBLE PRECISION;
