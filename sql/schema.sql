-- =============================================================================
-- Surface Metrics Core
-- One row per (trade_date, quote_time) snapshot.
-- Reads from spx_surface and spx_atm (interpolation pipeline output).
-- VIX columns use the CBOE variance swap formula applied to raw parquet data.
--
-- Delta convention follows spx_surface unified put delta:
--   10p = put_delta 10,  25p = put_delta 25
--   25c = put_delta 75,  10c = put_delta 90
--   atm = forward ATM from spx_atm (k=0, not put_delta=50)
--
-- Skew slope:  sqrt(DTE/365) * (IV_b - IV_a) / ln(K_b / K_a)
-- Term slope:  sqrt((IV_b^2 * T_b - IV_a^2 * T_a) / (T_b - T_a))  [forward vol]
-- Convexity:   (w_left * IV_left + w_right * IV_right) - IV_center
--              where weights are delta-interpolated
-- =============================================================================

CREATE TABLE IF NOT EXISTS surface_metrics_core (

    -- -------------------------------------------------------------------------
    -- Primary key
    -- -------------------------------------------------------------------------
    trade_date              DATE             NOT NULL,
    quote_time              TIME             NOT NULL,

    -- -------------------------------------------------------------------------
    -- Calendar context
    -- -------------------------------------------------------------------------
    day_of_week             SMALLINT,                    -- 0=Mon ... 4=Fri
    days_to_monthly_opex    SMALLINT,                    -- calendar days to 3rd Friday

    -- -------------------------------------------------------------------------
    -- Spot and forward
    -- -------------------------------------------------------------------------
    spot                    DOUBLE PRECISION,            -- underlying_price from spx_atm

    forward_1d              DOUBLE PRECISION,
    forward_7d              DOUBLE PRECISION,
    forward_30d             DOUBLE PRECISION,
    forward_90d             DOUBLE PRECISION,
    forward_180d            DOUBLE PRECISION,

    -- -------------------------------------------------------------------------
    -- IV matrix: iv_{dte}d_{delta}
    -- -------------------------------------------------------------------------
    iv_1d_25p               DOUBLE PRECISION,
    iv_1d_atm               DOUBLE PRECISION,
    iv_1d_25c               DOUBLE PRECISION,

    iv_7d_25p               DOUBLE PRECISION,
    iv_7d_atm               DOUBLE PRECISION,
    iv_7d_25c               DOUBLE PRECISION,

    iv_30d_25p              DOUBLE PRECISION,
    iv_30d_atm              DOUBLE PRECISION,
    iv_30d_25c              DOUBLE PRECISION,

    iv_90d_25p              DOUBLE PRECISION,
    iv_90d_atm              DOUBLE PRECISION,
    iv_90d_25c              DOUBLE PRECISION,

    iv_180d_25p             DOUBLE PRECISION,
    iv_180d_atm             DOUBLE PRECISION,
    iv_180d_25c             DOUBLE PRECISION,

    -- -------------------------------------------------------------------------
    -- VIX (CBOE variance swap formula applied to raw options, interpolated to DTE)
    -- Stored as decimal (0.20 = 20%), consistent with IV convention
    -- -------------------------------------------------------------------------
    vix_1d                  DOUBLE PRECISION,
    vix_7d                  DOUBLE PRECISION,
    vix_30d                 DOUBLE PRECISION,
    vix_90d                 DOUBLE PRECISION,
    vix_180d                DOUBLE PRECISION,

    -- -------------------------------------------------------------------------
    -- Term ratios: ATM iv_a / iv_b (raw IV ratio)
    -- -------------------------------------------------------------------------
    term_ratio_1d_7d        DOUBLE PRECISION,
    term_ratio_7d_30d       DOUBLE PRECISION,
    term_ratio_30d_90d      DOUBLE PRECISION,

    -- -------------------------------------------------------------------------
    -- Skew slopes: skew_{dte}d_{pair}
    -- -------------------------------------------------------------------------
    skew_1d_10p_25p         DOUBLE PRECISION,
    skew_1d_25p_atm         DOUBLE PRECISION,
    skew_1d_10p_atm         DOUBLE PRECISION,
    skew_1d_atm_25c         DOUBLE PRECISION,
    skew_1d_atm_10c         DOUBLE PRECISION,
    skew_1d_25p_25c         DOUBLE PRECISION,

    skew_7d_10p_25p         DOUBLE PRECISION,
    skew_7d_25p_atm         DOUBLE PRECISION,
    skew_7d_10p_atm         DOUBLE PRECISION,
    skew_7d_atm_25c         DOUBLE PRECISION,
    skew_7d_atm_10c         DOUBLE PRECISION,
    skew_7d_25p_25c         DOUBLE PRECISION,

    skew_30d_10p_25p        DOUBLE PRECISION,
    skew_30d_25p_atm        DOUBLE PRECISION,
    skew_30d_10p_atm        DOUBLE PRECISION,
    skew_30d_atm_25c        DOUBLE PRECISION,
    skew_30d_atm_10c        DOUBLE PRECISION,
    skew_30d_25p_25c        DOUBLE PRECISION,

    skew_90d_10p_25p        DOUBLE PRECISION,
    skew_90d_25p_atm        DOUBLE PRECISION,
    skew_90d_10p_atm        DOUBLE PRECISION,
    skew_90d_atm_25c        DOUBLE PRECISION,
    skew_90d_atm_10c        DOUBLE PRECISION,
    skew_90d_25p_25c        DOUBLE PRECISION,

    skew_180d_10p_25p       DOUBLE PRECISION,
    skew_180d_25p_atm       DOUBLE PRECISION,
    skew_180d_10p_atm       DOUBLE PRECISION,
    skew_180d_atm_25c       DOUBLE PRECISION,
    skew_180d_atm_10c       DOUBLE PRECISION,
    skew_180d_25p_25c       DOUBLE PRECISION,

    -- -------------------------------------------------------------------------
    -- Term slope (annualized forward vol): term_slope_{dte_a}_{dte_b}_{delta}
    -- -------------------------------------------------------------------------
    term_slope_1_7_25p      DOUBLE PRECISION,
    term_slope_1_7_atm      DOUBLE PRECISION,
    term_slope_1_7_25c      DOUBLE PRECISION,

    term_slope_7_30_25p     DOUBLE PRECISION,
    term_slope_7_30_atm     DOUBLE PRECISION,
    term_slope_7_30_25c     DOUBLE PRECISION,

    term_slope_30_90_25p    DOUBLE PRECISION,
    term_slope_30_90_atm    DOUBLE PRECISION,
    term_slope_30_90_25c    DOUBLE PRECISION,

    -- -------------------------------------------------------------------------
    -- Convexity: convex_{dte}d_{left}_{center}_{right}
    -- -------------------------------------------------------------------------
    convex_1d_10p_25p_atm   DOUBLE PRECISION,
    convex_1d_atm_25c_10c   DOUBLE PRECISION,
    convex_1d_25p_atm_25c   DOUBLE PRECISION,

    convex_7d_10p_25p_atm   DOUBLE PRECISION,
    convex_7d_atm_25c_10c   DOUBLE PRECISION,
    convex_7d_25p_atm_25c   DOUBLE PRECISION,

    convex_30d_10p_25p_atm  DOUBLE PRECISION,
    convex_30d_atm_25c_10c  DOUBLE PRECISION,
    convex_30d_25p_atm_25c  DOUBLE PRECISION,

    convex_90d_10p_25p_atm  DOUBLE PRECISION,
    convex_90d_atm_25c_10c  DOUBLE PRECISION,
    convex_90d_25p_atm_25c  DOUBLE PRECISION,

    convex_180d_10p_25p_atm DOUBLE PRECISION,
    convex_180d_atm_25c_10c DOUBLE PRECISION,
    convex_180d_25p_atm_25c DOUBLE PRECISION,

    PRIMARY KEY (trade_date, quote_time)
);

CREATE INDEX IF NOT EXISTS surface_metrics_core_date_idx
    ON surface_metrics_core (trade_date);
