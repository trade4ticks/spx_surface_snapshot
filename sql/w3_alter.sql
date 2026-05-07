-- =============================================================================
-- Wave 3 — Time-series base metrics (15 columns)
-- Require a daily SPX close series (sourced from spx_atm.underlying_price at
-- the last quote_time of each trade_date) plus history of W1+W2 IV columns.
--
-- Convention: "_Nd" is calendar tenor (option-pricing).  "_d/1w/1m/3m" are
-- trading-day windows annualized with sqrt(252).  Snapshot-aligned: every
-- trailing window pulls values at the SAME quote_time on prior trading days.
-- =============================================================================

-- ─── Spot log returns (snapshot-aligned trading-day lookback) ────────────────
-- log_ret_d  = log(spot_t / spot_{t-1 trading day,  same quote_time})
-- log_ret_1w = log(spot_t / spot_{t-5 trading days, same quote_time})
-- log_ret_1m = log(spot_t / spot_{t-21 trading days, same quote_time})
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS log_ret_d  DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS log_ret_1w DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS log_ret_1m DOUBLE PRECISION;

-- ─── Realized vol (trailing trading-day stdev of daily log returns × √252) ──
-- rv_1w = stdev of last 5  daily close-to-close log returns × sqrt(252)
-- rv_1m =                 21 trading days
-- rv_3m =                 63 trading days
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS rv_1w DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS rv_1m DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS rv_3m DOUBLE PRECISION;

-- ─── Volatility risk premium (IV − RV at matched horizons) ──────────────────
-- vrp_1w = iv_7d_atm   - rv_1w   (5td ≈ 7 calendar days)
-- vrp_1m = iv_30d_atm  - rv_1m   (21td ≈ 30 calendar days)
-- vrp_3m = iv_90d_atm  - rv_3m   (63td ≈ 90 calendar days)
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS vrp_1w DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS vrp_1m DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS vrp_3m DOUBLE PRECISION;

-- ─── VRP ratio (IV / RV) — note: explodes when RV is small ──────────────────
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS vrp_ratio_1w DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS vrp_ratio_1m DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS vrp_ratio_3m DOUBLE PRECISION;

-- ─── Vol of vol — trailing stdev of daily Δ ATM IV at the chosen tenor ──────
-- vov_<iv_tenor>_<window> = stdev of last N daily Δ(iv_<tenor>_atm)
-- vov_30d_1m = 21-trading-day stdev of d_iv_30d_atm
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS vov_30d_1m DOUBLE PRECISION;

-- ─── Spot–vol correlation — rolling corr(log_ret_d, Δ ATM IV) ───────────────
-- spot_vol_<iv_tenor>_<window>
-- spot_vol_30d_1m = 21td corr( log_ret_d, d_iv_30d_atm )
-- spot_vol_30d_3m = 63td corr( log_ret_d, d_iv_30d_atm )
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS spot_vol_30d_1m DOUBLE PRECISION;
ALTER TABLE surface_metrics_core ADD COLUMN IF NOT EXISTS spot_vol_30d_3m DOUBLE PRECISION;
