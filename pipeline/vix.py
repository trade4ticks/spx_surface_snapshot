"""
VIX calculation using the CBOE variance swap methodology.

For each target DTE, finds the two expiries that bracket it in time,
applies the CBOE variance formula to each expiry's OTM options, then
interpolates in total-variance space to get the implied vol at the
target tenor.

Result is stored as a decimal (0.20 = 20%), consistent with IV convention
used in spx_surface and spx_atm.

CBOE variance formula (per expiry):
    sigma^2 = (2/T) * sum_i[ DK_i / K_i^2 * e^(rT) * Q(K_i) ]
              - (1/T) * (F/K0 - 1)^2

where:
    Q(K_i)  = bid-ask midpoint of the OTM option at K_i
    DK_i    = strike width: (K_{i+1} - K_{i-1}) / 2 for interior strikes,
              adjacent interval width for edge strikes
    K0      = first strike <= F
    F       = forward price (estimated via put-call parity)
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, time
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from scipy.stats import linregress

from .config import (
    AM_EXPIRY_HOUR, AM_EXPIRY_MINUTE,
    MINUTES_PER_YEAR,
    MIN_OPTION_PRICE,
    PARQUET_COLS,
    PCP_MONEYNESS_BAND,
    PM_EXPIRY_HOUR, PM_EXPIRY_MINUTE,
    R_DEFAULT, R_MAX, R_MIN,
    STEP2_FLAG_COLS,
    TARGET_DTES,
)

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Time to expiry
# ---------------------------------------------------------------------------

def _compute_T(snapshot_ts: pd.Timestamp, expiry_date, is_am: bool) -> float:
    """Return T = calendar minutes to expiry / MINUTES_PER_YEAR."""
    exp_time = (
        time(AM_EXPIRY_HOUR, AM_EXPIRY_MINUTE) if is_am
        else time(PM_EXPIRY_HOUR, PM_EXPIRY_MINUTE)
    )
    expiry_naive = datetime.combine(expiry_date, exp_time)

    if getattr(snapshot_ts, "tzinfo", None) is not None:
        snap_naive = snapshot_ts.tz_convert(_ET).replace(tzinfo=None)
    else:
        snap_naive = snapshot_ts.to_pydatetime().replace(tzinfo=None)

    minutes = (expiry_naive - snap_naive).total_seconds() / 60.0
    return max(minutes, 0.0) / MINUTES_PER_YEAR


# ---------------------------------------------------------------------------
# Forward price estimation via put-call parity
# ---------------------------------------------------------------------------

def _estimate_forward(df: pd.DataFrame, T: float) -> tuple[float, float]:
    """
    Estimate (F, r) from put-call parity OLS on near-ATM strikes.
    df must have columns: strike, option_type ('c'/'p'), bid, ask.
    Raises ValueError on failure.
    """
    df = df.copy()
    df["_mid"] = (df["bid"] + df["ask"]) / 2.0

    calls = (
        df[df["option_type"] == "c"]
        .set_index("strike")[["_mid"]]
        .rename(columns={"_mid": "call_mid"})
    )
    puts = (
        df[df["option_type"] == "p"]
        .set_index("strike")[["_mid"]]
        .rename(columns={"_mid": "put_mid"})
    )
    pairs = calls.join(puts, how="inner").dropna()
    pairs = pairs[
        (pairs["call_mid"] >= MIN_OPTION_PRICE) &
        (pairs["put_mid"]  >= MIN_OPTION_PRICE)
    ]

    if len(pairs) < 3:
        raise ValueError(f"Only {len(pairs)} put-call pairs")

    K = pairs.index.to_numpy(dtype=float)
    y = (pairs["call_mid"] - pairs["put_mid"]).to_numpy(dtype=float)

    atm_guess = K[np.argmin(np.abs(y))]
    band_mask = np.abs(K / atm_guess - 1.0) <= PCP_MONEYNESS_BAND
    if band_mask.sum() < 3:
        band_mask = np.ones(len(K), dtype=bool)

    res = linregress(K[band_mask], y[band_mask])
    B   = -res.slope
    A   = res.intercept

    if B <= 0:
        raise ValueError(f"Non-positive discount factor B={B:.6f}")

    r = -np.log(B) / T
    F = A / B

    if F <= 0 or not (R_MIN <= r <= R_MAX):
        raise ValueError(f"Implausible PCP result F={F:.2f} r={r:.4f}")

    return float(F), float(r)


# ---------------------------------------------------------------------------
# CBOE variance formula
# ---------------------------------------------------------------------------

def _cboe_variance(df: pd.DataFrame, T: float, F: float, r: float) -> float | None:
    """
    Apply CBOE variance swap formula to OTM options for one expiry.

    df: columns [strike, option_type ('c'/'p'), bid, ask]
    Returns annualized variance sigma^2, or None if insufficient data.
    """
    df = df.copy()
    df["_mid"] = (df["bid"] + df["ask"]) / 2.0

    # Keep only options with positive bid (non-zero quotes)
    df = df[df["bid"] > 0].copy()

    if df.empty:
        return None

    # K0 = first strike <= F
    put_strikes = sorted(df[df["option_type"] == "p"]["strike"].unique())
    if not put_strikes:
        return None
    K0 = max(k for k in put_strikes if k <= F) if any(k <= F for k in put_strikes) else put_strikes[0]

    # Separate OTM options
    otm_puts  = df[(df["option_type"] == "p") & (df["strike"] < K0)].copy()
    otm_calls = df[(df["option_type"] == "c") & (df["strike"] > K0)].copy()

    # K0 row: average put and call if both available, else whichever exists
    put_at_k0  = df[(df["option_type"] == "p") & (df["strike"] == K0)]
    call_at_k0 = df[(df["option_type"] == "c") & (df["strike"] == K0)]

    if not put_at_k0.empty and not call_at_k0.empty:
        k0_mid = (put_at_k0["_mid"].values[0] + call_at_k0["_mid"].values[0]) / 2.0
    elif not put_at_k0.empty:
        k0_mid = put_at_k0["_mid"].values[0]
    elif not call_at_k0.empty:
        k0_mid = call_at_k0["_mid"].values[0]
    else:
        return None

    # Aggregate to one row per strike (take mean mid if duplicates)
    def _agg(sub: pd.DataFrame) -> pd.DataFrame:
        return sub.groupby("strike")["_mid"].mean().reset_index()

    puts_agg  = _agg(otm_puts).sort_values("strike")
    calls_agg = _agg(otm_calls).sort_values("strike")
    k0_df     = pd.DataFrame({"strike": [K0], "_mid": [k0_mid]})

    # CBOE truncation: stop at first pair of consecutive zero-bid strikes
    # moving outward from ATM. Since we already filtered bid > 0, any
    # remaining zero mids come from averaged duplicates — treat as valid.
    # Build combined strike/mid array in ascending order
    combined = pd.concat([puts_agg, k0_df, calls_agg], ignore_index=True)
    combined = (
        combined
        .drop_duplicates("strike")
        .sort_values("strike")
        .reset_index(drop=True)
    )

    if len(combined) < 2:
        return None

    strikes = combined["strike"].to_numpy(dtype=float)
    mids    = combined["_mid"].to_numpy(dtype=float)
    n       = len(strikes)

    # Strike widths
    dk       = np.empty(n)
    dk[0]    = strikes[1] - strikes[0]
    dk[-1]   = strikes[-1] - strikes[-2]
    if n > 2:
        dk[1:-1] = (strikes[2:] - strikes[:-2]) / 2.0

    # Variance contributions: 2 * DK_i / K_i^2 * e^(rT) * Q_i
    disc    = math.exp(r * T)
    contrib = 2.0 * dk / (strikes ** 2) * disc * mids
    total   = contrib.sum() / T

    # Adjustment term for F not landing on a discrete strike
    adjustment = ((F / K0) - 1.0) ** 2 / T

    sigma2 = total - adjustment
    return float(sigma2) if sigma2 > 0 else None


# ---------------------------------------------------------------------------
# Per-expiry wrapper
# ---------------------------------------------------------------------------

def _variance_for_expiry(
    group: pd.DataFrame,
    snapshot_ts: pd.Timestamp,
    expiry_date,
    is_am: bool,
) -> tuple[float, float] | None:
    """
    Compute (T, sigma2) for one expiry group at one snapshot.
    Returns None on any failure.
    """
    try:
        T = _compute_T(snapshot_ts, expiry_date, is_am)
        if T <= 1e-6:
            return None

        # Normalise columns
        df = group.copy()
        col_map = {
            PARQUET_COLS["strike"]:           "strike",
            PARQUET_COLS["option_type"]:      "option_type",
            PARQUET_COLS["bid"]:              "bid",
            PARQUET_COLS["ask"]:              "ask",
            PARQUET_COLS["underlying_price"]: "underlying_price",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        df["option_type"] = df["option_type"].astype(str).str.strip().str.lower().str[0]

        # Drop step-2 flagged rows
        for flag in STEP2_FLAG_COLS:
            if flag in df.columns:
                df = df[~df[flag].astype(bool)]

        # Basic quality filter
        df = df[(df["bid"] > 0) & (df["ask"] > df["bid"])].copy()

        if len(df) < 6:
            return None

        # Estimate forward
        try:
            F, r = _estimate_forward(df, T)
        except ValueError:
            up_col = "underlying_price"
            if up_col in df.columns and not df[up_col].isna().all():
                S = float(df[up_col].median())
                r = R_DEFAULT
                F = S * math.exp(r * T)
            else:
                return None

        sigma2 = _cboe_variance(df, T, F, r)
        if sigma2 is None:
            return None

        return T, sigma2

    except Exception as exc:
        logger.debug("VIX expiry error: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Snapshot-level VIX computation
# ---------------------------------------------------------------------------

def compute_vix_for_snapshot(
    snap_df: pd.DataFrame,
    snapshot_ts: pd.Timestamp,
    target_dtes: list[int] = TARGET_DTES,
) -> dict[int, float | None]:
    """
    Compute VIX-style implied vol for each target DTE at a single snapshot.

    snap_df must have _expiry (Timestamp) and _session ('AM'/'PM') columns,
    and must already be filtered to the relevant quote_time.

    Uses the same PM-primary / AM-fallback expiry pool as the interpolation
    pipeline (AM expiry admitted only when no PM expiry exists on that date).

    Returns { dte: float_or_None } where floats are annualized vol decimals.
    """
    # Determine which expiry dates have a PM session
    pm_dates = {
        (exp.date() if hasattr(exp, "date") else exp)
        for (exp, ses), _ in snap_df.groupby(["_expiry", "_session"])
        if str(ses).upper() == "PM"
    }

    # Collect (T, sigma2) for each eligible expiry
    expiry_variances: list[tuple[float, float]] = []

    for (expiry_ts, session), group in snap_df.groupby(["_expiry", "_session"]):
        exp_date = expiry_ts.date() if hasattr(expiry_ts, "date") else expiry_ts
        is_am    = str(session).upper() == "AM"

        # Skip AM when a PM exists for the same expiry date
        if is_am and exp_date in pm_dates:
            continue

        result = _variance_for_expiry(group, snapshot_ts, exp_date, is_am)
        if result is not None:
            expiry_variances.append(result)

    if not expiry_variances:
        return {dte: None for dte in target_dtes}

    # Sort by T ascending
    expiry_variances.sort(key=lambda x: x[0])

    vix_out: dict[int, float | None] = {}
    for target_dte in target_dtes:
        T_target = target_dte / 365.0

        # Find bracketing pair
        lo: tuple[float, float] | None = None
        hi: tuple[float, float] | None = None
        for i in range(len(expiry_variances) - 1):
            if expiry_variances[i][0] <= T_target <= expiry_variances[i + 1][0]:
                lo = expiry_variances[i]
                hi = expiry_variances[i + 1]
                break

        if lo is not None and hi is not None:
            T1, s2_1 = lo
            T2, s2_2 = hi
            # Interpolate in total-variance space
            alpha     = (T_target - T1) / (T2 - T1)
            tv_target = s2_1 * T1 + alpha * (s2_2 * T2 - s2_1 * T1)
            vix_out[target_dte] = math.sqrt(tv_target / T_target) if tv_target > 0 else None
        else:
            # No bracket — use nearest single expiry (handles DTE=1 edge cases)
            nearest = min(expiry_variances, key=lambda x: abs(x[0] - T_target))
            T_n, s2_n = nearest
            vix_out[target_dte] = math.sqrt(s2_n) if s2_n > 0 else None

    return vix_out


# ---------------------------------------------------------------------------
# Parquet loading helpers (shared with backfill)
# ---------------------------------------------------------------------------

def load_parquet_day(data_roots, trade_date) -> pd.DataFrame:
    """
    Load all parquet files for trade_date, tagging each row with
    _expiry (Timestamp) and _session ('AM'/'PM').

    Searches each directory in data_roots for the trade_date folder.
    Returns an empty DataFrame if no files found.
    """
    from datetime import datetime as dt
    from pathlib import Path

    if not isinstance(data_roots, (list, tuple)):
        data_roots = [data_roots]

    trade_dir = None
    for root in data_roots:
        candidate = Path(root) / trade_date.strftime("%Y%m%d")
        if candidate.is_dir():
            trade_dir = candidate
            break
    if trade_dir is None:
        return pd.DataFrame()

    frames = []
    for expiry_dir in sorted(trade_dir.iterdir()):
        if not expiry_dir.is_dir():
            continue
        try:
            expiry = dt.strptime(expiry_dir.name, "%Y%m%d").date()
        except ValueError:
            continue

        for session in ("PM", "AM"):
            path = expiry_dir / f"{session}.parquet"
            if not path.exists():
                continue
            try:
                df = pd.read_parquet(path)
            except Exception as exc:
                logger.warning("Failed to read %s: %s", path, exc)
                continue

            df["_expiry"] = pd.Timestamp(expiry)

            # Prefer the in-file settlement column if present
            settlement_col = PARQUET_COLS.get("settlement", "settlement")
            if settlement_col in df.columns:
                df["_session"] = (
                    df[settlement_col].astype(str).str.upper().str.strip()
                )
            else:
                df["_session"] = session

            frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Build full datetime _ts from the split trade_date + quote_time columns
    date_col = PARQUET_COLS["trade_date"]
    time_col = PARQUET_COLS["quote_time"]
    combined["_ts"] = pd.to_datetime(
        combined[date_col].astype(str) + " " + combined[time_col].astype(str)
    )
    return combined
