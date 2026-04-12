"""
Metric computation for one (trade_date, quote_time) snapshot.

Takes pre-fetched DB data (atm, surface) and pre-computed VIX values,
returns a flat dict of all surface_metrics_core columns.
"""
from __future__ import annotations

import math
from calendar import monthrange
from datetime import date, timedelta

from .config import (
    CONVEXITY_TRIPLES,
    DELTA_TO_COORD,
    DELTA_TO_PD,
    SKEW_PAIRS,
    TARGET_DTES,
    TERM_RATIO_PAIRS,
    TERM_SLOPE_DELTAS,
    TERM_SLOPE_PAIRS,
)


# ---------------------------------------------------------------------------
# Calendar helpers
# ---------------------------------------------------------------------------

def _third_friday(year: int, month: int) -> date:
    """Return the third Friday of the given year/month."""
    first = date(year, month, 1)
    days_to_friday = (4 - first.weekday()) % 7   # 4 = Friday
    return first + timedelta(days=days_to_friday + 14)


def days_to_monthly_opex(trade_date: date) -> int:
    """Calendar days from trade_date to the next (or same-day) monthly OPEX."""
    opex = _third_friday(trade_date.year, trade_date.month)
    if opex < trade_date:
        # This month's OPEX has passed — advance to next month
        m = trade_date.month % 12 + 1
        y = trade_date.year + (1 if trade_date.month == 12 else 0)
        opex = _third_friday(y, m)
    return (opex - trade_date).days


# ---------------------------------------------------------------------------
# IV and strike lookups
# ---------------------------------------------------------------------------

def _get_iv(
    dte: int,
    label: str,
    atm: dict,
    surface: dict,
) -> float | None:
    """
    Return IV for (dte, delta_label).
    'atm' uses spx_atm.atm_iv; all others use spx_surface.iv.
    """
    if label == "atm":
        d = atm.get(dte)
        return d["atm_iv"] if d else None
    pd_int = DELTA_TO_PD[label]
    d = surface.get(dte, {}).get(pd_int)
    return d["iv"] if d else None


def _get_strike(
    dte: int,
    label: str,
    atm: dict,
    surface: dict,
) -> float | None:
    """
    Return strike for (dte, delta_label).
    'atm' uses spx_atm.atm_strike (= atm_forward); others use spx_surface.strike.
    """
    if label == "atm":
        d = atm.get(dte)
        return d["atm_strike"] if d else None
    pd_int = DELTA_TO_PD[label]
    d = surface.get(dte, {}).get(pd_int)
    return d["strike"] if d else None


# ---------------------------------------------------------------------------
# Individual metric formulas
# ---------------------------------------------------------------------------

def _skew_slope(
    dte: int,
    label_a: str,
    label_b: str,
    atm: dict,
    surface: dict,
) -> float | None:
    """sqrt(DTE/365) * (IV_b - IV_a) / ln(K_b / K_a)"""
    iv_a = _get_iv(dte, label_a, atm, surface)
    iv_b = _get_iv(dte, label_b, atm, surface)
    k_a  = _get_strike(dte, label_a, atm, surface)
    k_b  = _get_strike(dte, label_b, atm, surface)

    if any(v is None for v in (iv_a, iv_b, k_a, k_b)):
        return None
    if k_a <= 0 or k_b <= 0:
        return None

    ln_ratio = math.log(k_b / k_a)
    if abs(ln_ratio) < 1e-10:
        return None

    return math.sqrt(dte / 365.0) * (iv_b - iv_a) / ln_ratio


def _term_slope(
    dte_a: int,
    dte_b: int,
    label: str,
    atm: dict,
    surface: dict,
) -> float | None:
    """
    Annualized forward vol between two tenors:
        sqrt((IV_b^2 * T_b - IV_a^2 * T_a) / (T_b - T_a))
    Returns None when forward variance is negative (calendar arb).
    """
    T_a  = dte_a / 365.0
    T_b  = dte_b / 365.0
    iv_a = _get_iv(dte_a, label, atm, surface)
    iv_b = _get_iv(dte_b, label, atm, surface)

    if iv_a is None or iv_b is None:
        return None

    fwd_var = (iv_b ** 2 * T_b - iv_a ** 2 * T_a) / (T_b - T_a)
    return math.sqrt(fwd_var) if fwd_var > 0 else None


def _convexity(
    dte: int,
    label_left: str,
    label_center: str,
    label_right: str,
    atm: dict,
    surface: dict,
) -> float | None:
    """
    Weighted curvature of the smile:
        (w_left * IV_left + w_right * IV_right) - IV_center
    Weights are delta-interpolated so the formula stays correct when
    wings are not evenly spaced in delta space.
    """
    iv_l = _get_iv(dte, label_left,   atm, surface)
    iv_c = _get_iv(dte, label_center, atm, surface)
    iv_r = _get_iv(dte, label_right,  atm, surface)

    if any(v is None for v in (iv_l, iv_c, iv_r)):
        return None

    d_l = DELTA_TO_COORD[label_left]
    d_c = DELTA_TO_COORD[label_center]
    d_r = DELTA_TO_COORD[label_right]
    span = d_r - d_l
    if span == 0:
        return None

    w_l = (d_r - d_c) / span
    w_r = (d_c - d_l) / span
    return (w_l * iv_l + w_r * iv_r) - iv_c


# ---------------------------------------------------------------------------
# Main snapshot computation
# ---------------------------------------------------------------------------

def compute_snapshot_metrics(
    trade_date: date,
    quote_time,
    atm: dict,
    surface: dict,
    vix: dict,
) -> dict:
    """
    Compute all surface_metrics_core columns for one (trade_date, quote_time).

    Parameters
    ----------
    trade_date  : datetime.date
    quote_time  : datetime.time
    atm         : { dte: { atm_iv, atm_forward, atm_strike, underlying_price } }
    surface     : { dte: { put_delta_int: { iv, strike } } }
    vix         : { dte: float_or_None }

    Returns
    -------
    Flat dict ready for INSERT into surface_metrics_core.
    """
    row: dict = {
        "trade_date":           trade_date.isoformat(),
        "quote_time":           quote_time,
        "day_of_week":          trade_date.weekday(),          # 0=Mon … 4=Fri
        "days_to_monthly_opex": days_to_monthly_opex(trade_date),
    }

    # ------------------------------------------------------------------ spot
    spot = None
    for dte in TARGET_DTES:
        d = atm.get(dte)
        if d and d.get("underlying_price") is not None:
            spot = d["underlying_price"]
            break
    row["spot"] = spot

    # -------------------------------------------------------- forward by DTE
    for dte in TARGET_DTES:
        d   = atm.get(dte)
        col = f"forward_{dte}d"
        row[col] = d["atm_forward"] if d else None

    # ------------------------------------------------------------ IV matrix
    for dte in TARGET_DTES:
        row[f"iv_{dte}d_25p"] = _get_iv(dte, "25p", atm, surface)
        row[f"iv_{dte}d_atm"] = _get_iv(dte, "atm", atm, surface)
        row[f"iv_{dte}d_25c"] = _get_iv(dte, "25c", atm, surface)

    # -------------------------------------------------------------------VIX
    for dte in TARGET_DTES:
        row[f"vix_{dte}d"] = vix.get(dte)

    # -----------------------------------------------------------term ratios
    for dte_a, dte_b in TERM_RATIO_PAIRS:
        iv_a = _get_iv(dte_a, "atm", atm, surface)
        iv_b = _get_iv(dte_b, "atm", atm, surface)
        col  = f"term_ratio_{dte_a}d_{dte_b}d"
        if iv_a is not None and iv_b is not None and iv_b != 0:
            row[col] = iv_a / iv_b
        else:
            row[col] = None

    # ---------------------------------------------------------skew slopes
    for dte in TARGET_DTES:
        for la, lb in SKEW_PAIRS:
            col      = f"skew_{dte}d_{la}_{lb}"
            row[col] = _skew_slope(dte, la, lb, atm, surface)

    # ---------------------------------------------------------- term slope
    for dte_a, dte_b in TERM_SLOPE_PAIRS:
        for delta_lbl in TERM_SLOPE_DELTAS:
            col      = f"term_slope_{dte_a}_{dte_b}_{delta_lbl}"
            row[col] = _term_slope(dte_a, dte_b, delta_lbl, atm, surface)

    # ----------------------------------------------------------- convexity
    for dte in TARGET_DTES:
        for ll, lc, lr in CONVEXITY_TRIPLES:
            col      = f"convex_{dte}d_{ll}_{lc}_{lr}"
            row[col] = _convexity(dte, ll, lc, lr, atm, surface)

    return row
