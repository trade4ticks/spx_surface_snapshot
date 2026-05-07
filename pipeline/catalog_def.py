"""
Shared catalog / metric-set definitions.

Single source of truth for:
  - The complete list of base (level) columns in surface_metrics_core
    (W1 + W2 + W3 — generated from the same config lists that drive compute)
  - Which columns receive which W4 transforms (chg_d, chg_1w, z)

Used by:
  - scripts/build_catalog.py — adds human-readable descriptions/formulas
  - scripts/backfill_w4.py    — drives the transform computation

If a base column is added/removed, update base_column_names() here and
the corresponding ALTER scripts under sql/. Everything else flows from this.
"""
from __future__ import annotations

from .config import (
    CONVEXITY_TRIPLES,
    IV_MATRIX_DELTAS,
    RISK_REVERSAL_SPECS,
    SKEW_PAIRS,
    TARGET_DTES,
    TERM_RATIO_PAIRS,
    TERM_SLOPE_DELTAS,
    TERM_SLOPE_PAIRS,
    VIX_BASIS_PAIRS,
)


# ---------------------------------------------------------------------------
# Skip rules — must match the column set in sql/w4_alter.sql
# ---------------------------------------------------------------------------

# Bases that get NO transforms (PK, calendar, non-stationary trend)
NO_TRANSFORM: frozenset[str] = frozenset({
    "trade_date", "quote_time",
    "day_of_week", "days_to_monthly_opex",
    "spot",
    "forward_1d", "forward_7d", "forward_30d", "forward_90d", "forward_180d",
})

# Bases that get z only (skip chg_d, chg_1w) — already changes / rolling MAs
Z_ONLY: frozenset[str] = frozenset({
    "log_ret_d", "log_ret_1w", "log_ret_1m",
    "vov_30d_1m",
    "spot_vol_30d_1m", "spot_vol_30d_3m",
})


# ---------------------------------------------------------------------------
# Base column enumeration
# ---------------------------------------------------------------------------

def base_column_names():
    """Yield every base (level) column of surface_metrics_core in declaration order."""
    # ── PK / calendar ────────────────────────────────────────────────
    yield "trade_date"
    yield "quote_time"
    yield "day_of_week"
    yield "days_to_monthly_opex"

    # ── Spot / forward ───────────────────────────────────────────────
    yield "spot"
    for dte in TARGET_DTES:
        yield f"forward_{dte}d"

    # ── IV matrix ────────────────────────────────────────────────────
    for dte in TARGET_DTES:
        for label in IV_MATRIX_DELTAS:
            yield f"iv_{dte}d_{label}"

    # ── VIX ──────────────────────────────────────────────────────────
    for dte in TARGET_DTES:
        yield f"vix_{dte}d"

    # ── Term ratios ─────────────────────────────────────────────────
    for a, b in TERM_RATIO_PAIRS:
        yield f"term_ratio_{a}d_{b}d"

    # ── Skew slopes ─────────────────────────────────────────────────
    for dte in TARGET_DTES:
        for la, lb in SKEW_PAIRS:
            yield f"skew_{dte}d_{la}_{lb}"

    # ── Term slopes ─────────────────────────────────────────────────
    for a, b in TERM_SLOPE_PAIRS:
        for delta in TERM_SLOPE_DELTAS:
            yield f"term_slope_{a}_{b}_{delta}"

    # ── Convexity ───────────────────────────────────────────────────
    for dte in TARGET_DTES:
        for ll, lc, lr in CONVEXITY_TRIPLES:
            yield f"convex_{dte}d_{ll}_{lc}_{lr}"

    # ── W2: Risk reversal ───────────────────────────────────────────
    for dte, delta in RISK_REVERSAL_SPECS:
        yield f"rr_{dte}d_{delta}"

    # ── W2: VIX basis ───────────────────────────────────────────────
    for a, b in VIX_BASIS_PAIRS:
        yield f"vix_basis_{a}d_{b}d"

    # ── W3: log returns ─────────────────────────────────────────────
    for period in ("d", "1w", "1m"):
        yield f"log_ret_{period}"

    # ── W3: realized vol ────────────────────────────────────────────
    for period in ("1w", "1m", "3m"):
        yield f"rv_{period}"

    # ── W3: VRP / VRP ratio ─────────────────────────────────────────
    for period in ("1w", "1m", "3m"):
        yield f"vrp_{period}"
    for period in ("1w", "1m", "3m"):
        yield f"vrp_ratio_{period}"

    # ── W3: vov + spot-vol corr ─────────────────────────────────────
    yield "vov_30d_1m"
    yield "spot_vol_30d_1m"
    yield "spot_vol_30d_3m"


def chg_eligible_bases() -> list[str]:
    """Bases that get chg_d AND chg_1w AND z transforms."""
    return [c for c in base_column_names()
            if c not in NO_TRANSFORM and c not in Z_ONLY]


def z_eligible_bases() -> list[str]:
    """Bases that get z transforms (chg-eligible plus Z_ONLY-only)."""
    return [c for c in base_column_names() if c not in NO_TRANSFORM]
