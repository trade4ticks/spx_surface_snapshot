"""
PostgreSQL storage for surface_metrics_core.
"""
from __future__ import annotations

import logging
import math
from pathlib import Path

import numpy as np
import psycopg2
import psycopg2.extras

from .config import DB_URL

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).parent.parent / "sql"
_SCHEMA_FILES = ["schema.sql", "w2_alter.sql", "w3_alter.sql", "w4_alter.sql"]


# ---------------------------------------------------------------------------
# Scalar sanitisation (mirrors interpolation pipeline)
# ---------------------------------------------------------------------------

def _sanitize(row: dict) -> dict:
    """Convert numpy scalars to Python natives; map NaN/Inf to None."""
    result = {}
    for k, v in row.items():
        if isinstance(v, np.floating):
            f = float(v)
            result[k] = None if not math.isfinite(f) else f
        elif isinstance(v, np.integer):
            result[k] = int(v)
        elif isinstance(v, np.bool_):
            result[k] = bool(v)
        elif isinstance(v, float) and not math.isfinite(v):
            result[k] = None
        else:
            result[k] = v
    return result


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(DB_URL)


def init_db() -> None:
    """
    Apply all schema files in order: base table + W2/W3/W4 ALTERs.
    Every statement is idempotent (CREATE/ADD COLUMN IF NOT EXISTS).
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            for fname in _SCHEMA_FILES:
                path = _SQL_DIR / fname
                if not path.exists():
                    logger.warning("Schema file missing: %s", path)
                    continue
                cur.execute(path.read_text())
        conn.commit()
    logger.info("Schema initialised (or already current)")


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

# All columns of surface_metrics_core in declaration order.
# W1 = original, W2 = wing IVs / 10d convex / RR / VIX basis.
_COLUMNS = [
    "trade_date", "quote_time",
    "day_of_week", "days_to_monthly_opex",
    "spot",
    "forward_1d", "forward_7d", "forward_30d", "forward_90d", "forward_180d",
    # IV matrix (W1 25p/atm/25c + W2 10p/10c)
    "iv_1d_10p",   "iv_1d_25p",   "iv_1d_atm",   "iv_1d_25c",   "iv_1d_10c",
    "iv_7d_10p",   "iv_7d_25p",   "iv_7d_atm",   "iv_7d_25c",   "iv_7d_10c",
    "iv_30d_10p",  "iv_30d_25p",  "iv_30d_atm",  "iv_30d_25c",  "iv_30d_10c",
    "iv_90d_10p",  "iv_90d_25p",  "iv_90d_atm",  "iv_90d_25c",  "iv_90d_10c",
    "iv_180d_10p", "iv_180d_25p", "iv_180d_atm", "iv_180d_25c", "iv_180d_10c",
    # VIX
    "vix_1d", "vix_7d", "vix_30d", "vix_90d", "vix_180d",
    # Term ratios
    "term_ratio_1d_7d", "term_ratio_7d_30d", "term_ratio_30d_90d",
    # Skew slopes
    "skew_1d_10p_25p", "skew_1d_25p_atm", "skew_1d_10p_atm",
    "skew_1d_atm_25c", "skew_1d_atm_10c", "skew_1d_25p_25c",
    "skew_7d_10p_25p", "skew_7d_25p_atm", "skew_7d_10p_atm",
    "skew_7d_atm_25c", "skew_7d_atm_10c", "skew_7d_25p_25c",
    "skew_30d_10p_25p", "skew_30d_25p_atm", "skew_30d_10p_atm",
    "skew_30d_atm_25c", "skew_30d_atm_10c", "skew_30d_25p_25c",
    "skew_90d_10p_25p", "skew_90d_25p_atm", "skew_90d_10p_atm",
    "skew_90d_atm_25c", "skew_90d_atm_10c", "skew_90d_25p_25c",
    "skew_180d_10p_25p", "skew_180d_25p_atm", "skew_180d_10p_atm",
    "skew_180d_atm_25c", "skew_180d_atm_10c", "skew_180d_25p_25c",
    # Term slope
    "term_slope_1_7_25p", "term_slope_1_7_atm", "term_slope_1_7_25c",
    "term_slope_7_30_25p", "term_slope_7_30_atm", "term_slope_7_30_25c",
    "term_slope_30_90_25p", "term_slope_30_90_atm", "term_slope_30_90_25c",
    # Convexity (W1 + W2 10p_atm_10c symmetric)
    "convex_1d_10p_25p_atm",   "convex_1d_atm_25c_10c",   "convex_1d_25p_atm_25c",   "convex_1d_10p_atm_10c",
    "convex_7d_10p_25p_atm",   "convex_7d_atm_25c_10c",   "convex_7d_25p_atm_25c",   "convex_7d_10p_atm_10c",
    "convex_30d_10p_25p_atm",  "convex_30d_atm_25c_10c",  "convex_30d_25p_atm_25c",  "convex_30d_10p_atm_10c",
    "convex_90d_10p_25p_atm",  "convex_90d_atm_25c_10c",  "convex_90d_25p_atm_25c",  "convex_90d_10p_atm_10c",
    "convex_180d_10p_25p_atm", "convex_180d_atm_25c_10c", "convex_180d_25p_atm_25c", "convex_180d_10p_atm_10c",
    # W2: Risk reversal
    "rr_1d_25", "rr_7d_25", "rr_30d_25", "rr_90d_25", "rr_180d_25",
    "rr_30d_10", "rr_90d_10",
    # W2: VIX basis
    "vix_basis_1d_30d", "vix_basis_30d_90d",
]

_INSERT_COLS   = ", ".join(_COLUMNS)
_INSERT_PARAMS = ", ".join(f"%({c})s" for c in _COLUMNS)
_UPDATE_SET    = ", ".join(
    f"{c} = EXCLUDED.{c}"
    for c in _COLUMNS
    if c not in ("trade_date", "quote_time")
)

_UPSERT = f"""
INSERT INTO surface_metrics_core ({_INSERT_COLS})
VALUES ({_INSERT_PARAMS})
ON CONFLICT (trade_date, quote_time) DO UPDATE SET
    {_UPDATE_SET}
"""


def upsert_snapshots(
    conn: psycopg2.extensions.connection,
    rows: list[dict],
) -> None:
    """Bulk-upsert a list of snapshot rows into surface_metrics_core."""
    if not rows:
        return
    sanitized = [_sanitize(r) for r in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, _UPSERT, sanitized, page_size=500)
    conn.commit()
