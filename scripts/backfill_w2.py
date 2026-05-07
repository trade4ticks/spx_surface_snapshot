"""
backfill_w2.py — populate Wave 2 columns on already-processed rows.

W2 columns (24 total):
  - iv_*_10p, iv_*_10c           (10) — read from spx_surface put_delta=10/90
  - convex_*_10p_atm_10c         (5)  — symmetric 10d butterfly
  - rr_*_25, rr_*_10             (7)  — risk reversal (iv_call - iv_put)
  - vix_basis_1d_30d, _30d_90d   (2)  — term-structure spread

Strategy: re-fetch spx_atm + spx_surface per day, read existing vix_*d values
already stored in surface_metrics_core, recompute via compute_snapshot_metrics,
UPDATE only the 24 W2 columns. No parquet, no CBOE variance.

Usage:
    python scripts/backfill_w2.py --all
    python scripts/backfill_w2.py --start 2024-01-01 --end 2024-12-31
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from pathlib import Path

# Allow running as a script from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2.extras

from pipeline.compute import compute_snapshot_metrics
from pipeline.config  import TARGET_DTES
from pipeline.db      import fetch_day_atm, fetch_day_surface, get_connection
from pipeline.store   import init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# W2 columns — keep in sync with sql/w2_alter.sql
# ---------------------------------------------------------------------------
W2_COLUMNS = [
    # Wing IVs (10)
    "iv_1d_10p",   "iv_1d_10c",
    "iv_7d_10p",   "iv_7d_10c",
    "iv_30d_10p",  "iv_30d_10c",
    "iv_90d_10p",  "iv_90d_10c",
    "iv_180d_10p", "iv_180d_10c",
    # Symmetric 10d convexity (5)
    "convex_1d_10p_atm_10c",
    "convex_7d_10p_atm_10c",
    "convex_30d_10p_atm_10c",
    "convex_90d_10p_atm_10c",
    "convex_180d_10p_atm_10c",
    # Risk reversal (7)
    "rr_1d_25", "rr_7d_25", "rr_30d_25", "rr_90d_25", "rr_180d_25",
    "rr_30d_10", "rr_90d_10",
    # VIX basis (2)
    "vix_basis_1d_30d", "vix_basis_30d_90d",
]

_UPDATE_SQL = (
    "UPDATE surface_metrics_core SET "
    + ", ".join(f"{c} = %({c})s" for c in W2_COLUMNS)
    + " WHERE trade_date = %(trade_date)s AND quote_time = %(quote_time)s"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_existing_vix(conn, trade_date_str: str) -> dict:
    """
    Pull already-computed vix_*d values per quote_time so we can compute
    vix_basis_* without re-running the W1 parquet/CBOE pipeline.
    Returns: { quote_time: { dte_int: vix_value_or_None } }
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT quote_time, vix_1d, vix_7d, vix_30d, vix_90d, vix_180d
            FROM surface_metrics_core
            WHERE trade_date = %s
            """,
            (trade_date_str,),
        )
        return {
            row[0]: {1: row[1], 7: row[2], 30: row[3], 90: row[4], 180: row[5]}
            for row in cur.fetchall()
        }


def fetch_dates_needing_w2(conn) -> list[date]:
    """
    Return distinct trade_dates that still have NULL in iv_30d_10p — used
    as a sentinel for "this row has not had W2 applied."
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT trade_date
            FROM surface_metrics_core
            WHERE iv_30d_10p IS NULL
            ORDER BY trade_date
            """
        )
        return [r[0] for r in cur.fetchall()]


def fetch_dates_in_range(conn, start: date, end: date) -> list[date]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT trade_date
            FROM surface_metrics_core
            WHERE trade_date BETWEEN %s AND %s
            ORDER BY trade_date
            """,
            (start, end),
        )
        return [r[0] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Per-date processing
# ---------------------------------------------------------------------------

def process_date(conn, trade_date: date) -> int:
    """Recompute and UPDATE the 24 W2 columns for every quote_time on trade_date."""
    date_str = trade_date.isoformat()

    atm_by_qt     = fetch_day_atm(conn, date_str)
    surface_by_qt = fetch_day_surface(conn, date_str)
    vix_by_qt     = fetch_existing_vix(conn, date_str)

    if not atm_by_qt:
        log.info("  %s: no spx_atm data, skipping", date_str)
        return 0

    update_rows = []
    for qt, atm in atm_by_qt.items():
        surface = surface_by_qt.get(qt, {})
        vix     = vix_by_qt.get(qt, {dte: None for dte in TARGET_DTES})

        full_row = compute_snapshot_metrics(trade_date, qt, atm, surface, vix)

        rec = {c: full_row.get(c) for c in W2_COLUMNS}
        rec["trade_date"] = date_str
        rec["quote_time"] = qt
        update_rows.append(rec)

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, _UPDATE_SQL, update_rows, page_size=500)
    conn.commit()
    return len(update_rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Wave 2 columns")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--all",
        action="store_true",
        help="Every trade_date where iv_30d_10p IS NULL",
    )
    group.add_argument(
        "--range",
        nargs=2,
        metavar=("START", "END"),
        help="YYYY-MM-DD YYYY-MM-DD inclusive",
    )
    args = parser.parse_args()

    # Ensure all W2/W3/W4 columns exist before we try to UPDATE them.
    init_db()

    conn = get_connection()
    try:
        if args.all:
            dates = fetch_dates_needing_w2(conn)
            log.info("Found %d dates with any NULL W2 column", len(dates))
        else:
            start = datetime.strptime(args.range[0], "%Y-%m-%d").date()
            end   = datetime.strptime(args.range[1], "%Y-%m-%d").date()
            dates = fetch_dates_in_range(conn, start, end)
            log.info("Found %d dates in range %s..%s", len(dates), start, end)

        total = 0
        for d in dates:
            try:
                n = process_date(conn, d)
                log.info("  %s: %d rows updated", d.isoformat(), n)
                total += n
            except Exception as exc:
                log.error("  %s FAILED: %s", d.isoformat(), exc)
                conn.rollback()

        log.info("Done. %d rows updated across %d dates.", total, len(dates))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
