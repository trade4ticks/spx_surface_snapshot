"""
backfill_w1w2_nonvix.py — recompute every W1+W2 column EXCEPT vix_*d.

When to use
-----------
After fixing rows in upstream spx_atm / spx_surface.  The W1+W2 columns that
read from those tables need recomputation, but vix_*d came from parquet +
CBOE variance and is independent — so we read existing vix_*d values back
out of surface_metrics_core and pass them through, skipping the expensive
parquet load entirely.

Cost: minutes for hundreds of days, vs ~5-6 minutes per day for the full
backfill.py (which re-runs parquet + CBOE variance).

Skips (NOT updated):
  - trade_date, quote_time            (primary key)
  - day_of_week, days_to_monthly_opex (deterministic from trade_date)
  - vix_1d / 7d / 30d / 90d / 180d    (parquet-derived, unchanged by spx_atm/surface fixes)

Updates (~102 cols):
  - spot, forward_*d
  - iv matrix (5 tenors x 5 deltas = 25)
  - term_ratio_*
  - skew_*
  - term_slope_*
  - convex_*
  - rr_*
  - vix_basis_*       (recomputed from the passed-through vix_*d values)

After running this, re-run:
  python scripts/backfill_w3.py --all
  python scripts/backfill_w4.py --all

Both W3 and W4 use rolling windows that look back into the recomputed
range, so they must be re-derived across the full table — even rows
outside this range may have had stale W3/W4 if their lookback windows
included the bad data.

Usage:
    python scripts/backfill_w1w2_nonvix.py                       # prompts for dates
    python scripts/backfill_w1w2_nonvix.py --start 20210101 --end 20220630
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
from pipeline.store   import _COLUMNS, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column lists
# ---------------------------------------------------------------------------

_VIX_COLS = frozenset({"vix_1d", "vix_7d", "vix_30d", "vix_90d", "vix_180d"})
_SKIP = frozenset({
    "trade_date", "quote_time",
    "day_of_week", "days_to_monthly_opex",
}) | _VIX_COLS

# Every W1+W2 column that's derived from spx_atm or spx_surface
UPDATE_COLUMNS: list[str] = [c for c in _COLUMNS if c not in _SKIP]

_UPDATE_SQL = (
    "UPDATE surface_metrics_core SET "
    + ", ".join(f"{c} = %({c})s" for c in UPDATE_COLUMNS)
    + " WHERE trade_date = %(trade_date)s AND quote_time = %(quote_time)s"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_existing_vix(conn, trade_date_str: str) -> dict:
    """Pull stored vix_*d values per quote_time so we can pass them through
    when recomputing vix_basis_* without touching the parquet pipeline.

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
    """Recompute and UPDATE the non-VIX W1+W2 columns for every quote_time."""
    date_str = trade_date.isoformat()

    atm_by_qt     = fetch_day_atm(conn, date_str)
    surface_by_qt = fetch_day_surface(conn, date_str)
    vix_by_qt     = fetch_existing_vix(conn, date_str)

    if not atm_by_qt:
        log.info("  %s: no spx_atm data, skipping", date_str)
        return 0

    rows = []
    for qt, atm in atm_by_qt.items():
        surface = surface_by_qt.get(qt, {})
        vix     = vix_by_qt.get(qt, {dte: None for dte in TARGET_DTES})

        full_row = compute_snapshot_metrics(trade_date, qt, atm, surface, vix)

        rec = {c: full_row.get(c) for c in UPDATE_COLUMNS}
        rec["trade_date"] = date_str
        rec["quote_time"] = qt
        rows.append(rec)

    # 102 update cols + 2 PK = 104 params/row.  page_size=200 -> 20,800
    # params/batch (well under PG's 65,535 limit).
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, _UPDATE_SQL, rows, page_size=200)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> date:
    return datetime.strptime(raw, "%Y%m%d").date()


def _prompt_date(prompt: str) -> date:
    while True:
        raw = input(prompt).strip()
        try:
            return _parse_date(raw)
        except ValueError:
            print("  Invalid date — use YYYYMMDD format.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Recompute W1+W2 (excluding vix_*d) for a date range"
    )
    parser.add_argument(
        "--start", help="Start date YYYYMMDD (skips prompt if provided)",
    )
    parser.add_argument(
        "--end", help="End date YYYYMMDD (skips prompt if provided)",
    )
    args = parser.parse_args()

    start = _parse_date(args.start) if args.start else _prompt_date("Start date (YYYYMMDD): ")
    end   = _parse_date(args.end)   if args.end   else _prompt_date("End date   (YYYYMMDD): ")

    if start > end:
        print("Error: start date must be <= end date")
        sys.exit(1)

    init_db()  # idempotent

    conn = get_connection()
    try:
        dates = fetch_dates_in_range(conn, start, end)
        log.info("Recomputing W1+W2 (non-VIX) for %d dates: %s to %s",
                 len(dates), start, end)
        log.info("  UPDATE column count: %d (vix_*d and calendar fields excluded)",
                 len(UPDATE_COLUMNS))

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
        log.info("Next: python scripts/backfill_w3.py --all")
        log.info("      python scripts/backfill_w4.py --all")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
