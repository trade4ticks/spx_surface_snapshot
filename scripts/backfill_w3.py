"""
backfill_w3.py — populate Wave 3 time-series base metrics.

W3 columns (15):
  - log_ret_d / 1w / 1m              (3) — snapshot-aligned spot log returns
  - rv_1w / 1m / 3m                  (3) — trailing realized vol × √252
  - vrp_1w / 1m / 3m                 (3) — iv_<tenor>_atm − rv_<period>
  - vrp_ratio_1w / 1m / 3m           (3) — iv / rv (sensitive when rv → 0)
  - vov_30d_1m                       (1) — 21td stdev of d_iv_30d_atm × √252
  - spot_vol_30d_1m / 30d_3m         (2) — rolling corr(log_ret_d, d_iv_30d_atm)

All windows use TRADING days, snapshot-aligned: lookbacks pull the same
quote_time on prior trading days.

Architecture:
  - Pull (trade_date, quote_time, spot, iv_7d_atm, iv_30d_atm, iv_90d_atm)
    rows from surface_metrics_core
  - Group by quote_time; within each group, sort by trade_date and apply
    pandas .diff() / .rolling() / .corr()
  - UPDATE the 15 W3 columns

Single vectorized pass; minutes for years of history.

Usage:
    python scripts/backfill_w3.py --all                              # full table
    python scripts/backfill_w3.py                                    # prompts for start/end
    python scripts/backfill_w3.py --start 20240101 --end 20241231
"""
from __future__ import annotations

import argparse
import logging
import math
import sys
import warnings
from datetime import date, datetime
from pathlib import Path

# Allow running as a script from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
import psycopg2.extras

from pipeline.db    import get_connection
from pipeline.store import _sanitize, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column lists — keep in sync with sql/w3_alter.sql
# ---------------------------------------------------------------------------

W3_COLUMNS = [
    "log_ret_d", "log_ret_1w", "log_ret_1m",
    "rv_1w", "rv_1m", "rv_3m",
    "vrp_1w", "vrp_1m", "vrp_3m",
    "vrp_ratio_1w", "vrp_ratio_1m", "vrp_ratio_3m",
    "vov_30d_1m",
    "spot_vol_30d_1m", "spot_vol_30d_3m",
]

_FETCH_SQL = """
    SELECT trade_date, quote_time, spot,
           iv_7d_atm, iv_30d_atm, iv_90d_atm
    FROM surface_metrics_core
    ORDER BY trade_date, quote_time
"""

_UPDATE_SQL = (
    "UPDATE surface_metrics_core SET "
    + ", ".join(f"{c} = %({c})s" for c in W3_COLUMNS)
    + " WHERE trade_date = %(trade_date)s AND quote_time = %(quote_time)s"
)

_SQRT_252 = math.sqrt(252.0)


# ---------------------------------------------------------------------------
# Pull / compute / write
# ---------------------------------------------------------------------------

def fetch_history(conn) -> pd.DataFrame:
    with warnings.catch_warnings():
        # pandas warns when given a non-SQLAlchemy connection; fine here.
        warnings.simplefilter("ignore", UserWarning)
        df = pd.read_sql_query(_FETCH_SQL, conn)
    return df


def compute_w3(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add the 15 W3 columns. Snapshot-aligned: groupby quote_time, then sort
    by trade_date within each group before applying time-series ops.
    """
    pieces = []
    for qt, grp in df.groupby("quote_time", sort=False):
        grp = grp.sort_values("trade_date").reset_index(drop=True).copy()

        # ── Spot log returns ──────────────────────────────────────────────
        log_spot = np.log(grp["spot"])
        grp["log_ret_d"]  = log_spot.diff(1)
        grp["log_ret_1w"] = log_spot.diff(5)
        grp["log_ret_1m"] = log_spot.diff(21)

        # ── Realized vol (trailing trading-day stdev × √252) ─────────────
        ret_d = grp["log_ret_d"]
        grp["rv_1w"] = ret_d.rolling(5).std()  * _SQRT_252
        grp["rv_1m"] = ret_d.rolling(21).std() * _SQRT_252
        grp["rv_3m"] = ret_d.rolling(63).std() * _SQRT_252

        # ── VRP at matched horizons ──────────────────────────────────────
        grp["vrp_1w"] = grp["iv_7d_atm"]  - grp["rv_1w"]
        grp["vrp_1m"] = grp["iv_30d_atm"] - grp["rv_1m"]
        grp["vrp_3m"] = grp["iv_90d_atm"] - grp["rv_3m"]

        # ── VRP ratio (NaN where rv is 0/NaN) ────────────────────────────
        with np.errstate(divide="ignore", invalid="ignore"):
            grp["vrp_ratio_1w"] = grp["iv_7d_atm"]  / grp["rv_1w"]
            grp["vrp_ratio_1m"] = grp["iv_30d_atm"] / grp["rv_1m"]
            grp["vrp_ratio_3m"] = grp["iv_90d_atm"] / grp["rv_3m"]
            # ±inf → NaN so _sanitize maps to None on write
            for c in ("vrp_ratio_1w", "vrp_ratio_1m", "vrp_ratio_3m"):
                grp[c] = grp[c].replace([np.inf, -np.inf], np.nan)

        # ── Vol of vol — daily Δ in iv_30d_atm, 21td stdev, annualized ───
        chg_iv30 = grp["iv_30d_atm"].diff(1)
        grp["vov_30d_1m"] = chg_iv30.rolling(21).std() * _SQRT_252

        # ── Spot–vol correlation ─────────────────────────────────────────
        grp["spot_vol_30d_1m"] = ret_d.rolling(21).corr(chg_iv30)
        grp["spot_vol_30d_3m"] = ret_d.rolling(63).corr(chg_iv30)

        pieces.append(grp)

    return pd.concat(pieces, ignore_index=True)


def upsert_w3(
    conn,
    df: pd.DataFrame,
    start: date | None = None,
    end:   date | None = None,
) -> int:
    """UPDATE surface_metrics_core with W3 columns, optionally filtered."""
    sub = df
    if start is not None:
        sub = sub[sub["trade_date"] >= start]
    if end is not None:
        sub = sub[sub["trade_date"] <= end]
    if sub.empty:
        return 0

    cols = ["trade_date", "quote_time"] + W3_COLUMNS
    rows = [_sanitize(r) for r in sub[cols].to_dict("records")]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, _UPDATE_SQL, rows, page_size=1000)
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
    parser = argparse.ArgumentParser(description="Backfill Wave 3 columns")
    parser.add_argument(
        "--all", action="store_true",
        help="Update every row in surface_metrics_core (skips date prompts)",
    )
    parser.add_argument(
        "--start", help="Start date YYYYMMDD (skips prompt if provided)",
    )
    parser.add_argument(
        "--end", help="End date YYYYMMDD (skips prompt if provided)",
    )
    args = parser.parse_args()

    if args.all:
        start = end = None
    else:
        start = _parse_date(args.start) if args.start else _prompt_date("Start date (YYYYMMDD): ")
        end   = _parse_date(args.end)   if args.end   else _prompt_date("End date   (YYYYMMDD): ")
        if start > end:
            print("Error: start date must be <= end date")
            sys.exit(1)

    init_db()  # idempotent — ensures W3 columns exist

    conn = get_connection()
    try:
        log.info("Loading history from surface_metrics_core...")
        df = fetch_history(conn)
        log.info("  %d rows, %d quote_times, %d trade_dates",
                 len(df), df["quote_time"].nunique(), df["trade_date"].nunique())

        if df.empty:
            log.warning("No rows to process.")
            return

        log.info("Computing W3 metrics (per-quote_time, vectorized)...")
        result = compute_w3(df)

        if args.all:
            n = upsert_w3(conn, result)
            log.info("Done. %d rows updated (full table).", n)
        else:
            n = upsert_w3(conn, result, start, end)
            log.info("Done. %d rows updated in %s..%s.", n, start, end)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
