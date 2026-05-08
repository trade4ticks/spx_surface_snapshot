"""
process_intraday_followup.py — fill W3 + W4 columns for the most recent days.

Companion to process_intraday.py.  process_intraday.py populates W1+W2 for
new snapshots; this script reads them back and computes the time-series
W3 base metrics and the W4 chg/z transforms.

Architecture:
  - Pull the last ~100 calendar days of base columns (covers the 63-trading-
    day rolling windows with a buffer).
  - Group by quote_time, sort by trade_date, compute W3 then W4.
  - UPDATE rows whose trade_date falls inside the catch-up window
    (default: last 3 calendar days), so any slice that was missed or
    arrived late gets refreshed.

Catch-up window:
  Defaults to 3 calendar days.  On a Monday this covers Friday + Monday
  (Sat/Sun produce no rows).  Pass --catchup-days N to change.

Cron example (run a couple minutes after process_intraday.py):
  5-59/5 9-16 * * 1-5  /path/to/python ^
      C:/Personal/Data/spx_surface_snapshot/scripts/process_intraday_followup.py
"""
from __future__ import annotations

import argparse
import logging
import math
import sys
import warnings
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# Make the project root importable when invoked directly by cron
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
import psycopg2.extras

from pipeline.catalog_def import chg_eligible_bases, z_eligible_bases
from pipeline.db          import get_connection
from pipeline.store       import _sanitize, init_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")
_OPEN_TIME  = time(9, 35)
_CLOSE_TIME = time(17, 0)
_SQRT_252   = math.sqrt(252.0)


# ---------------------------------------------------------------------------
# Column lists — single source of truth: pipeline/catalog_def.py
# ---------------------------------------------------------------------------

CHG_BASES: list[str] = chg_eligible_bases()      # 110
Z_BASES:   list[str] = z_eligible_bases()        # 116

# W3 columns — kept in same order as sql/w3_alter.sql
W3_COLUMNS: list[str] = [
    "log_ret_d", "log_ret_1w", "log_ret_1m",
    "rv_1w", "rv_1m", "rv_3m",
    "vrp_1w", "vrp_1m", "vrp_3m",
    "vrp_ratio_1w", "vrp_ratio_1m", "vrp_ratio_3m",
    "vov_30d_1m",
    "spot_vol_30d_1m", "spot_vol_30d_3m",
]

# W4 transforms
W4_COLUMNS: list[str] = (
    [f"chg_d_{c}"  for c in CHG_BASES]
    + [f"chg_1w_{c}" for c in CHG_BASES]
    + [f"z_{c}"    for c in Z_BASES]
)

# Combined output: 15 + 336 = 351
ALL_DERIVED = W3_COLUMNS + W4_COLUMNS

# Columns to fetch from surface_metrics_core: PK + spot + everything z-eligible.
# Z_BASES already includes the W1/W2 IV columns we need for VRP (iv_7d_atm,
# iv_30d_atm, iv_90d_atm).  spot is in NO_TRANSFORM so add it explicitly.
FETCH_COLS: list[str] = ["trade_date", "quote_time", "spot"] + Z_BASES


# ---------------------------------------------------------------------------
# DB I/O
# ---------------------------------------------------------------------------

def fetch_history(conn, history_start: date) -> pd.DataFrame:
    sql = (
        "SELECT " + ", ".join(FETCH_COLS) +
        " FROM surface_metrics_core "
        "WHERE trade_date >= %s "
        "ORDER BY trade_date, quote_time"
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        return pd.read_sql_query(sql, conn, params=(history_start,))


_UPDATE_SQL = (
    "UPDATE surface_metrics_core SET "
    + ", ".join(f"{c} = %({c})s" for c in ALL_DERIVED)
    + " WHERE trade_date = %(trade_date)s AND quote_time = %(quote_time)s"
)


# ---------------------------------------------------------------------------
# Compute (W3 then W4 in one pass per quote_time)
# ---------------------------------------------------------------------------

def compute_group(grp: pd.DataFrame) -> pd.DataFrame:
    """
    Compute W3 base metrics + W4 transforms for one quote_time group
    (must already be sorted by trade_date).
    """
    grp = grp.copy()

    # ── W3 base time-series metrics (mirrors backfill_w3.compute_w3) ────
    log_spot = np.log(grp["spot"])
    grp["log_ret_d"]  = log_spot.diff(1)
    grp["log_ret_1w"] = log_spot.diff(5)
    grp["log_ret_1m"] = log_spot.diff(21)

    ret_d = grp["log_ret_d"]
    grp["rv_1w"] = ret_d.rolling(5).std()  * _SQRT_252
    grp["rv_1m"] = ret_d.rolling(21).std() * _SQRT_252
    grp["rv_3m"] = ret_d.rolling(63).std() * _SQRT_252

    grp["vrp_1w"] = grp["iv_7d_atm"]  - grp["rv_1w"]
    grp["vrp_1m"] = grp["iv_30d_atm"] - grp["rv_1m"]
    grp["vrp_3m"] = grp["iv_90d_atm"] - grp["rv_3m"]

    with np.errstate(divide="ignore", invalid="ignore"):
        for period, iv_col in (("1w", "iv_7d_atm"),
                               ("1m", "iv_30d_atm"),
                               ("3m", "iv_90d_atm")):
            ratio = grp[iv_col] / grp[f"rv_{period}"]
            grp[f"vrp_ratio_{period}"] = ratio.replace([np.inf, -np.inf], np.nan)

    chg_iv30 = grp["iv_30d_atm"].diff(1)
    grp["vov_30d_1m"]      = chg_iv30.rolling(21).std() * _SQRT_252
    grp["spot_vol_30d_1m"] = ret_d.rolling(21).corr(chg_iv30)
    grp["spot_vol_30d_3m"] = ret_d.rolling(63).corr(chg_iv30)

    # ── Build output (PK + W3 + W4) ───────────────────────────────────
    out: dict = {
        "trade_date": grp["trade_date"].values,
        "quote_time": grp["quote_time"].values,
    }
    for c in W3_COLUMNS:
        out[c] = grp[c].values

    # ── W4 transforms (mirrors backfill_w4.compute_group) ─────────────
    for col in CHG_BASES:
        s = grp[col]
        out[f"chg_d_{col}"]  = s.diff(1).values
        out[f"chg_1w_{col}"] = s.diff(5).values

    for col in Z_BASES:
        s    = grp[col]
        roll = s.rolling(63)
        mean = roll.mean()
        std  = roll.std()
        with np.errstate(divide="ignore", invalid="ignore"):
            z = (s - mean) / std
            z = z.replace([np.inf, -np.inf], np.nan)
        out[f"z_{col}"] = z.values

    return pd.DataFrame(out)


def upsert_window(conn, df: pd.DataFrame, update_start: date) -> int:
    """UPDATE only rows with trade_date >= update_start (the catch-up window)."""
    sub = df[df["trade_date"] >= update_start]
    if sub.empty:
        return 0

    cols = ["trade_date", "quote_time"] + ALL_DERIVED
    rows = [_sanitize(r) for r in sub[cols].to_dict("records")]

    # 351 + 2 PK = 353 params/row.  page_size=50 keeps total well under
    # PostgreSQL's 65,535 parameter limit (353 × 50 = 17,650).
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, _UPDATE_SQL, rows, page_size=50)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _is_trading_day(now_et: datetime) -> bool:
    """Cheap weekday check.  Holidays still run but exit harmlessly when
    no source data exists."""
    return now_et.weekday() < 5


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Intraday W3+W4 follow-up — runs after process_intraday.py"
    )
    parser.add_argument(
        "--catchup-days", type=int, default=3,
        help="UPDATE rows whose trade_date is in the last N calendar days "
             "(default: 3 — Monday picks up Friday).",
    )
    parser.add_argument(
        "--for-date",
        help="Override 'today' (YYYY-MM-DD); defaults to current ET date.",
    )
    parser.add_argument(
        "--ignore-market-hours", action="store_true",
        help="Skip the trading-day / market-hours guard.",
    )
    args = parser.parse_args()

    if args.for_date:
        today = datetime.strptime(args.for_date, "%Y-%m-%d").date()
        log.info("Using override --for-date %s", today)
    else:
        now_et = datetime.now(_ET)
        if not args.ignore_market_hours:
            if not _is_trading_day(now_et):
                log.info("Not a weekday — nothing to do.")
                return
            t = now_et.time()
            if t < _OPEN_TIME or t > _CLOSE_TIME:
                log.info(
                    "Outside market hours (%s ET) — nothing to do.",
                    now_et.strftime("%H:%M:%S"),
                )
                return
        today = now_et.date()

    # Need 63 trading days of lookback for the oldest date in the catch-up
    # window; ~95 calendar days covers that with weekends.  Pad for safety.
    history_start = today - timedelta(days=100 + args.catchup_days)
    update_start  = today - timedelta(days=args.catchup_days)

    log.info(
        "Follow-up for %s (catchup=%dd; UPDATE rows trade_date >= %s)",
        today.isoformat(), args.catchup_days, update_start.isoformat(),
    )

    init_db()

    with get_connection() as conn:
        try:
            log.info("Loading history from %s ...", history_start.isoformat())
            df = fetch_history(conn, history_start)

            if df.empty:
                log.info("No rows in history window — nothing to do.")
                return

            log.info("  %d rows / %d quote_times / %d trade_dates",
                     len(df),
                     df["quote_time"].nunique(),
                     df["trade_date"].nunique())

            log.info("Computing W3 + W4 per quote_time...")
            total = 0
            for qt, grp_df in df.groupby("quote_time", sort=False):
                grp_df = grp_df.sort_values("trade_date").reset_index(drop=True)
                result = compute_group(grp_df)
                n = upsert_window(conn, result, update_start)
                total += n

            log.info("Done — %d row(s) updated (catchup=%dd).",
                     total, args.catchup_days)

        except Exception as exc:
            log.error("Failed: %s", exc, exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    main()
