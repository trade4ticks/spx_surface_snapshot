"""
backfill_w4.py — populate Wave 4 transforms (chg_d, chg_1w, z) over W1+W2+W3.

W4 columns (336 total):
  - chg_d_<base>   = base(t) - base(t-1td)        (110 cols)
  - chg_1w_<base>  = base(t) - base(t-5td)        (110 cols)
  - z_<base>       = (base(t) - μ) / σ over 63td  (116 cols)

All windows snapshot-aligned: lookbacks pull the same quote_time on prior
trading days. Z window is 63 trading days at the same quote_time.

Skip rules (must mirror pipeline/catalog_def.py):
  - PK / calendar / spot / forward_*d            : no transforms
  - log_ret / vov / spot_vol                      : z only
  - everything else                               : all 3 transforms

Order of operations: backfill (W1) → backfill_w2 → backfill_w3 → backfill_w4.
W3-derived bases (rv_*, vrp_*, vov_*, spot_vol_*, log_ret_*) must be
populated before this script can produce their transforms.

Architecture:
  - SELECT all 116 base columns + PK from surface_metrics_core.
  - Group by quote_time; per group, sort by trade_date, apply diff/rolling.
  - UPDATE the 336 W4 columns per group (avoids holding all derived data
    in memory at once).

Usage:
    python scripts/backfill_w4.py --all
    python scripts/backfill_w4.py --range 2024-01-01 2024-12-31
"""
from __future__ import annotations

import argparse
import logging
import sys
import warnings
from datetime import date, datetime
from pathlib import Path

# Allow running as a script from the project root
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
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column lists — derived from pipeline.catalog_def
# ---------------------------------------------------------------------------

CHG_BASES: list[str] = chg_eligible_bases()    # 110
Z_BASES:   list[str] = z_eligible_bases()      # 116

FETCH_COLS: list[str] = ["trade_date", "quote_time"] + Z_BASES   # 118

W4_COLUMNS: list[str] = (
    [f"chg_d_{c}"  for c in CHG_BASES]
    + [f"chg_1w_{c}" for c in CHG_BASES]
    + [f"z_{c}"    for c in Z_BASES]
)   # 110 + 110 + 116 = 336


_FETCH_SQL = (
    "SELECT " + ", ".join(FETCH_COLS) +
    " FROM surface_metrics_core ORDER BY trade_date, quote_time"
)

_UPDATE_SQL = (
    "UPDATE surface_metrics_core SET "
    + ", ".join(f"{c} = %({c})s" for c in W4_COLUMNS)
    + " WHERE trade_date = %(trade_date)s AND quote_time = %(quote_time)s"
)


# ---------------------------------------------------------------------------
# Compute and write
# ---------------------------------------------------------------------------

def fetch_history(conn) -> pd.DataFrame:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        return pd.read_sql_query(_FETCH_SQL, conn)


def compute_group(grp: pd.DataFrame) -> pd.DataFrame:
    """Compute W4 transforms for a single quote_time group sorted by trade_date."""
    out: dict = {
        "trade_date": grp["trade_date"].values,
        "quote_time": grp["quote_time"].values,
    }

    # ── chg_d, chg_1w (snapshot-aligned diffs) ──────────────────────
    for col in CHG_BASES:
        s = grp[col]
        out[f"chg_d_{col}"]  = s.diff(1).values
        out[f"chg_1w_{col}"] = s.diff(5).values

    # ── z (63td rolling, snapshot-aligned) ──────────────────────────
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


def upsert_group(
    conn,
    df: pd.DataFrame,
    start: date | None,
    end:   date | None,
) -> int:
    """UPDATE this group's W4 columns, optionally filtered by date range."""
    sub = df
    if start is not None:
        sub = sub[sub["trade_date"] >= start]
    if end is not None:
        sub = sub[sub["trade_date"] <= end]
    if sub.empty:
        return 0

    cols = ["trade_date", "quote_time"] + W4_COLUMNS
    rows = [_sanitize(r) for r in sub[cols].to_dict("records")]

    # page_size=100 keeps total parameters per round-trip well under PG's
    # 65,535 limit (338 params/row × 100 rows = 33,800).
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, _UPDATE_SQL, rows, page_size=100)
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Wave 4 transforms")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument(
        "--all", action="store_true",
        help="Update every row in surface_metrics_core",
    )
    grp.add_argument(
        "--range", nargs=2, metavar=("START", "END"),
        help="YYYY-MM-DD YYYY-MM-DD inclusive (full history is still loaded "
             "so rolling windows have lookback)",
    )
    args = parser.parse_args()

    init_db()  # idempotent

    if args.range:
        start = datetime.strptime(args.range[0], "%Y-%m-%d").date()
        end   = datetime.strptime(args.range[1], "%Y-%m-%d").date()
    else:
        start = end = None

    conn = get_connection()
    try:
        log.info(
            "Loading history (%d base cols)... this can be ~250 MB for years of data",
            len(FETCH_COLS),
        )
        df = fetch_history(conn)
        n_rows  = len(df)
        n_qt    = df["quote_time"].nunique()
        n_dates = df["trade_date"].nunique()
        log.info("  %d rows  /  %d quote_times  /  %d trade_dates",
                 n_rows, n_qt, n_dates)

        if df.empty:
            log.warning("No rows to process.")
            return

        log.info("Processing per quote_time (%d groups)...", n_qt)
        total = 0
        for i, (qt, grp_df) in enumerate(df.groupby("quote_time", sort=False), 1):
            grp_df = grp_df.sort_values("trade_date").reset_index(drop=True)
            result = compute_group(grp_df)
            n = upsert_group(conn, result, start, end)
            total += n
            if i % 10 == 0 or i == n_qt:
                log.info("  [%d/%d] %s — group %d rows updated (cum %d)",
                         i, n_qt, qt, n, total)

        scope = "full table" if args.all else f"{start}..{end}"
        log.info("Done. %d total rows updated across %d quote_times (%s).",
                 total, n_qt, scope)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
