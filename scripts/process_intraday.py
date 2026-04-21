"""
process_intraday.py — Incremental surface_metrics_core processing for today.

Designed to run via cron every 5 minutes during market hours.  Each run
checks which 5-min snapshots in spx_atm have not yet been captured (or
have gained additional DTEs since last processed) and computes only those.

Completeness logic:
  - For each quote_time today, count distinct TARGET DTEs in spx_atm (source).
  - For each quote_time already in surface_metrics_core, count non-null
    forward columns (proxy for DTEs available at last processing time).
  - A snapshot is (re-)processed when the source DTE count exceeds the
    processed count, ensuring newly available tenors are picked up.

All writes are upserts, so the job is idempotent and safe to call repeatedly.

Cron example (run a couple minutes after interpolate_SPX intraday):
  3-59/5 9-16 * * 1-5  /path/to/venv/Scripts/python.exe ^
      C:/Personal/Data/spx_surface_snapshot/scripts/process_intraday.py
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, time
from pathlib import Path

# Make the project root importable when invoked directly by cron
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytz

from pipeline.compute import compute_snapshot_metrics
from pipeline.config  import DATA_ROOTS, TARGET_DTES
from pipeline.db      import (
    fetch_day_atm,
    fetch_day_surface,
    fetch_processed_dte_counts,
    fetch_source_dte_counts,
    get_connection,
)
from pipeline.store   import init_db, upsert_snapshots
from pipeline.vix     import compute_vix_for_snapshot, load_parquet_day

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_ET = pytz.timezone("US/Eastern")

# Market hours window (ET).  Start a few minutes after the open so the
# first snapshot from the interpolation pipeline is available; allow a
# tail past the close to catch the final bar.
_OPEN_TIME  = time(9, 35)
_CLOSE_TIME = time(17, 0)


def _is_trading_day(now_et: datetime) -> bool:
    """Cheap weekday check.  Holidays still run but exit harmlessly when
    no source data exists."""
    return now_et.weekday() < 5


# ---------------------------------------------------------------------------
# Intraday processing
# ---------------------------------------------------------------------------

def process_intraday(today, conn) -> int:
    """
    Process new or incomplete snapshots for *today*.

    Returns the number of snapshots upserted.
    """
    date_str = today.isoformat()

    # ---- Determine which quote_times need (re-)processing ----------------
    source_counts    = fetch_source_dte_counts(conn, date_str)
    processed_counts = fetch_processed_dte_counts(conn, date_str)

    if not source_counts:
        log.info("No spx_atm data for %s yet — nothing to do.", date_str)
        return 0

    to_process = []
    for qt, src_n in source_counts.items():
        proc_n = processed_counts.get(qt, 0)
        if src_n > proc_n:
            to_process.append(qt)

    n_total   = len(source_counts)
    n_skipped = n_total - len(to_process)

    if n_skipped:
        log.info("Skipping %d / %d snapshots (already up-to-date)",
                 n_skipped, n_total)

    if not to_process:
        log.info("All %d snapshots already up-to-date", n_total)
        return 0

    to_process.sort()
    log.info("Processing %d snapshot(s): %s … %s",
             len(to_process),
             to_process[0].strftime("%H:%M"),
             to_process[-1].strftime("%H:%M"))

    # ---- Fetch source data (once for the whole day) ----------------------
    atm_by_qt     = fetch_day_atm(conn, date_str)
    surface_by_qt = fetch_day_surface(conn, date_str)

    # ---- Load parquet data for VIX (once for the whole day) --------------
    parquet_df = load_parquet_day(DATA_ROOTS, today)
    if parquet_df.empty:
        log.warning("No parquet data — VIX columns will be NULL")

    # ---- Compute metrics for each snapshot that needs it -----------------
    rows = []
    for qt in to_process:
        atm     = atm_by_qt.get(qt, {})
        surface = surface_by_qt.get(qt, {})

        # VIX: filter parquet to this snapshot's timestamp
        vix = {dte: None for dte in TARGET_DTES}
        if not parquet_df.empty:
            ts      = pd.Timestamp(f"{date_str} {qt.strftime('%H:%M:%S')}")
            snap_df = parquet_df[parquet_df["_ts"] == ts]
            if not snap_df.empty:
                vix = compute_vix_for_snapshot(snap_df, ts, TARGET_DTES)

        row = compute_snapshot_metrics(today, qt, atm, surface, vix)
        rows.append(row)

    upsert_snapshots(conn, rows)
    log.info("Upserted %d snapshots", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    now_et = datetime.now(_ET)

    if not _is_trading_day(now_et):
        log.info("Not a weekday — nothing to do.")
        return

    t = now_et.time()
    if t < _OPEN_TIME or t > _CLOSE_TIME:
        log.info("Outside market hours (%s ET) — nothing to do.",
                 now_et.strftime("%H:%M:%S"))
        return

    today = now_et.date()
    log.info("Intraday run for %s at %s ET",
             today.isoformat(), now_et.strftime("%H:%M:%S"))

    init_db()

    with get_connection() as conn:
        try:
            n = process_intraday(today, conn)
            log.info("Done — %d snapshot(s) written.", n)
        except Exception as exc:
            log.error("Failed: %s", exc, exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    main()
