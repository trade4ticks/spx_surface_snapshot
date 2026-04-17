"""
Backfill script for surface_metrics_core.

Run from the project root (or directly) in the VPS terminal:
    python scripts/backfill.py

Prompts for a start and end date, then processes each trading date in that
range, computing and upserting all surface_metrics_core rows.

Efficiency:
  - DB data (spx_atm, spx_surface) is fetched once per trade_date.
  - Parquet data is loaded once per trade_date.
  - Snapshots already present in surface_metrics_core are skipped unless
    --force is passed.

Usage:
    python scripts/backfill.py              # interactive date prompts
    python scripts/backfill.py --force      # reprocess all, overwriting existing rows
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Allow running as a script from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from pipeline.compute import compute_snapshot_metrics
from pipeline.config  import DATA_ROOT, TARGET_DTES
from pipeline.db      import (
    fetch_day_atm,
    fetch_day_surface,
    fetch_processed_quote_times,
    get_connection,
)
from pipeline.store   import init_db, upsert_snapshots
from pipeline.vix     import compute_vix_for_snapshot, load_parquet_day

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-date processing
# ---------------------------------------------------------------------------

def process_date(
    trade_date: date,
    conn,
    force: bool = False,
) -> int:
    """
    Process all snapshots for trade_date.

    Returns the number of snapshots upserted (0 if skipped).
    """
    date_str = trade_date.isoformat()
    log.info("Processing %s", date_str)

    # ------------------------------------------------------------------ DB fetch
    atm_by_qt     = fetch_day_atm(conn, date_str)
    surface_by_qt = fetch_day_surface(conn, date_str)

    if not atm_by_qt:
        log.info("  No spx_atm data — skipping")
        return 0

    # Determine which quote_times to process
    all_quote_times = sorted(atm_by_qt.keys())

    if not force:
        already_done = fetch_processed_quote_times(conn, date_str)
        to_process   = [qt for qt in all_quote_times if qt not in already_done]
        skipped      = len(all_quote_times) - len(to_process)
        if skipped:
            log.info("  Skipping %d already-processed snapshots", skipped)
    else:
        to_process = all_quote_times

    if not to_process:
        log.info("  All snapshots already processed")
        return 0

    # -------------------------------------------------------- Parquet load (VIX)
    parquet_df = load_parquet_day(DATA_ROOT, trade_date)
    if parquet_df.empty:
        log.warning("  No parquet data found — VIX columns will be NULL")

    # ---------------------------------------------------- Per-snapshot compute
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

        row = compute_snapshot_metrics(trade_date, qt, atm, surface, vix)
        rows.append(row)

    upsert_snapshots(conn, rows)
    log.info("  Upserted %d snapshots", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Date range runner
# ---------------------------------------------------------------------------

def run_backfill(start: date, end: date, force: bool = False) -> None:
    init_db()

    with get_connection() as conn:
        d = start
        while d <= end:
            try:
                process_date(d, conn, force=force)
            except Exception as exc:
                log.error("Date %s failed: %s", d.isoformat(), exc, exc_info=True)
            d += timedelta(days=1)

    log.info("Backfill complete: %s → %s", start.isoformat(), end.isoformat())


# ---------------------------------------------------------------------------
# CLI
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
        description="Backfill surface_metrics_core table"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess and overwrite already-completed snapshots",
    )
    parser.add_argument(
        "--start",
        help="Start date YYYYMMDD (skips prompt if provided)",
    )
    parser.add_argument(
        "--end",
        help="End date YYYYMMDD (skips prompt if provided)",
    )
    args = parser.parse_args()

    if args.start:
        start = _parse_date(args.start)
    else:
        start = _prompt_date("Start date (YYYYMMDD): ")

    if args.end:
        end = _parse_date(args.end)
    else:
        end = _prompt_date("End date   (YYYYMMDD): ")

    if start > end:
        print("Error: start date must be <= end date")
        sys.exit(1)

    log.info("Backfill %s → %s (force=%s)", start.isoformat(), end.isoformat(), args.force)
    run_backfill(start, end, force=args.force)


if __name__ == "__main__":
    main()
