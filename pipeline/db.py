"""
Database helpers: connection and bulk fetches for spx_atm and spx_surface.
"""
from __future__ import annotations

import psycopg2
import psycopg2.extras

from .config import DB_URL, TARGET_DTES, SURFACE_DELTAS


def get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(DB_URL)


# ---------------------------------------------------------------------------
# Bulk day-level fetches — one query per table per trade_date
# ---------------------------------------------------------------------------

def fetch_day_atm(
    conn: psycopg2.extensions.connection,
    trade_date_str: str,
) -> dict:
    """
    Fetch all spx_atm rows for trade_date across TARGET_DTES.

    Returns:
        { datetime.time: { dte_int: { atm_iv, atm_forward, atm_strike, underlying_price } } }
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT quote_time, dte, atm_iv, atm_forward, atm_strike, underlying_price
            FROM spx_atm
            WHERE trade_date = %s AND dte = ANY(%s)
            ORDER BY quote_time, dte
            """,
            (trade_date_str, TARGET_DTES),
        )
        rows = cur.fetchall()

    result: dict = {}
    for row in rows:
        qt  = row["quote_time"]
        dte = int(row["dte"])
        result.setdefault(qt, {})[dte] = {
            "atm_iv":           row["atm_iv"],
            "atm_forward":      row["atm_forward"],
            "atm_strike":       row["atm_strike"],
            "underlying_price": row["underlying_price"],
        }
    return result


def fetch_day_surface(
    conn: psycopg2.extensions.connection,
    trade_date_str: str,
) -> dict:
    """
    Fetch all spx_surface rows for trade_date across TARGET_DTES and SURFACE_DELTAS.

    Returns:
        { datetime.time: { dte_int: { put_delta_int: { iv, strike } } } }
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT quote_time, dte, put_delta, iv, strike
            FROM spx_surface
            WHERE trade_date = %s AND dte = ANY(%s) AND put_delta = ANY(%s)
            ORDER BY quote_time, dte, put_delta
            """,
            (trade_date_str, TARGET_DTES, SURFACE_DELTAS),
        )
        rows = cur.fetchall()

    result: dict = {}
    for row in rows:
        qt  = row["quote_time"]
        dte = int(row["dte"])
        pd  = int(row["put_delta"])
        result.setdefault(qt, {}).setdefault(dte, {})[pd] = {
            "iv":     row["iv"],
            "strike": row["strike"],
        }
    return result


def fetch_processed_quote_times(
    conn: psycopg2.extensions.connection,
    trade_date_str: str,
) -> set:
    """
    Return the set of quote_times already in surface_metrics_core for this date.
    Used by the backfill to skip already-completed snapshots.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT quote_time FROM surface_metrics_core WHERE trade_date = %s",
            (trade_date_str,),
        )
        return {row[0] for row in cur.fetchall()}
