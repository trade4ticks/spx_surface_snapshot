"""
build_catalog.py — generate and UPSERT surface_metrics_catalog rows.

One row per column of surface_metrics_core, including all W1/W2/W3 levels
and W4 transforms. Re-run any time the column set changes.

Generator-style: derives column names from the same lists in pipeline/config.py
that drive the compute pipeline, so the catalog never falls out of sync.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

# Allow running as a script from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import psycopg2.extras

from pipeline.config import (
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
from pipeline.db import get_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

_CATALOG_SQL_PATH = Path(__file__).resolve().parent.parent / "sql" / "catalog.sql"


# ---------------------------------------------------------------------------
# Skip rules — must mirror sql/w4_alter.sql
# ---------------------------------------------------------------------------

_NO_TRANSFORM = {
    "trade_date", "quote_time",
    "day_of_week", "days_to_monthly_opex",
    "spot",
    "forward_1d", "forward_7d", "forward_30d", "forward_90d", "forward_180d",
}

_Z_ONLY = {
    "log_ret_d", "log_ret_1w", "log_ret_1m",
    "vov_30d_1m",
    "spot_vol_30d_1m", "spot_vol_30d_3m",
}

_WING_DESC = {
    "10p": "10-delta put",
    "25p": "25-delta put",
    "atm": "at-the-money forward (k=0)",
    "25c": "25-delta call",
    "10c": "10-delta call",
}


def _row(**kwargs) -> dict:
    """Default-fill a catalog row."""
    base = {
        "column_name": None, "family": None, "tenor": None, "wing": None,
        "form": "level", "base_column": None, "units": None,
        "description": None, "formula": None,
    }
    base.update(kwargs)
    return base


# ---------------------------------------------------------------------------
# Base (level) metric rows
# ---------------------------------------------------------------------------

def base_metric_rows():
    # ── PK / calendar ────────────────────────────────────────────────────
    yield _row(column_name="trade_date", family="meta", units="date",
               description="Trading date")
    yield _row(column_name="quote_time", family="meta", units="time",
               description="Intraday quote time (5-min bar)")
    yield _row(column_name="day_of_week", family="meta", units="int",
               description="0=Mon, 4=Fri")
    yield _row(column_name="days_to_monthly_opex", family="meta", units="int",
               description="Calendar days to next monthly OPEX (3rd Friday)")

    # ── Spot / forward ───────────────────────────────────────────────────
    yield _row(column_name="spot", family="spot", units="price",
               description="Underlying spot from spx_atm.underlying_price")
    for dte in TARGET_DTES:
        yield _row(
            column_name=f"forward_{dte}d", family="forward",
            tenor=f"{dte}d", units="price",
            description=f"ATM forward at {dte}-day calendar tenor",
            formula=f"spx_atm.atm_forward where dte={dte}",
        )

    # ── IV matrix ────────────────────────────────────────────────────────
    for dte in TARGET_DTES:
        for label in IV_MATRIX_DELTAS:
            yield _row(
                column_name=f"iv_{dte}d_{label}", family="iv",
                tenor=f"{dte}d", wing=label, units="vol_decimal",
                description=f"Implied vol at {dte}d calendar tenor, "
                            f"{_WING_DESC[label]}",
            )

    # ── VIX (CBOE variance swap, interpolated to tenor) ──────────────────
    for dte in TARGET_DTES:
        yield _row(
            column_name=f"vix_{dte}d", family="vix",
            tenor=f"{dte}d", units="vol_decimal",
            description=f"CBOE variance-swap implied vol at {dte}d tenor, "
                        f"interpolated in total-variance space",
            formula="sqrt(2/T·Σ ΔK_i/K_i² e^(rT) Q(K_i) - (F/K0 - 1)²/T)",
        )

    # ── Term ratios ─────────────────────────────────────────────────────
    for a, b in TERM_RATIO_PAIRS:
        yield _row(
            column_name=f"term_ratio_{a}d_{b}d", family="term_ratio",
            tenor=f"{a}d_{b}d", units="ratio",
            description=f"ATM IV ratio: iv_{a}d_atm / iv_{b}d_atm",
            formula=f"iv_{a}d_atm / iv_{b}d_atm",
        )

    # ── Skew slopes (sqrt-T-scaled, log-strike normalized) ──────────────
    for dte in TARGET_DTES:
        for la, lb in SKEW_PAIRS:
            yield _row(
                column_name=f"skew_{dte}d_{la}_{lb}", family="skew",
                tenor=f"{dte}d", wing=f"{la}_{lb}", units="slope",
                description=f"Sqrt-T-scaled IV slope between {la} and {lb} at "
                            f"{dte}d tenor",
                formula=f"sqrt({dte}/365) × (iv_{lb} - iv_{la}) / "
                        f"ln(K_{lb} / K_{la})",
            )

    # ── Term slopes (forward vol between two tenors) ────────────────────
    for a, b in TERM_SLOPE_PAIRS:
        for delta in TERM_SLOPE_DELTAS:
            yield _row(
                column_name=f"term_slope_{a}_{b}_{delta}", family="term_slope",
                tenor=f"{a}d_{b}d", wing=delta, units="vol_decimal",
                description=f"Annualized forward vol from {a}d to {b}d at "
                            f"{delta} delta",
                formula=f"sqrt((iv_{b}d_{delta}² × T_{b} - "
                        f"iv_{a}d_{delta}² × T_{a}) / (T_{b} - T_{a}))",
            )

    # ── Convexity (delta-weighted curvature) ────────────────────────────
    for dte in TARGET_DTES:
        for ll, lc, lr in CONVEXITY_TRIPLES:
            yield _row(
                column_name=f"convex_{dte}d_{ll}_{lc}_{lr}", family="convex",
                tenor=f"{dte}d", wing=f"{ll}_{lc}_{lr}", units="vol_decimal",
                description=f"Smile curvature at {dte}d: weighted chord "
                            f"({ll},{lr}) minus center {lc}",
                formula=f"(w_l × iv_{ll} + w_r × iv_{lr}) - iv_{lc};  "
                        f"weights delta-interpolated",
            )

    # ── W2: Risk reversal ───────────────────────────────────────────────
    for dte, delta in RISK_REVERSAL_SPECS:
        yield _row(
            column_name=f"rr_{dte}d_{delta}", family="rr",
            tenor=f"{dte}d", wing=f"{delta}d", units="vol_decimal",
            description=f"{delta}-delta risk reversal at {dte}d "
                        f"(call IV − put IV)",
            formula=f"iv_{dte}d_{delta}c - iv_{dte}d_{delta}p",
        )

    # ── W2: VIX basis ───────────────────────────────────────────────────
    for a, b in VIX_BASIS_PAIRS:
        yield _row(
            column_name=f"vix_basis_{a}d_{b}d", family="vix_basis",
            tenor=f"{a}d_{b}d", units="vol_decimal",
            description=f"VIX term spread: vix_{a}d − vix_{b}d",
            formula=f"vix_{a}d - vix_{b}d",
        )

    # ── W3: Spot log returns ────────────────────────────────────────────
    for period, td in (("d", 1), ("1w", 5), ("1m", 21)):
        yield _row(
            column_name=f"log_ret_{period}", family="log_ret",
            tenor=period, units="log_return",
            description=f"Log return over {td} trading day"
                        f"{'s' if td > 1 else ''} (snapshot-aligned)",
            formula=f"ln(spot_t / spot_{{t-{td}td, same quote_time}})",
        )

    # ── W3: Realized vol ────────────────────────────────────────────────
    for period, td in (("1w", 5), ("1m", 21), ("3m", 63)):
        yield _row(
            column_name=f"rv_{period}", family="rv",
            tenor=period, units="vol_decimal",
            description=f"Realized vol over trailing {td} trading days, "
                        f"annualized × √252",
            formula=f"stdev(log_ret_d over last {td}td) × sqrt(252)",
        )

    # ── W3: VRP (and ratio) ─────────────────────────────────────────────
    vrp_iv = {"1w": "iv_7d_atm", "1m": "iv_30d_atm", "3m": "iv_90d_atm"}
    for period, iv_col in vrp_iv.items():
        yield _row(
            column_name=f"vrp_{period}", family="vrp",
            tenor=period, units="vol_decimal",
            description=f"Vol risk premium: {iv_col} − rv_{period}",
            formula=f"{iv_col} - rv_{period}",
        )
        yield _row(
            column_name=f"vrp_ratio_{period}", family="vrp_ratio",
            tenor=period, units="ratio",
            description=f"VRP ratio: {iv_col} / rv_{period} "
                        f"(blows up when RV is small)",
            formula=f"{iv_col} / rv_{period}",
        )

    # ── W3: Vol of vol ──────────────────────────────────────────────────
    yield _row(
        column_name="vov_30d_1m", family="vov",
        tenor="30d", wing="1m", units="vol_decimal",
        description="21-trading-day stdev of daily Δ iv_30d_atm — vol of vol",
        formula="stdev(chg_d_iv_30d_atm over last 21td)",
    )

    # ── W3: Spot–vol correlation ────────────────────────────────────────
    for window, td in (("1m", 21), ("3m", 63)):
        yield _row(
            column_name=f"spot_vol_30d_{window}", family="spot_vol",
            tenor="30d", wing=window, units="corr",
            description=f"{td}-trading-day rolling corr( log_ret_d, "
                        f"chg_d_iv_30d_atm )",
            formula=f"corr(log_ret_d, chg_d_iv_30d_atm) over last {td}td",
        )


# ---------------------------------------------------------------------------
# Transform rows (chg_d / chg_1w / z)
# ---------------------------------------------------------------------------

def transform_rows(base_rows):
    for base in base_rows:
        col = base["column_name"]
        if col in _NO_TRANSFORM:
            continue
        z_only = col in _Z_ONLY

        # z (always emitted for eligible bases)
        yield _row(
            column_name=f"z_{col}", family=base["family"],
            tenor=base["tenor"], wing=base["wing"],
            form="z", base_column=col, units="z_score",
            description=f"63-trading-day rolling z-score of {col} "
                        f"(snapshot-aligned)",
            formula=f"({col}_t - μ) / σ over last 63td at same quote_time",
        )

        if z_only:
            continue

        yield _row(
            column_name=f"chg_d_{col}", family=base["family"],
            tenor=base["tenor"], wing=base["wing"],
            form="chg_d", base_column=col, units=base["units"],
            description=f"Daily change of {col} (snapshot-aligned)",
            formula=f"{col}_t - {col}_{{t-1td, same quote_time}}",
        )
        yield _row(
            column_name=f"chg_1w_{col}", family=base["family"],
            tenor=base["tenor"], wing=base["wing"],
            form="chg_1w", base_column=col, units=base["units"],
            description=f"Weekly (5td) change of {col} (snapshot-aligned)",
            formula=f"{col}_t - {col}_{{t-5td, same quote_time}}",
        )


def build_catalog_rows() -> list[dict]:
    bases = list(base_metric_rows())
    transforms = list(transform_rows(bases))
    return bases + transforms


# ---------------------------------------------------------------------------
# DB write
# ---------------------------------------------------------------------------

_UPSERT = """
INSERT INTO surface_metrics_catalog
    (column_name, family, tenor, wing, form, base_column,
     units, description, formula)
VALUES
    (%(column_name)s, %(family)s, %(tenor)s, %(wing)s, %(form)s, %(base_column)s,
     %(units)s, %(description)s, %(formula)s)
ON CONFLICT (column_name) DO UPDATE SET
    family      = EXCLUDED.family,
    tenor       = EXCLUDED.tenor,
    wing        = EXCLUDED.wing,
    form        = EXCLUDED.form,
    base_column = EXCLUDED.base_column,
    units       = EXCLUDED.units,
    description = EXCLUDED.description,
    formula     = EXCLUDED.formula
"""


def main() -> None:
    rows = build_catalog_rows()
    log.info("Generated %d catalog rows", len(rows))

    by_form = {}
    for r in rows:
        by_form[r["form"]] = by_form.get(r["form"], 0) + 1
    log.info("  by form: %s", by_form)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(_CATALOG_SQL_PATH.read_text())
            psycopg2.extras.execute_batch(cur, _UPSERT, rows, page_size=500)
        conn.commit()
        log.info("Catalog populated.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
