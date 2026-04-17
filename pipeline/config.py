"""
Pipeline configuration.
Override DATA_ROOT and DB_URL via environment variables (or a .env file at project root).
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# ---------------------------------------------------------------------------
# Paths and database — same env vars as interpolation project
# ---------------------------------------------------------------------------
DATA_ROOT = Path(os.environ.get("SPX_DATA_ROOT", "/data/spx_options"))
DB_URL    = os.environ.get("SPX_DB_URL", "postgresql://user:password@localhost:5432/spx")

# ---------------------------------------------------------------------------
# Target DTEs for the surface snapshot
# ---------------------------------------------------------------------------
TARGET_DTES    = [1, 7, 30, 90, 180]

# put_delta integers queried from spx_surface (ATM comes from spx_atm separately)
SURFACE_DELTAS = [10, 25, 75, 90]

# ---------------------------------------------------------------------------
# Delta label mappings
# ---------------------------------------------------------------------------
# Maps a delta label to its put_delta integer in spx_surface.
# None signals "use spx_atm" (forward ATM, k=0).
DELTA_TO_PD: dict[str, int | None] = {
    "10p": 10,
    "25p": 25,
    "25c": 75,
    "10c": 90,
    "atm": None,
}

# Integer coordinate used for convexity delta-weighting.
# ATM is treated as coordinate 50 for weighting purposes.
DELTA_TO_COORD: dict[str, int] = {
    "10p": 10,
    "25p": 25,
    "25c": 75,
    "10c": 90,
    "atm": 50,
}

# ---------------------------------------------------------------------------
# Metric definitions
# ---------------------------------------------------------------------------

# Skew slope pairs: (label_a, label_b)
# Formula: sqrt(DTE/365) * (IV_b - IV_a) / ln(K_b / K_a)
SKEW_PAIRS: list[tuple[str, str]] = [
    ("10p", "25p"),
    ("25p", "atm"),
    ("10p", "atm"),
    ("atm", "25c"),
    ("atm", "10c"),
    ("25p", "25c"),
]

# Convexity triples: (label_left, label_center, label_right)
# Formula: (w_left * IV_left + w_right * IV_right) - IV_center
# Weights are delta-interpolated using DELTA_TO_COORD values.
CONVEXITY_TRIPLES: list[tuple[str, str, str]] = [
    ("10p", "25p", "atm"),   # coords: 10, 25, 50
    ("atm", "25c", "10c"),   # coords: 50, 75, 90
    ("25p", "atm", "25c"),   # coords: 25, 50, 75
]

# Term slope DTE pairs: (dte_a, dte_b) — forward vol between the two tenors
# Formula: sqrt((IV_b^2 * T_b - IV_a^2 * T_a) / (T_b - T_a)), NULL if negative
TERM_SLOPE_PAIRS:  list[tuple[int, int]] = [(1, 7), (7, 30), (30, 90)]
TERM_SLOPE_DELTAS: list[str]             = ["25p", "atm", "25c"]

# Term ratio DTE pairs: iv_a / iv_b using ATM IV
TERM_RATIO_PAIRS: list[tuple[int, int]] = [(1, 7), (7, 30), (30, 90)]

# ---------------------------------------------------------------------------
# Parquet column names (step-2 output schema)
# ---------------------------------------------------------------------------
PARQUET_COLS: dict[str, str] = {
    "trade_date":       "trade_date",
    "quote_time":       "quote_time",
    "strike":           "strike",
    "option_type":      "right",           # actual parquet column name
    "settlement":       "settlement",
    "bid":              "bid",
    "ask":              "ask",
    "iv":               "implied_vol",
    "underlying_price": "underlying_price",
    "dte":              "dte",
}

# Step-2 flag columns — rows where any True are skipped for VIX calculation
STEP2_FLAG_COLS: list[str] = [
    "flag_crossed_market",
    "flag_zero_bid",
    "flag_negative_extrinsic",
    "flag_iv_missing",
    "flag_iv_extreme_high",
    "flag_iv_extreme_low",
]

# ---------------------------------------------------------------------------
# PCP regression settings (for VIX forward price estimation)
# ---------------------------------------------------------------------------
PCP_MONEYNESS_BAND = 0.15
R_MIN              = -0.05
R_MAX              =  0.20
R_DEFAULT          =  0.05
MIN_OPTION_PRICE   =  0.05

# ---------------------------------------------------------------------------
# Expiry settlement times (Eastern, 24h)
# ---------------------------------------------------------------------------
PM_EXPIRY_HOUR,   PM_EXPIRY_MINUTE   = 16, 15
AM_EXPIRY_HOUR,   AM_EXPIRY_MINUTE   =  9, 30
MINUTES_PER_YEAR: float              = 365.0 * 24.0 * 60.0
