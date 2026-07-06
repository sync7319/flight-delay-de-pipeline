"""
Shared paths for the flight-delay medallion pipeline.

Layout (a local lakehouse):
    data/
      raw/      source CSVs downloaded from BTS   (external input)
      bronze/   raw → typed Parquet, 1:1 with source, minimal transformation
      silver/   cleaned: deduped, lower-cased columns, profiled
      gold/     combined + enriched, analytics-ready single table

Every path is overridable via environment variables so the same code runs on a
laptop, in CI, or against a mounted object-store bucket without edits.
"""
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Root of the local lakehouse. Point FLIGHT_DATA_DIR at anything (e.g. an S3
# mount) to relocate every layer at once.
DATA_DIR = Path(os.getenv("FLIGHT_DATA_DIR", REPO_ROOT / "data"))

RAW_DIR = Path(os.getenv("FLIGHT_RAW_DIR", DATA_DIR / "raw"))
BRONZE_DIR = Path(os.getenv("FLIGHT_BRONZE_DIR", DATA_DIR / "bronze"))
SILVER_DIR = Path(os.getenv("FLIGHT_SILVER_DIR", DATA_DIR / "silver"))
GOLD_DIR = Path(os.getenv("FLIGHT_GOLD_DIR", DATA_DIR / "gold"))
DOCS_DIR = REPO_ROOT / "docs"

GOLD_TABLE = GOLD_DIR / "flights_combined.parquet"


def ensure_dirs(*dirs: Path) -> None:
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
