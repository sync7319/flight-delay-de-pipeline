# `data/` — local lakehouse

The pipeline follows a **medallion architecture**. Each layer is a directory of
Parquet files; contents are gitignored (large + fully regenerable), the folder
structure is kept via `.gitkeep`.

| Layer | Path | What lives here | Produced by |
|-------|------|-----------------|-------------|
| Raw | `data/raw/` | Source BTS CSVs, one per month (external input — you download these) | — |
| Bronze | `data/bronze/` | Raw → typed Parquet, 1:1 with source, column projection only | `01_ingest_raw_to_bronze.py` |
| Silver | `data/silver/` | Cleaned: deduped, lower-cased columns, null-profiled | `02_clean_bronze_to_silver.py` |
| Gold | `data/gold/` | Single combined + enriched table, analytics-ready | `03_enrich_silver_to_gold.py` |

**Why three layers instead of transforming in place:** each stage is
independently re-runnable and debuggable. If enrichment logic changes you
rebuild gold from silver in seconds without re-ingesting; if a source file is
re-released you re-land just that month into bronze. The storage cost of
keeping all three is trivial next to the reproducibility it buys.

## Getting the source data

Download **"Reporting Carrier On-Time Performance (1987–present)"** monthly CSVs
from the U.S. Bureau of Transportation Statistics (TranStats), unzip them into
`data/raw/`, then run the pipeline. See the top-level [README](../README.md).

Any layer can be relocated with an environment variable
(`FLIGHT_DATA_DIR`, `FLIGHT_RAW_DIR`, `FLIGHT_BRONZE_DIR`, …) — e.g. point the
whole lakehouse at a mounted S3 bucket without touching code.
