# Flight Delay Analysis — Data Engineering Pipeline

A batch pipeline that processes **~49.7 million U.S. flight records** from the
Bureau of Transportation Statistics (Jan 2018 – Jan 2025) into a single
analytics-ready table, with a Power BI dashboard on top answering one question:
*why are flights late?*

The pipeline follows a medallion architecture — raw CSVs land as typed Parquet,
get cleaned and deduplicated, then combined and enriched into one gold table.
All transformations are SQL running on DuckDB, and the dashboard's headline
numbers are recomputed independently in SQL as a check against the DAX. The
entire thing runs on a laptop with no infrastructure and no cost.

![Power BI dashboard — Overview](docs/dashboard_preview.png)

> Overview page of the Power BI dashboard built on the pipeline's gold table.
> Full export: [`reports/PowerBI_Dashboard_Export.pdf`](reports/PowerBI_Dashboard_Export.pdf).

---

## Architecture

Bronze → silver → gold, each layer an independently re-runnable stage.
[DuckDB](https://duckdb.org) is the compute engine throughout;
[Parquet](https://parquet.apache.org) (ZSTD) is the storage format between
layers.

```
   data/raw/                 CSV, one file per month  (external — from BTS)
       │   01_ingest_raw_to_bronze.py     project columns, CSV → Parquet
       ▼
   data/bronze/              typed Parquet, 1:1 with source
       │   02_clean_bronze_to_silver.py   null-profile, dedupe, lowercase cols
       ▼
   data/silver/              cleaned monthly Parquet
       │   03_enrich_silver_to_gold.py    union all months + enrich codes→labels
       ▼
   data/gold/                one analytics-ready table  ──►  Power BI dashboard
       │   04_validate_gold_kpis.py       recompute KPIs in SQL (QA gate)
       ▼
   verified metrics
```

| Stage | Script | In → Out | What it does |
|-------|--------|----------|--------------|
| 1 · Ingest | `pipeline/01_ingest_raw_to_bronze.py` | `raw/*.csv` → `bronze/` | Projects the ~30 relevant columns from the ~110-column BTS schema; lands each month as ZSTD Parquet via a streaming `COPY`, so a month never has to fit in memory. |
| 2 · Clean | `pipeline/02_clean_bronze_to_silver.py` | `bronze/` → `silver/` | Profiles null rates across all files (writes `docs/null_profile.png`), deduplicates with `SELECT DISTINCT`, and lower-cases column names so downstream SQL is case-stable. |
| 3 · Enrich | `pipeline/03_enrich_silver_to_gold.py` | `silver/` → `gold/` | Unions every month into one table and decodes dimensions (`AA` → American Airlines, cancellation `B` → Weather) so the BI layer needs no joins. |
| 4 · Validate | `pipeline/04_validate_gold_kpis.py` | `gold/` → stdout | Recomputes the headline KPIs in SQL and compares them against the Power BI cards. Catches DAX mistakes — wrong filter context, bad null handling — before the dashboard is trusted. |

Paths are centralized in `pipeline/config.py` and overridable by environment
variable, so the same code runs against a local `data/` folder, a CI runner, or
a mounted object-store bucket.

---

## Why DuckDB

DuckDB is an in-process columnar OLAP engine — SQLite for analytics. It fits
this project well:

- **No infrastructure.** No server, no cluster, no credentials. `pip install
  duckdb` and the whole 50M-row pipeline runs on a laptop, which also means
  anyone who clones the repo can reproduce it.
- **Native Parquet and CSV.** Stage 1 streams CSV straight to Parquet with a
  single `COPY`; later stages scan only the columns they touch thanks to
  projection pushdown.
- **Handles more data than fits in RAM.** DuckDB spills to disk, so 50M rows
  aggregate fine without hand-written batching.
- **It's just SQL.** The `CASE` enrichment and `COUNT(*) FILTER (...)` KPI
  queries would run nearly unchanged on Snowflake, BigQuery, or Spark SQL, so
  nothing here is locked in.

At ~50M rows and single-digit gigabytes compressed, this dataset sits
comfortably on one machine. Spark or a cloud warehouse would add cost, spin-up
latency, and operational overhead without making anything faster — queries here
return in seconds, which is what matters when you're iterating on
transformations and re-checking KPIs all day.

---

## Design decisions & trade-offs

- **DuckDB (embedded, single-node).** Simple and fast for a one-machine
  dataset. The cost: no multi-user concurrency, no streaming, no horizontal
  scale. If this data were 50× bigger or had concurrent writers, DuckDB would
  be the wrong tool.
- **Parquet between layers instead of CSV.** Columnar compression, real types,
  and pushdown. The cost: not human-readable — you need a reader to inspect it.
- **Medallion layers instead of transforming in place.** Each stage can be
  rebuilt independently; changing the enrichment logic rebuilds gold from
  silver in seconds without re-ingesting. The cost is roughly 3× the storage,
  which at this size is trivial.
- **`SELECT DISTINCT` for dedupe.** One line, no key management. The cost: a
  full scan-and-sort, and it can't tell "legitimately identical rows" from true
  duplicates. A production version would dedupe on a business key with a
  deterministic tie-break.
- **Full rebuild every run, not incremental.** Simple and idempotent, and the
  whole history reprocesses in minutes. Wasteful once the archive grows —
  production would load incrementally by month.
- **Power BI import mode.** Fast, snappy visuals off a compact gold Parquet.
  The cost: it's a static snapshot, and refreshing means re-importing.
  DirectQuery would be live but slower.
- **KPI validation as a script, not a test suite.** The SQL cross-check caught
  a real class of problem (DAX filter-context bugs), but it's still an eyeball
  step. In production this becomes automated tests that fail the build — see
  below.

---

## How this would differ in production

This is a local, single-node build. At real scale or on a team, the shape
changes:

| Concern | This project | Production |
|---------|--------------|------------|
| Compute | DuckDB on a laptop | Warehouse (Snowflake/BigQuery/Databricks) or Spark for distributed scale |
| Transformations | Python + inline SQL | **dbt** models — versioned, tested, documented, with lineage |
| Orchestration | run 4 scripts by hand, in order | **Airflow / Dagster / Prefect** — scheduled, retried, alerted, backfillable |
| Loading | full rebuild every run | **incremental & idempotent** — partition by month, `MERGE` on keys |
| Storage | local `data/` folder | object store (S3/GCS) with **Iceberg/Delta** for ACID + time travel |
| Data quality | manual `04_validate_kpis.py` | **dbt tests / Great Expectations** as CI gates that fail the build |
| BI | static import from Parquet | DirectQuery / a semantic layer on the warehouse |
| Ops | none | CI/CD, monitoring, schema-contract enforcement, data-freshness SLAs |

None of that would make this particular dataset faster or the numbers more
correct — it buys reliability, collaboration, and scale that a solo local build
doesn't need. And since the logic is already SQL over Parquet, moving it into
that stack is a port, not a rewrite.

---

## Dataset

**Reporting Carrier On-Time Performance (1987–present)** from the U.S. Bureau of
Transportation Statistics (BTS / TranStats). Monthly CSVs, Jan 2018 – Jan 2025,
~49.7M rows after cleaning. The data isn't committed — it's several GB and
freely re-downloadable — so the `data/` folders in this repo are empty scaffolding
until you fetch it. See [`data/README.md`](data/README.md) for the layer
definitions and download instructions.

---

## Results

Computed over the full ~49.7M-row gold table and cross-checked against the
Power BI dashboard:

| Metric | Value |
|--------|-------|
| Total flights | ~49.7M |
| Avg arrival delay | 4.79 min (early arrivals count as negative) |
| On-time arrival rate | ~81.3% (arrived within 15 min) |
| Cancellation rate | 2.2% |

The dashboard breaks delays down by cause (carrier, weather, NAS, security,
late aircraft), by carrier, and over time. Full analysis:
[`reports/Flight_Delay_Analysis_Report.pdf`](reports/Flight_Delay_Analysis_Report.pdf).

---

## Repository layout

```
flight-delay-de-pipeline/
├── pipeline/                          # the ELT stages, run in order
│   ├── config.py                      # centralized, env-overridable paths
│   ├── 01_ingest_raw_to_bronze.py
│   ├── 02_clean_bronze_to_silver.py
│   ├── 03_enrich_silver_to_gold.py
│   └── 04_validate_gold_kpis.py
├── data/                              # medallion lakehouse (gitignored contents)
│   ├── bronze/  ·  silver/  ·  gold/
│   └── README.md                      # layer definitions + how to get source data
├── docs/                              # dashboard preview + null-profile chart
└── reports/                           # written report (PDF/DOCX) + Power BI export
```

---

## Running it

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. download BTS monthly CSVs into data/raw/  (see data/README.md)

# 2. run the pipeline, in order
python pipeline/01_ingest_raw_to_bronze.py     # raw    → bronze
python pipeline/02_clean_bronze_to_silver.py   # bronze → silver  (+ null_profile.png)
python pipeline/03_enrich_silver_to_gold.py    # silver → gold
python pipeline/04_validate_gold_kpis.py       # QA gate: recompute KPIs

# 3. point Power BI at data/gold/flights_combined.parquet
```

Each stage is idempotent — safe to re-run — and skips with a clear message if
its input layer is empty.

## License

MIT — see [LICENSE](LICENSE).
