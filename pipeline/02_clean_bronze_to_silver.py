"""
Stage 2 — Clean: bronze Parquet  ->  silver Parquet.

Two jobs:
  1. Profile null rates for every column across all bronze files and save a
     chart to docs/null_profile.png — this is what drives the keep / drop
     column decisions documented in the report.
  2. Write cleaned files to the silver layer: SELECT DISTINCT (row-level
     dedupe) and lower-case every column name so downstream SQL is
     case-stable.

    python pipeline/02_clean_bronze_to_silver.py

Input : data/bronze/flights_*.parquet
Output: data/silver/flights_*.parquet   +   docs/null_profile.png
"""
import glob
import os

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mtick

from config import BRONZE_DIR, SILVER_DIR, DOCS_DIR, ensure_dirs

GLOB_PATH = (BRONZE_DIR / "flights_*.parquet").as_posix()
CHART_OUT = DOCS_DIR / "null_profile.png"

# Original column names by decision group (used to colour the null chart).
KEEP_COLS = {
    "FlightDate", "Marketing_Airline_Network", "Origin", "OriginCityName",
    "OriginState", "Dest", "DestCityName", "DestState", "DepDelay", "DepDel15",
    "ArrDelay", "ArrDel15", "Cancelled", "CancellationCode", "Diverted",
    "Flights", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
    "LateAircraftDelay",
}
MAYBE_KEEP_COLS = {
    "DayOfWeek", "CRSDepTime", "DepTimeBlk", "ArrTimeBlk", "TaxiOut",
    "TaxiIn", "Distance", "WheelsOff", "WheelsOn",
    "IATA_Code_Operating_Airline", "Operating_Airline",
}


def get_group(col):
    if col in KEEP_COLS:
        return "Keep"
    if col in MAYBE_KEEP_COLS:
        return "Maybe Keep"
    return "Unknown"


def main() -> None:
    ensure_dirs(SILVER_DIR, DOCS_DIR)

    bronze_files = sorted(glob.glob(str(BRONZE_DIR / "flights_*.parquet")))
    if not bronze_files:
        print("No bronze files found. Run 01_ingest_raw_to_bronze.py first.")
        return

    con = duckdb.connect()

    # Discover columns from the whole set.
    cols = [
        row[0] for row in
        con.execute(f"DESCRIBE SELECT * FROM read_parquet('{GLOB_PATH}')").fetchall()
    ]
    print(f"Columns found: {len(cols)}")

    # ── STEP 1: NULL PROFILING ────────────────────────────────────────────────
    print(f"\nProfiling nulls across all {len(bronze_files)} files (~30s)...")
    null_exprs = ",\n  ".join(
        [f'COUNT(*) FILTER (WHERE "{c}" IS NULL) * 100.0 / COUNT(*) AS "{c}"'
         for c in cols]
    )
    null_df = con.execute(
        f"SELECT\n  {null_exprs}\nFROM read_parquet('{GLOB_PATH}')"
    ).fetchdf()

    null_pct = null_df.iloc[0].sort_values(ascending=False)
    null_pct = null_pct[null_pct > 0]   # only columns that actually have nulls

    print(f"\n{'Column':<35} {'Null %':>8}   Group")
    print("-" * 58)
    for col, pct in null_pct.items():
        print(f"{col:<35} {pct:>7.2f}%   {get_group(col)}")

    # ── STEP 2: NULL PROFILE CHART ───────────────────────────────────────────
    GROUP_COLORS = {"Keep": "#e05c5c", "Maybe Keep": "#f5a623"}

    groups = [get_group(c) for c in null_pct.index]
    bar_colors = [GROUP_COLORS[g] for g in groups]

    # Flip so highest % is at top of horizontal bar chart
    labels_rev = list(null_pct.index[::-1])
    values_rev = list(null_pct.values[::-1])
    colors_rev = bar_colors[::-1]

    fig, ax = plt.subplots(figsize=(11, max(4, len(null_pct) * 0.55)))
    bars = ax.barh(labels_rev, values_rev, color=colors_rev, edgecolor="white", height=0.6)

    for bar, val in zip(bars, values_rev):
        ax.text(
            bar.get_width() + 0.4,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.1f}%",
            va="center", fontsize=8.5
        )

    ax.set_xlabel("% Null", fontsize=10)
    ax.set_title("Null % by Column  (Jan 2018 – Jan 2025, ~49.7M rows)", fontsize=12, fontweight="bold")
    ax.xaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_xlim(0, null_pct.max() * 1.18)
    ax.tick_params(axis="y", labelsize=8.5)
    ax.tick_params(axis="x", labelsize=8.5)
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)

    legend_patches = [mpatches.Patch(color=c, label=g) for g, c in GROUP_COLORS.items()]
    ax.legend(handles=legend_patches, title="Decision group", fontsize=9, title_fontsize=9)

    plt.tight_layout()
    plt.savefig(CHART_OUT, dpi=150)
    plt.close()
    print(f"\nChart saved -> {CHART_OUT}")

    # ── STEP 3: DEDUPLICATE + LOWER-CASE COLUMN NAMES + SAVE ─────────────────
    col_rename = {c: c.lower() for c in cols}

    print(f"\nCleaning + deduplicating {len(bronze_files)} files...\n")
    for i, fpath in enumerate(bronze_files, 1):
        fname = os.path.basename(fpath)
        out_path = SILVER_DIR / fname   # same name, silver layer

        select_cols = ",\n        ".join(
            [f'"{orig}" AS "{renamed}"' for orig, renamed in col_rename.items()]
        )

        con.execute(f"""
            COPY (
                SELECT DISTINCT
                    {select_cols}
                FROM read_parquet('{os.path.abspath(fpath)}')
            )
            TO '{out_path.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        print(f"  [{i:02d}/{len(bronze_files)}] -> silver/{fname}")

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
