"""
Stage 1 — Ingest: raw CSV  ->  bronze Parquet.

Reads the raw BTS On-Time Performance CSVs (one file per month), projects the
columns we care about, and writes one ZSTD-compressed Parquet per month. This
is a 1:1, near-lossless landing of the source into a columnar format — no
business logic yet, so the bronze layer stays cheap to re-derive.

DuckDB reads the CSVs directly and streams straight to Parquet via COPY, so a
month of data never has to be fully materialised in Python.

    python pipeline/01_ingest_raw_to_bronze.py

Input : data/raw/*.csv        (override with FLIGHT_RAW_DIR)
Output: data/bronze/flights_<YYYY>_<M>.parquet
"""
import glob
import os

import duckdb

from config import RAW_DIR, BRONZE_DIR, ensure_dirs

# Columns retained from the ~110-column BTS schema.
# KEEP = core delay/cancellation facts; MAYBE KEEP = useful dimensions.
COLUMNS = [
    # --- KEEP ---
    "FlightDate",
    "Marketing_Airline_Network",
    "Origin",
    "OriginCityName",
    "OriginState",
    "Dest",
    "DestCityName",
    "DestState",
    "DepDelay",
    "DepDel15",
    "ArrDelay",
    "ArrDel15",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "Flights",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay",
    # --- MAYBE KEEP ---
    "DayOfWeek",
    "CRSDepTime",
    "DepTimeBlk",
    "ArrTimeBlk",
    "TaxiOut",
    "TaxiIn",
    "Distance",
    "WheelsOff",
    "WheelsOn",
    "IATA_Code_Operating_Airline",
    "Operating_Airline",
]

col_select = ", ".join(f'"{c}"' for c in COLUMNS)


def main() -> None:
    ensure_dirs(BRONZE_DIR)

    csv_files = sorted(glob.glob(str(RAW_DIR / "*.csv")))
    print(f"Found {len(csv_files)} CSV files in {RAW_DIR}\n")
    if not csv_files:
        print("Nothing to ingest. Download BTS CSVs into data/raw/ first "
              "(see README → Dataset).")
        return

    con = duckdb.connect()
    for i, csv_path in enumerate(csv_files, 1):
        fname = os.path.basename(csv_path)
        # Filenames look like "...)_2018_1.csv" -> stem "_2018_1"
        stem = fname.replace(".csv", "").split(")")[-1]
        out_path = BRONZE_DIR / f"flights{stem}.parquet"

        con.execute(f"""
            COPY (
                SELECT {col_select}
                FROM read_csv_auto('{os.path.abspath(csv_path)}', header=true)
            )
            TO '{out_path.as_posix()}'
            (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        print(f"[{i:02d}/{len(csv_files)}] -> {out_path.name}")

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
