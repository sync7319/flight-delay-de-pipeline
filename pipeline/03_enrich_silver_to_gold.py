"""
Stage 3 — Enrich + combine: silver Parquet  ->  gold Parquet.

Unions every monthly silver file into one analytics-ready table and enriches
the coded dimensions into human-readable labels:
  * marketing_airline_network  ->  carrier_name   (AA -> American Airlines)
  * cancellationcode           ->  reason label   (B  -> Weather)

The result is a single ZSTD Parquet that Power BI imports directly — one file,
one schema, no joins needed at the BI layer.

    python pipeline/03_enrich_silver_to_gold.py

Input : data/silver/*.parquet
Output: data/gold/flights_combined.parquet
"""
import glob

import duckdb

from config import SILVER_DIR, GOLD_TABLE, ensure_dirs

SRC_GLOB = (SILVER_DIR / "*.parquet").as_posix()


def main() -> None:
    ensure_dirs(GOLD_TABLE.parent)

    if not glob.glob(str(SILVER_DIR / "*.parquet")):
        print("No silver files found. Run 02_clean_bronze_to_silver.py first.")
        return

    con = duckdb.connect()
    print("Combining, enriching, and writing single gold Parquet...")

    con.execute(f"""
        COPY (
            SELECT
                flightdate,
                marketing_airline_network,

                -- Full carrier name
                CASE marketing_airline_network
                    WHEN 'AA' THEN 'American Airlines'
                    WHEN 'AS' THEN 'Alaska Airlines'
                    WHEN 'B6' THEN 'JetBlue Airways'
                    WHEN 'DL' THEN 'Delta Air Lines'
                    WHEN 'F9' THEN 'Frontier Airlines'
                    WHEN 'G4' THEN 'Allegiant Air'
                    WHEN 'HA' THEN 'Hawaiian Airlines'
                    WHEN 'NK' THEN 'Spirit Airlines'
                    WHEN 'UA' THEN 'United Airlines'
                    WHEN 'VX' THEN 'Virgin America'
                    WHEN 'WN' THEN 'Southwest Airlines'
                    ELSE marketing_airline_network
                END AS carrier_name,

                operating_airline,
                iata_code_operating_airline,
                origin,
                origincityname,
                originstate,
                dest,
                destcityname,
                deststate,
                depdelay,
                depdel15,
                arrdelay,
                arrdel15,
                cancelled,

                -- Map cancellation code to readable label
                CASE cancellationcode
                    WHEN 'A' THEN 'Carrier'
                    WHEN 'B' THEN 'Weather'
                    WHEN 'C' THEN 'National Air System'
                    WHEN 'D' THEN 'Security'
                    ELSE NULL
                END AS cancellationcode,

                diverted,
                flights,
                carrierdelay,
                weatherdelay,
                nasdelay,
                securitydelay,
                lateaircraftdelay,
                dayofweek,
                crsdeptime,
                deptimeblk,
                arrtimeblk,
                taxiout,
                taxiin,
                distance,
                wheelsoff,
                wheelson

            FROM read_parquet('{SRC_GLOB}')
        )
        TO '{GOLD_TABLE.as_posix()}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    con.close()

    # Verify the written table.
    con2 = duckdb.connect()
    gold = GOLD_TABLE.as_posix()
    row_count = con2.execute(f"SELECT COUNT(*) FROM read_parquet('{gold}')").fetchone()[0]
    schema = con2.execute(f"DESCRIBE SELECT * FROM read_parquet('{gold}')").fetchdf()
    con2.close()

    size_mb = GOLD_TABLE.stat().st_size / 1024 / 1024

    print(f"\nRows    : {row_count:,}")
    print(f"Columns : {len(schema)}")
    print(f"Size    : {size_mb:.1f} MB")
    print("\nSchema:")
    print(schema[["column_name", "column_type"]].to_string(index=False))
    print(f"\nFile -> {GOLD_TABLE}")


if __name__ == "__main__":
    main()
