"""
Stage 4 — Validate: recompute the headline KPIs straight from the gold table.

This is the data-quality gate. The same numbers are computed independently in
Power BI with DAX; running them here in SQL catches any DAX misconfiguration
(wrong filter context, null handling, double counting) before the dashboard is
trusted. In production this becomes a set of assertions in CI (dbt tests /
Great Expectations) rather than an eyeball check.

    python pipeline/04_validate_gold_kpis.py

Input: data/gold/flights_combined.parquet
"""
import duckdb

from config import GOLD_TABLE


def main() -> None:
    if not GOLD_TABLE.exists():
        print("Gold table not found. Run 03_enrich_silver_to_gold.py first.")
        return

    con = duckdb.connect()
    result = con.execute(f"""
        SELECT
            -- Total Flights
            COUNT(*) AS total_flights,

            -- Avg Arrival Delay (matches Power BI AVERAGE(arrdelay) — includes negatives for early arrivals)
            AVG(arrdelay) AS avg_arrival_delay,

            -- Arrival On-Time % (Arrival Only): flights where arrdel15=0 / flights where arrdel15 is not null
            COUNT(*) FILTER (WHERE arrdel15 = 0) * 100.0 /
                COUNT(*) FILTER (WHERE arrdel15 IS NOT NULL) AS on_time_pct_arrival_only,

            -- On-Time % (Dep or Arr): both depdel15=0 AND arrdel15=0
            COUNT(*) FILTER (WHERE arrdel15 = 0 AND depdel15 = 0) * 100.0 /
                COUNT(*) FILTER (WHERE arrdel15 IS NOT NULL AND depdel15 IS NOT NULL) AS on_time_pct_dep_or_arr,

            -- Cancellation % (cancelled flag / total rows)
            SUM(cancelled) * 100.0 / COUNT(*) AS cancellation_pct,

            -- Extra context
            SUM(cancelled) AS total_cancelled,
            COUNT(*) FILTER (WHERE arrdel15 IS NULL) AS arrdel15_nulls
        FROM read_parquet('{GOLD_TABLE.as_posix()}')
    """).fetchdf()

    print("\n=== KPI VERIFICATION ===\n")
    print(f"Total Flights            : {int(result['total_flights'][0]):>15,}")
    print(f"Avg Arrival Delay (min)  : {result['avg_arrival_delay'][0]:>15.4f}")
    print(f"On-Time % (Arrival Only) : {result['on_time_pct_arrival_only'][0]:>14.2f}%")
    print(f"On-Time % (Dep or Arr)   : {result['on_time_pct_dep_or_arr'][0]:>14.2f}%")
    print(f"Cancellation %           : {result['cancellation_pct'][0]:>14.2f}%")
    print()
    print(f"(Total Cancelled         : {int(result['total_cancelled'][0]):>15,})")
    print(f"(Rows with null arrdel15 : {int(result['arrdel15_nulls'][0]):>15,})")

    print("\n=== POWER BI CARDS (expected) ===")
    print("Total Flights   : 50M      -> verify matches ~49.7M above")
    print("Avg Arrival D.. : 4.79     -> verify matches above")
    print("Arrival On-Time : 81.3%    -> verify matches on_time_pct_arrival_only")
    print("Cancellation %  : 2.2%     -> verify matches cancellation_pct")

    con.close()


if __name__ == "__main__":
    main()
