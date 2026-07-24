"""
Query layer over the partitioned Parquet lake. DuckDB stands in here for
Presto/Trino/Spark: it understands Hive-style partitioning and pushes
predicates down to the file scan the same way those engines do, so the
partition-pruning behavior demonstrated below is the same principle at
local scale. In production this SQL would run unchanged (modulo dialect)
against Athena/Presto over S3.

Run:
    python -m src.query_examples --date 2026-07-23
"""
import argparse
import time
from datetime import date, datetime
from pathlib import Path

import duckdb

BASE = Path(__file__).resolve().parent.parent
LAKE_DIR = BASE / "data" / "lake"


def _glob(table: str) -> str:
    return str(LAKE_DIR / table / "**" / "*.parquet")


def strategy_pnl_summary(con: duckdb.DuckDBPyConnection, run_date: date) -> None:
    """Unpruned scan: reads every partition, then filters."""
    q_unpruned = f"""
        SELECT strategy_id, date, pnl_usd, gross_exposure_usd, net_exposure_usd
        FROM read_parquet('{_glob("strategy_pnl")}', hive_partitioning=1)
        WHERE date = DATE '{run_date.isoformat()}'
        ORDER BY strategy_id
    """
    t0 = time.perf_counter()
    result = con.execute(q_unpruned).fetchdf()
    elapsed = time.perf_counter() - t0
    print(f"\n-- strategy_pnl for {run_date.isoformat()} ({elapsed*1000:.2f}ms) --")
    print(result.to_string(index=False))

    print("\n-- EXPLAIN (note the Hive partition filter DuckDB pushes down) --")
    plan = con.execute(f"EXPLAIN {q_unpruned}").fetchall()
    for row in plan:
        print(row[-1] if isinstance(row, tuple) else row)


def top_signals(con: duckdb.DuckDBPyConnection, run_date: date, n: int = 5) -> None:
    q = f"""
        SELECT strategy_id, ticker, signal_score, position_target_usd
        FROM read_parquet('{_glob("strategy_signals")}', hive_partitioning=1)
        WHERE date = DATE '{run_date.isoformat()}'
        ORDER BY abs(signal_score) DESC
        LIMIT {n}
    """
    print(f"\n-- top {n} signals by |score| for {run_date.isoformat()} --")
    print(con.execute(q).fetchdf().to_string(index=False))


def cross_strategy_net_exposure(con: duckdb.DuckDBPyConnection, run_date: date) -> None:
    q = f"""
        SELECT date, SUM(net_exposure_usd) AS firm_net_exposure_usd, SUM(gross_exposure_usd) AS firm_gross_exposure_usd
        FROM read_parquet('{_glob("strategy_pnl")}', hive_partitioning=1)
        WHERE date = DATE '{run_date.isoformat()}'
        GROUP BY date
    """
    print(f"\n-- firm-wide exposure roll-up for {run_date.isoformat()} --")
    print(con.execute(q).fetchdf().to_string(index=False))


def main(run_date: date) -> None:
    con = duckdb.connect()
    strategy_pnl_summary(con, run_date)
    top_signals(con, run_date)
    cross_strategy_net_exposure(con, run_date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=date.today().isoformat())
    args = parser.parse_args()
    run_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    main(run_date)
