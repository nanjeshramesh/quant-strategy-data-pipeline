"""
Reads each strategy's raw, bespoke output, parses it into the canonical
schema (schema_contracts.py), validates it, and writes it to a
strategy/date-partitioned Parquet lake.

Design choices worth calling out in an interview:
  - Each source has its own parser (parse_strategy_a/b/c). A change in one
    source's raw format can only break that source's parser -- it can't
    silently corrupt the others. This is the "isolate blast radius" pattern.
  - A parser failure does not fail the whole run. It's caught, logged, and
    the bad partition is quarantined so the rest of the batch still lands.
    That's what makes "reporting lag from 24h to 3h" possible in practice --
    one broken upstream source shouldn't hold every other strategy hostage.
  - Writing is idempotent: re-running the same date overwrites that
    partition rather than appending, so backfills are safe to re-run.

Run:
    python -m src.standardize --date 2026-07-23
"""
import argparse
import json
import shutil
import traceback
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pandera.pandas as pa

from src.schema_contracts import SCHEMAS

BASE = Path(__file__).resolve().parent.parent
RAW_DIR = BASE / "data" / "raw"
LAKE_DIR = BASE / "data" / "lake"
QUARANTINE_DIR = BASE / "data" / "quarantine"


class SchemaDriftError(Exception):
    """Raised when a raw source no longer matches the columns our parser expects."""


def _require_columns(df: pd.DataFrame, cols: list[str], source: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SchemaDriftError(
            f"[{source}] missing expected column(s) {missing}; got columns={list(df.columns)}"
        )


def parse_strategy_a(run_date: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    path = RAW_DIR / "strategy_a" / f"{run_date.isoformat()}.csv"
    df = pd.read_csv(path)
    _require_columns(df, ["strat", "dt", "ticker", "score", "target_pos", "pnl", "gross", "net"], "strategy_a")

    signals = pd.DataFrame({
        "strategy_id": df["strat"],
        "date": pd.to_datetime(df["dt"]),
        "ticker": df["ticker"],
        "signal_score": df["score"],
        "position_target_usd": df["target_pos"],
    })
    pnl = pd.DataFrame({
        "strategy_id": ["strategy_a"],
        "date": [pd.to_datetime(run_date)],
        "pnl_usd": [df["pnl"].iloc[0]],
        "gross_exposure_usd": [df["gross"].iloc[0]],
        "net_exposure_usd": [df["net"].iloc[0]],
    })
    return signals, pnl


# PnL column names strategy_b has used over time. ProfitLossUSD was the
# original name; strategy_b's team renamed it to PnLUSD without notice on
# 2026-07-24 (see runbooks/schema_drift_incident.md). Rather than hard-fail
# on that specific rename forever, the parser now accepts either name --
# this list is the fix, and the incident is the regression test for it
# (tests/test_pipeline_integration.py::test_previously_incident_causing_column_rename_now_handled).
_STRATEGY_B_PNL_COLUMN_ALIASES = ["ProfitLossUSD", "PnLUSD"]


def parse_strategy_b(run_date: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    path = RAW_DIR / "strategy_b" / f"{run_date.isoformat()}.csv"
    df = pd.read_csv(path)
    _require_columns(
        df, ["StrategyID", "Date", "Symbol", "SignalScore", "PositionTarget", "GrossExposure", "NetExposure"], "strategy_b"
    )

    pnl_col = next((c for c in _STRATEGY_B_PNL_COLUMN_ALIASES if c in df.columns), None)
    if pnl_col is None:
        raise SchemaDriftError(
            f"[strategy_b] missing PnL column; expected one of {_STRATEGY_B_PNL_COLUMN_ALIASES}, "
            f"got columns={list(df.columns)}"
        )

    signals = pd.DataFrame({
        "strategy_id": df["StrategyID"],
        "date": pd.to_datetime(df["Date"], format="%m/%d/%Y"),
        "ticker": df["Symbol"],
        "signal_score": df["SignalScore"],
        "position_target_usd": df["PositionTarget"],
    })
    pnl = pd.DataFrame({
        "strategy_id": ["strategy_b"],
        "date": [pd.to_datetime(run_date)],
        "pnl_usd": [df[pnl_col].iloc[0]],
        "gross_exposure_usd": [df["GrossExposure"].iloc[0]],
        "net_exposure_usd": [df["NetExposure"].iloc[0]],
    })
    return signals, pnl


def parse_strategy_c(run_date: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    path = RAW_DIR / "strategy_c" / f"{run_date.isoformat()}.json"
    payload = json.loads(path.read_text())
    for key in ("meta", "positions", "pnl"):
        if key not in payload:
            raise SchemaDriftError(f"[strategy_c] missing top-level key '{key}'")

    positions = pd.DataFrame(payload["positions"])
    signals = pd.DataFrame({
        "strategy_id": payload["meta"]["strategy"],
        "date": pd.to_datetime(payload["meta"]["date"]),
        "ticker": positions["ticker"],
        "signal_score": positions["signal"],
        "position_target_usd": positions["target"],
    })
    pnl = pd.DataFrame({
        "strategy_id": ["strategy_c"],
        "date": [pd.to_datetime(run_date)],
        "pnl_usd": [payload["pnl"]["usd"]],
        "gross_exposure_usd": [payload["pnl"]["gross"]],
        "net_exposure_usd": [payload["pnl"]["net"]],
    })
    return signals, pnl


PARSERS = {
    "strategy_a": parse_strategy_a,
    "strategy_b": parse_strategy_b,
    "strategy_c": parse_strategy_c,
}


def _write_partition(df: pd.DataFrame, table: str, strategy_id: str, run_date: date) -> Path:
    out_dir = LAKE_DIR / table / f"strategy_id={strategy_id}" / f"date={run_date.isoformat()}"
    if out_dir.exists():
        shutil.rmtree(out_dir)  # idempotent overwrite of this partition only
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part-0.parquet"
    df.to_parquet(out_path, index=False)
    return out_path


def _quarantine(source: str, run_date: date, error: Exception) -> Path:
    out_dir = QUARANTINE_DIR / source / run_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "source": source,
        "date": run_date.isoformat(),
        "error_type": type(error).__name__,
        "error": str(error),
        "detected_at": datetime.utcnow().isoformat(),
        "traceback": traceback.format_exc(),
    }
    out_path = out_dir / "error.json"
    out_path.write_text(json.dumps(report, indent=2))
    return out_path


def run(run_date: date) -> dict:
    """Standardizes every available source for run_date. Returns a summary
    dict used both for logging and for the quality-check step downstream."""
    summary = {"date": run_date.isoformat(), "succeeded": [], "quarantined": []}

    for strategy_id, parser in PARSERS.items():
        try:
            signals, pnl = parser(run_date)

            signals = SCHEMAS["strategy_signals"].validate(signals)
            pnl = SCHEMAS["strategy_pnl"].validate(pnl)

            _write_partition(signals, "strategy_signals", strategy_id, run_date)
            _write_partition(pnl, "strategy_pnl", strategy_id, run_date)

            summary["succeeded"].append(strategy_id)
            print(f"[OK]   {strategy_id} {run_date} -> standardized ({len(signals)} signal rows)")

        except (SchemaDriftError, pa.errors.SchemaError, FileNotFoundError) as e:
            _quarantine(strategy_id, run_date, e)
            summary["quarantined"].append({"strategy_id": strategy_id, "error": str(e)})
            print(f"[FAIL] {strategy_id} {run_date} -> quarantined: {e}")

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=date.today().isoformat())
    args = parser.parse_args()
    run_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    result = run(run_date)
    print(json.dumps(result, indent=2))
