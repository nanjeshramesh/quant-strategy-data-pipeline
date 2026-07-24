"""
Post-standardization data quality checks. This is deliberately separate from
standardize.py: standardization answers "does this parse and match the
schema contract?" while this answers "does this data look *right* compared
to what we've historically seen?" -- freshness, completeness, and
distributional sanity checks that catch problems a schema contract alone
can't (e.g. a source going silent, or reporting differently-shaped numbers).

Writes monitoring/reports/quality_<date>.json, which health_check.py reads
to decide whether to page.

Run:
    python -m src.quality_checks --date 2026-07-23
"""
import argparse
import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parent.parent
LAKE_DIR = BASE / "data" / "lake"
REPORTS_DIR = BASE / "monitoring" / "reports"

EXPECTED_STRATEGIES = ["strategy_a", "strategy_b", "strategy_c"]
FRESHNESS_SLA_DAYS = 0  # data for date D must exist by the time we check on D
MIN_SIGNAL_ROWS_PER_STRATEGY = 10


def _load_partition(table: str, strategy_id: str, run_date: date) -> pd.DataFrame | None:
    path = LAKE_DIR / table / f"strategy_id={strategy_id}" / f"date={run_date.isoformat()}" / "part-0.parquet"
    if not path.exists():
        return None
    return pd.read_parquet(path)


def check_freshness(run_date: date) -> dict:
    present, missing = [], []
    for s in EXPECTED_STRATEGIES:
        df = _load_partition("strategy_pnl", s, run_date)
        (present if df is not None else missing).append(s)
    return {
        "check": "freshness",
        "passed": len(missing) == 0,
        "present": present,
        "missing": missing,
        "detail": f"{len(present)}/{len(EXPECTED_STRATEGIES)} strategies landed for {run_date.isoformat()}",
    }


def check_row_counts(run_date: date) -> dict:
    results = {}
    all_passed = True
    for s in EXPECTED_STRATEGIES:
        df = _load_partition("strategy_signals", s, run_date)
        n = 0 if df is None else len(df)
        ok = n >= MIN_SIGNAL_ROWS_PER_STRATEGY
        all_passed &= ok
        results[s] = {"row_count": n, "passed": ok}
    return {
        "check": "row_counts",
        "passed": all_passed,
        "min_expected": MIN_SIGNAL_ROWS_PER_STRATEGY,
        "by_strategy": results,
    }


def check_null_rates(run_date: date) -> dict:
    results = {}
    all_passed = True
    for s in EXPECTED_STRATEGIES:
        df = _load_partition("strategy_signals", s, run_date)
        if df is None:
            results[s] = {"passed": False, "reason": "no data"}
            all_passed = False
            continue
        null_rate = float(df[["signal_score", "position_target_usd"]].isna().mean().max())
        ok = null_rate == 0.0
        all_passed &= ok
        results[s] = {"null_rate": null_rate, "passed": ok}
    return {"check": "null_rates", "passed": all_passed, "by_strategy": results}


def check_pnl_sanity(run_date: date) -> dict:
    """Gross exposure should always be >= |net exposure| -- a basic invariant
    that catches unit errors or swapped columns upstream."""
    results = {}
    all_passed = True
    for s in EXPECTED_STRATEGIES:
        df = _load_partition("strategy_pnl", s, run_date)
        if df is None:
            results[s] = {"passed": False, "reason": "no data"}
            all_passed = False
            continue
        row = df.iloc[0]
        ok = bool(row["gross_exposure_usd"] >= abs(row["net_exposure_usd"]))
        all_passed &= ok
        results[s] = {"gross": float(row["gross_exposure_usd"]), "net": float(row["net_exposure_usd"]), "passed": ok}
    return {"check": "pnl_sanity", "passed": all_passed, "by_strategy": results}


def run(run_date: date) -> dict:
    checks = [
        check_freshness(run_date),
        check_row_counts(run_date),
        check_null_rates(run_date),
        check_pnl_sanity(run_date),
    ]
    report = {
        "date": run_date.isoformat(),
        "generated_at": datetime.utcnow().isoformat(),
        "checks": checks,
        "overall_passed": all(c["passed"] for c in checks),
    }
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORTS_DIR / f"quality_{run_date.isoformat()}.json"
    out_path.write_text(json.dumps(report, indent=2))
    print(f"wrote {out_path}")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=date.today().isoformat())
    args = parser.parse_args()
    run_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    report = run(run_date)
    print(json.dumps(report, indent=2))
