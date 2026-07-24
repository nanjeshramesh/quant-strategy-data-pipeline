"""
Simulates the "before" state described in the JD: three strategy teams each
publish their own bespoke, inconsistently-shaped outputs. This is the raw
material the standardization pipeline (standardize.py) consolidates into a
single unified analytics schema.

Run:
    python -m src.generate_raw_data --date 2026-07-23
    python -m src.generate_raw_data --date 2026-07-24 --inject-drift
"""
import argparse
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
TICKERS = ["AAPL", "MSFT", "NVDA", "JPM", "XOM", "TSLA", "GOOGL", "AMZN"]


def _seeded_rng(strategy: str, run_date: date) -> np.random.Generator:
    seed = abs(hash((strategy, run_date.isoformat()))) % (2**32)
    return np.random.default_rng(seed)


def _positions_frame(strategy: str, run_date: date, n: int = 40) -> pd.DataFrame:
    rng = _seeded_rng(strategy, run_date)
    tickers = rng.choice(TICKERS, size=n)
    return pd.DataFrame(
        {
            "ticker": tickers,
            "score": rng.normal(0, 1, size=n).round(4),
            "target": rng.normal(0, 5_000_000, size=n).round(2),
        }
    )


def _pnl(strategy: str, run_date: date) -> dict:
    rng = _seeded_rng(strategy + "_pnl", run_date)
    pnl = float(rng.normal(150_000, 400_000))
    gross = float(abs(rng.normal(20_000_000, 3_000_000)))
    net = float(rng.normal(0, 4_000_000))
    return {"pnl": round(pnl, 2), "gross": round(gross, 2), "net": round(net, 2)}


def write_strategy_a(run_date: date) -> Path:
    """Clean-ish CSV, but uses its own short column names."""
    pos = _positions_frame("strategy_a", run_date)
    pnl = _pnl("strategy_a", run_date)
    df = pos.copy()
    df.insert(0, "dt", run_date.isoformat())
    df.insert(0, "strat", "strategy_a")
    df["pnl"] = pnl["pnl"]
    df["gross"] = pnl["gross"]
    df["net"] = pnl["net"]
    df = df.rename(columns={"score": "score", "target": "target_pos"})
    out = RAW_DIR / "strategy_a" / f"{run_date.isoformat()}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    return out


def write_strategy_b(run_date: date, inject_drift: bool = False) -> Path:
    """Same underlying data, different (capitalized, verbose) column names and
    a different date format -- a very common real-world source of pipeline
    breakage. When inject_drift=True, simulates a strategy team silently
    renaming the PnL column, which is the incident dramatized in
    runbooks/schema_drift_incident.md.
    """
    pos = _positions_frame("strategy_b", run_date)
    pnl = _pnl("strategy_b", run_date)
    df = pos.copy()
    df.insert(0, "Date", run_date.strftime("%m/%d/%Y"))
    df.insert(0, "StrategyID", "strategy_b")
    df = df.rename(columns={"ticker": "Symbol", "score": "SignalScore", "target": "PositionTarget"})

    pnl_col = "PnLUSD" if inject_drift else "ProfitLossUSD"
    df[pnl_col] = pnl["pnl"]
    df["GrossExposure"] = pnl["gross"]
    df["NetExposure"] = pnl["net"]

    out = RAW_DIR / "strategy_b" / f"{run_date.isoformat()}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    return out


def write_strategy_c(run_date: date) -> Path:
    """Nested JSON export -- the third common shape analytics teams inherit."""
    pos = _positions_frame("strategy_c", run_date)
    pnl = _pnl("strategy_c", run_date)
    payload = {
        "meta": {"strategy": "strategy_c", "date": run_date.isoformat(), "generated_at": datetime.utcnow().isoformat()},
        "positions": [
            {"ticker": r.ticker, "signal": r.score, "target": r.target} for r in pos.itertuples()
        ],
        "pnl": {"usd": pnl["pnl"], "gross": pnl["gross"], "net": pnl["net"]},
    }
    out = RAW_DIR / "strategy_c" / f"{run_date.isoformat()}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2))
    return out


def generate(run_date: date, inject_drift: bool = False) -> list[Path]:
    paths = [
        write_strategy_a(run_date),
        write_strategy_b(run_date, inject_drift=inject_drift),
        write_strategy_c(run_date),
    ]
    return paths


def generate_range(start: date, end: date, drift_on: date | None = None) -> None:
    d = start
    while d <= end:
        generate(d, inject_drift=(drift_on is not None and d == drift_on))
        d += timedelta(days=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=date.today().isoformat())
    parser.add_argument("--backfill-days", type=int, default=0, help="also generate N prior days for baseline history")
    parser.add_argument("--inject-drift", action="store_true", help="simulate strategy_b renaming its PnL column")
    args = parser.parse_args()

    run_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    if args.backfill_days:
        generate_range(run_date - timedelta(days=args.backfill_days), run_date - timedelta(days=1))

    paths = generate(run_date, inject_drift=args.inject_drift)
    for p in paths:
        print(f"wrote {p}")
