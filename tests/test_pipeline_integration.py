"""
End-to-end tests over a couple of dedicated test dates (far in the future so
they never collide with demo dates used in the README). Exercises the full
generate -> standardize -> quality_checks path for both the happy path and
the injected schema-drift incident, then cleans up everything it wrote.
"""
import shutil
from datetime import date
from pathlib import Path

import pytest

from src import generate_raw_data, quality_checks, standardize

BASE = Path(__file__).resolve().parent.parent
HAPPY_DATE = date(2099, 1, 1)
DRIFT_DATE = date(2099, 1, 2)


@pytest.fixture(autouse=True)
def cleanup():
    yield
    for d in (HAPPY_DATE, DRIFT_DATE):
        for root in ("data/raw", "data/lake", "data/quarantine"):
            for p in (BASE / root).glob(f"**/*{d.isoformat()}*"):
                if p.is_dir():
                    shutil.rmtree(p, ignore_errors=True)
        report = BASE / "monitoring" / "reports" / f"quality_{d.isoformat()}.json"
        report.unlink(missing_ok=True)


def test_happy_path_all_strategies_land_and_pass_quality():
    generate_raw_data.generate(HAPPY_DATE)
    summary = standardize.run(HAPPY_DATE)

    assert set(summary["succeeded"]) == {"strategy_a", "strategy_b", "strategy_c"}
    assert summary["quarantined"] == []

    report = quality_checks.run(HAPPY_DATE)
    assert report["overall_passed"] is True


def test_schema_drift_quarantines_strategy_b_but_others_still_land():
    generate_raw_data.generate(DRIFT_DATE, inject_drift=True)
    summary = standardize.run(DRIFT_DATE)

    assert set(summary["succeeded"]) == {"strategy_a", "strategy_c"}
    assert len(summary["quarantined"]) == 1
    assert summary["quarantined"][0]["strategy_id"] == "strategy_b"
    assert "ProfitLossUSD" in summary["quarantined"][0]["error"]

    report = quality_checks.run(DRIFT_DATE)
    assert report["overall_passed"] is False
    freshness = next(c for c in report["checks"] if c["check"] == "freshness")
    assert freshness["missing"] == ["strategy_b"]
