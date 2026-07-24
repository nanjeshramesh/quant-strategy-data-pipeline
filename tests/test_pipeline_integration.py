"""
End-to-end tests over a few dedicated test dates (far in the future so they
never collide with demo dates used in the README). Exercises the full
generate -> standardize -> quality_checks path for the happy path, the
now-fixed strategy_b column rename, and a generic still-unhandled break, then
cleans up everything it wrote.
"""
import json
import shutil
from datetime import date
from pathlib import Path

import pytest

from src import generate_raw_data, quality_checks, standardize

BASE = Path(__file__).resolve().parent.parent
HAPPY_DATE = date(2099, 1, 1)
FIXED_DRIFT_DATE = date(2099, 1, 2)
UNHANDLED_BREAK_DATE = date(2099, 1, 3)


@pytest.fixture(autouse=True)
def cleanup():
    yield
    for d in (HAPPY_DATE, FIXED_DRIFT_DATE, UNHANDLED_BREAK_DATE):
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


def test_previously_incident_causing_column_rename_now_handled():
    """Regression test for the 2026-07-24 incident (runbooks/schema_drift_incident.md):
    strategy_b renamed ProfitLossUSD -> PnLUSD with no notice, which used to
    quarantine the whole source. The parser now accepts either column name,
    so the exact conditions that caused that incident must no longer break
    the pipeline."""
    generate_raw_data.generate(FIXED_DRIFT_DATE, inject_drift=True)
    summary = standardize.run(FIXED_DRIFT_DATE)

    assert set(summary["succeeded"]) == {"strategy_a", "strategy_b", "strategy_c"}
    assert summary["quarantined"] == []

    report = quality_checks.run(FIXED_DRIFT_DATE)
    assert report["overall_passed"] is True


def test_generic_break_still_quarantines_source_without_taking_down_others():
    """The fix above is specific to the PnL column rename. This test makes
    sure the underlying isolation/quarantine mechanism itself still works
    for a break the parser genuinely can't recover from -- e.g. strategy_c
    dropping a required top-level key entirely."""
    generate_raw_data.generate(UNHANDLED_BREAK_DATE)

    corrupt_path = BASE / "data" / "raw" / "strategy_c" / f"{UNHANDLED_BREAK_DATE.isoformat()}.json"
    payload = json.loads(corrupt_path.read_text())
    del payload["pnl"]
    corrupt_path.write_text(json.dumps(payload))

    summary = standardize.run(UNHANDLED_BREAK_DATE)

    assert set(summary["succeeded"]) == {"strategy_a", "strategy_b"}
    assert len(summary["quarantined"]) == 1
    assert summary["quarantined"][0]["strategy_id"] == "strategy_c"
    assert "pnl" in summary["quarantined"][0]["error"]

    report = quality_checks.run(UNHANDLED_BREAK_DATE)
    assert report["overall_passed"] is False
