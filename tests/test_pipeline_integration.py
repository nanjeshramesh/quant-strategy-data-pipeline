"""
End-to-end tests over a few dedicated test dates (far in the future so they
never collide with demo dates used in the README). Exercises the full
generate -> standardize -> quality_checks path for the happy path, the
now-fixed strategy_b column rename, a generic still-unhandled break, and a
malformed raw CSV row, then cleans up everything it wrote.
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
MALFORMED_CSV_DATE = date(2099, 1, 4)


@pytest.fixture(autouse=True)
def cleanup():
    yield
    for d in (HAPPY_DATE, FIXED_DRIFT_DATE, UNHANDLED_BREAK_DATE, MALFORMED_CSV_DATE):
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


def test_malformed_csv_row_quarantines_source_instead_of_crashing_run():
    """Regression test for a real gap found during interview prep: a raw CSV
    row with an extra field doesn't always fail cleanly. Depending on which
    row it's in, pandas either raises ParserError outright, or silently
    misaligns columns so the failure surfaces later as something confusing
    and unrelated-looking (in practice: a DateParseError complaining it
    can't parse a ticker symbol as a date, because the date column had
    silently absorbed a shifted value). A pandera strict-mode column
    violation raises the *plural* SchemaErrors, a different, non-subclass
    exception from SchemaError. None of these are subclasses of the
    exceptions standardize.py used to catch (SchemaDriftError, the
    *singular* SchemaError, FileNotFoundError), so they used to propagate
    uncaught and crash standardize.run() for every strategy, not just the
    broken one -- contradicting the isolation guarantee documented in
    standardize.py's module docstring and in
    runbooks/schema_drift_incident.md. The except clause is now broad
    (catches any Exception per source), so this must quarantine cleanly
    regardless of which specific error pandas ends up raising."""
    generate_raw_data.generate(MALFORMED_CSV_DATE)

    raw_path = BASE / "data" / "raw" / "strategy_a" / f"{MALFORMED_CSV_DATE.isoformat()}.csv"
    lines = raw_path.read_text().splitlines()
    # Append a stray extra field to one data row -- still valid enough for a
    # human to have plausibly written it, but it desyncs pandas.read_csv's
    # column alignment from that row onward.
    lines[1] = lines[1] + ",unexpected_extra_field"
    raw_path.write_text("\n".join(lines) + "\n")

    summary = standardize.run(MALFORMED_CSV_DATE)

    assert set(summary["succeeded"]) == {"strategy_b", "strategy_c"}
    assert len(summary["quarantined"]) == 1
    assert summary["quarantined"][0]["strategy_id"] == "strategy_a"

    report = quality_checks.run(MALFORMED_CSV_DATE)
    assert report["overall_passed"] is False
    freshness = next(c for c in report["checks"] if c["check"] == "freshness")
    assert freshness["missing"] == ["strategy_a"]
