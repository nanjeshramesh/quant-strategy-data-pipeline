"""
SLO-style health check. Reads the quality report for a given date and
decides pass/fail the way an on-call rotation would: this is the piece that,
in production, would be wired to PagerDuty/Slack rather than to stdout.

Exit code 0 = healthy, 1 = SLO breach (would page on-call).

Run:
    python -m monitoring.health_check --date 2026-07-23
"""
import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
REPORTS_DIR = BASE / "monitoring" / "reports"


def load_report(run_date: date) -> dict | None:
    path = REPORTS_DIR / f"quality_{run_date.isoformat()}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def evaluate(report: dict) -> tuple[bool, list[str]]:
    breaches = []
    for check in report["checks"]:
        if not check["passed"]:
            breaches.append(f"{check['check']} FAILED: {json.dumps(check, default=str)}")
    return len(breaches) == 0, breaches


def alert(run_date: date, breaches: list[str]) -> None:
    """Stand-in for a real paging integration (PagerDuty/Opsgenie/Slack
    webhook). Kept as a clearly-labeled stub so it's obvious where that
    integration would plug in."""
    print("=" * 70)
    print(f"ALERT: analytics pipeline SLO breach for {run_date.isoformat()}")
    for b in breaches:
        print(f"  - {b}")
    print("See runbooks/schema_drift_incident.md for the triage playbook.")
    print("=" * 70)


def main(run_date: date) -> int:
    report = load_report(run_date)
    if report is None:
        print(f"No quality report found for {run_date.isoformat()} -- treating as freshness breach.")
        alert(run_date, ["no quality report produced (upstream pipeline likely did not run)"])
        return 1

    healthy, breaches = evaluate(report)
    if healthy:
        print(f"[HEALTHY] {run_date.isoformat()}: all {len(report['checks'])} checks passed.")
        return 0

    alert(run_date, breaches)
    return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=date.today().isoformat())
    args = parser.parse_args()
    run_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    sys.exit(main(run_date))
