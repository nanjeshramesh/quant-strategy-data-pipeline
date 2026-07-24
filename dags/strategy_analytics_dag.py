"""
Illustrative Airflow DAG for the strategy analytics pipeline.

This defines the same pipeline exercised locally by run_pipeline.sh
(generate -> standardize -> quality_checks -> health_check), but wired up
the way it would actually be scheduled in production: daily after market
close, with retries on the flaky/IO-bound steps, an SLA so a late run pages
before downstream consumers notice stale data, and a failure callback hook
for alerting.

This file intentionally is NOT wired into requirements.txt / run_pipeline.sh
-- Airflow is a heavy dependency for a portfolio repo to vendor. It's here to
show how the four pipeline steps map onto DAG task boundaries and retry/SLA
policy, which is the thing worth discussing in an interview, independent of
whether Airflow itself is installed.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from monitoring.health_check import main as health_check_main
from src.generate_raw_data import generate as generate_raw
from src.quality_checks import run as run_quality_checks
from src.standardize import run as run_standardize


def alert_on_failure(context) -> None:
    """Hook point for a real paging integration (PagerDuty/Slack/Opsgenie).
    Kept minimal + explicit rather than importing a vendor SDK the repo
    doesn't otherwise need."""
    ti = context["task_instance"]
    print(f"[PAGE] task {ti.task_id} failed on {context['ds']} -- see runbooks/schema_drift_incident.md")


default_args = {
    "owner": "research-analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="strategy_analytics_pipeline",
    description="Standardize per-strategy outputs into the unified analytics lake and run SLO checks",
    default_args=default_args,
    schedule_interval="0 21 * * 1-5",  # 9pm UTC on weekdays, after US market close
    start_date=datetime(2026, 1, 1),
    catchup=False,
    sla_miss_callback=alert_on_failure,
    tags=["research-analytics", "strategy-lake"],
) as dag:

    def _generate(ds: str, **_):
        run_date = datetime.strptime(ds, "%Y-%m-%d").date()
        generate_raw(run_date)

    def _standardize(ds: str, **_):
        run_date = datetime.strptime(ds, "%Y-%m-%d").date()
        summary = run_standardize(run_date)
        if summary["quarantined"]:
            # Don't fail the DAG outright -- partial landings still unblock
            # the strategies that succeeded. quality_checks downstream will
            # surface the gap via the freshness check and page on-call.
            print(f"partial landing, quarantined={summary['quarantined']}")

    def _quality_checks(ds: str, **_):
        run_date = datetime.strptime(ds, "%Y-%m-%d").date()
        run_quality_checks(run_date)

    def _health_check(ds: str, **_):
        run_date = datetime.strptime(ds, "%Y-%m-%d").date()
        exit_code = health_check_main(run_date)
        if exit_code != 0:
            raise RuntimeError(f"SLO breach for {run_date.isoformat()}; see monitoring/reports/")

    generate = PythonOperator(task_id="generate_raw_sources", python_callable=_generate, sla=timedelta(hours=1))
    standardize = PythonOperator(task_id="standardize_to_unified_schema", python_callable=_standardize, sla=timedelta(hours=1))
    quality_checks = PythonOperator(task_id="run_quality_checks", python_callable=_quality_checks, sla=timedelta(minutes=30))
    health_check = PythonOperator(task_id="evaluate_slo_and_alert", python_callable=_health_check, sla=timedelta(minutes=15))

    generate >> standardize >> quality_checks >> health_check
