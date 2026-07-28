# Quant Strategy Data Pipeline

A small, runnable model of the problem a research-analytics platform team
solves at a multi-strategy hedge fund: several strategy teams each publish
their own bespoke, inconsistently-shaped output; a central pipeline has to
standardize it into one schema-contracted, partitioned, queryable dataset;
and the whole thing has to be observable enough that a broken upstream
source gets caught and paged the same day, not discovered a week later by
someone querying an empty table.

Everything here is intentionally small enough to read end to end in fifteen
minutes, but the shape of the problem — fragmented sources, schema
contracts, partitioned columnar storage, SLO-style quality checks, an
incident runbook, a metadata catalog — maps directly onto recurring,
mission-critical analytics infrastructure.

## The problem this models

Three strategies (`strategy_a`, `strategy_b`, `strategy_c`) each export daily
signals and PnL in their own format:

| Source | Shape |
|---|---|
| `strategy_a` | CSV, short/cryptic column names (`strat`, `dt`, `score`) |
| `strategy_b` | CSV, verbose PascalCase columns, US-format dates |
| `strategy_c` | Nested JSON (`meta` / `positions` / `pnl`) |

That's the "fragmented, bespoke workflows" starting point. The pipeline's job
is to turn that into one **unified analytics schema** — two canonical
tables, `strategy_signals` and `strategy_pnl` — with an enforced schema
contract, partitioned for efficient querying, and monitored well enough that
failures are caught, not silently absorbed.

## Architecture

```
strategy_a (csv)  ─┐
strategy_b (csv)  ─┼─► standardize.py ─► schema_contracts.py (pandera) ─► data/lake/*.parquet
strategy_c (json) ─┘        │                (strategy_id=/date= partitions)
                             │
                     data/quarantine/     (source-isolated failures land here,
                     <source>/<date>/       not corrupted into the lake)
                     error.json
                             │
                     quality_checks.py ─► monitoring/reports/quality_<date>.json
                             │
                     health_check.py  ─► pass, or ALERT (stand-in for PagerDuty/Slack)
                             │
                     query_examples.py (DuckDB, partition-pruned SQL)
```

`dags/strategy_analytics_dag.py` shows how the same four steps
(generate → standardize → quality_checks → health_check) would actually be
scheduled in Airflow — daily after market close, with retries, per-task
SLAs, and a failure callback wired to alerting. It's not wired into the
runnable demo (Airflow is a heavy dependency for a portfolio repo), but the
task boundaries and retry/SLA policy are the real design.

## Design choices worth calling out

- **Per-source parsers, isolated blast radius.** `standardize.py` has one
  parser per strategy. A format change in `strategy_b` can only break
  `strategy_b`'s parsing — it can't silently corrupt `strategy_a` or
  `strategy_c`. The per-source `try`/`except` deliberately catches *any*
  exception, not just the ones we anticipated (`SchemaDriftError`, pandera's
  `SchemaError`, `FileNotFoundError`) — an earlier version only caught those
  three, which meant a malformed CSV row (`pandas.errors.ParserError`, or a
  confusing downstream error if pandas silently misaligned columns instead)
  or a pandera strict-mode violation (the *plural* `SchemaErrors`, not a
  subclass of `SchemaError`) would crash the entire run instead of
  quarantining just the broken source. See
  `tests/test_pipeline_integration.py::test_malformed_csv_row_quarantines_source_instead_of_crashing_run`.
- **Fail loud into quarantine, not silent-wrong into the lake.** A parser
  failure is caught, written to `data/quarantine/<source>/<date>/error.json`
  with the exact cause, and the run continues for the other sources. Nothing
  gets written to the lake with a guessed or defaulted value.
- **Idempotent, partition-scoped writes.** Re-running a date overwrites only
  that partition. Backfills and re-runs after a fix are safe.
- **Schema contract as code, not a wiki page.** `schema_contracts.py` uses
  `pandera` to enforce types, ranges, and allowed values on the canonical
  tables — this is the machine-checked version of "our schema doc."
  Duplicated definitions across teams turn into: `must pass this contract`.
- **Quality checks are separate from schema validation.** Schema validation
  answers "does this parse and match the contract?" `quality_checks.py`
  answers "does this look right *vs. what we've historically seen*?" —
  freshness, row counts, null rates, and a PnL sanity invariant
  (`gross_exposure >= |net_exposure|`) that a type-correct-but-wrong row
  would otherwise sail through.
- **DuckDB stands in for Presto/Trino/Spark.** `query_examples.py` reads
  Hive-partitioned Parquet (`strategy_id=.../date=...`) with predicate
  pushdown, the same partition-pruning principle a distributed engine uses
  over S3 at scale. The SQL in this repo would run close to unchanged
  against Athena/Presto in production.

## The incident

`runbooks/schema_drift_incident.md` documents a simulated but fully
reproducible incident: `strategy_b` renamed its PnL column
(`ProfitLossUSD` → `PnLUSD`) with no notice on 2026-07-24. The fix has since
shipped, so this is now a two-part story: what broke, and the regression
test proving it stays fixed.

```bash
# original incident conditions -- kept as historical record, no longer fails on main
make incident DATE=2026-07-24

# same renamed column, but on current main: all 3 strategies land clean
make incident DATE=2026-07-25
```

The runbook walks through detection, triage, root cause, the fix (with its
regression test), and postmortem follow-ups — the artifact this repo treats
as a first-class deliverable, not an afterthought.

## Repo layout

```
src/
  generate_raw_data.py   synthetic fragmented sources (the "before" state)
  schema_contracts.py    pandera contracts for the canonical tables
  standardize.py         per-source parsers + validation + partitioned writes
  quality_checks.py      freshness / row-count / null-rate / sanity checks
  query_examples.py      DuckDB queries over the partitioned lake
dags/
  strategy_analytics_dag.py   Airflow DAG (illustrative orchestration)
monitoring/
  health_check.py        SLO evaluation + alert stub
  reports/                quality_<date>.json (sample reports checked in)
metadata/
  catalog.yaml            table/schema/partition/lineage/ownership catalog
runbooks/
  schema_drift_incident.md   incident + postmortem
tests/
  test_schema_contracts.py       unit tests on the pandera contracts
  test_pipeline_integration.py   end-to-end: happy path, the fixed drift
                                  incident (regression test), and a generic
                                  unhandled-break isolation check
data/
  raw/, lake/, quarantine/   sample output for 2026-07-23 (happy path),
                              2026-07-24 (drift incident, pre-fix), and
                              2026-07-25 (same drift, post-fix) -- checked in
                              so the repo is browsable without running anything
```

## Running it

```bash
pip install -r requirements.txt

# happy path for a given date
make run DATE=2026-07-25

# the schema-drift incident, end to end
make incident DATE=2026-07-26

# tests
make test
```

## If this were going to production

- Swap DuckDB for Athena/Presto against an S3-backed lake; keep the same
  Hive partitioning scheme and SQL.
- Replace the `print`-based alert in `monitoring/health_check.py` with a
  real PagerDuty/Opsgenie/Slack integration; the DAG's
  `on_failure_callback` / `sla_miss_callback` are already the right hook
  points.
- Add a raw-side schema contract per source (today only the canonical
  output is contracted), so drift is caught with a clearer message and can
  optionally warn instead of hard-fail on non-critical fields — one of the
  follow-ups called out in the runbook.
- Replace `metadata/catalog.yaml` with a real catalog (Glue Data Catalog,
  DataHub, Amundsen) that derives lineage and freshness automatically
  instead of being hand-maintained.
- Track quarantine rate per source over time as a reliability metric, not
  just a per-run pass/fail.
- Turn the documented freshness target (`freshness_sla_target` in
  `metadata/catalog.yaml`) into something actually enforced. Today
  `check_freshness` only checks whether a partition exists, not whether it
  landed by a deadline (`freshness_sla_enforced: false` in the catalog is
  honest about this). Closing the gap means recording a `landed_at`
  timestamp per partition and comparing it against the target.
