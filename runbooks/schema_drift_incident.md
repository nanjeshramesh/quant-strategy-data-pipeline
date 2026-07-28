# Runbook + Postmortem: strategy_b PnL column rename

**Status:** Resolved, fix shipped and verified (simulated incident,
reproducible in this repo)
**Severity:** Sev-2 (one of three strategies missing from the unified lake; no
data corruption, no impact to the other two strategies)

**Reproduce the original incident (historical):**
```bash
python -m src.generate_raw_data --date 2026-07-24 --inject-drift
python -m src.standardize --date 2026-07-24
python -m src.quality_checks --date 2026-07-24
python -m monitoring.health_check --date 2026-07-24
```
Note: this is the exact command sequence that produced the checked-in
`data/quarantine/strategy_b/2026-07-24/` record. On the current `main`
branch it will **no longer fail** -- the fix below now ships in
`src/standardize.py`, so `parse_strategy_b` accepts the renamed column. The
2026-07-24 artifacts are kept as-is as the historical record of what
actually happened before the fix.

**Verify the fix (strategy_b's source still hasn't reverted the rename):**
```bash
python -m src.generate_raw_data --date 2026-07-25 --inject-drift
python -m src.standardize --date 2026-07-25     # all 3 strategies land
python -m src.quality_checks --date 2026-07-25  # overall_passed: true
python -m monitoring.health_check --date 2026-07-25   # [HEALTHY], exit 0
```

## Summary

`strategy_b`'s raw export renamed its PnL column from `ProfitLossUSD` to
`PnLUSD` with no notice. The standardization job's parser for `strategy_b`
required `ProfitLossUSD` explicitly, so the run failed fast for that source,
quarantined it with a structured error, and continued landing `strategy_a`
and `strategy_c` normally. The freshness check caught the gap the same day
and `health_check` paged before any downstream consumer queried stale/missing
data for `strategy_b`.

## Detection

- `quality_checks.py`'s `freshness` check found `strategy_b` missing from
  `data/lake/strategy_pnl/` for the run date.
- `health_check.py` read that report, found `overall_passed = false`, and
  raised an alert (stand-in for a PagerDuty page) pointing at this runbook.
- Root cause was already captured for free: `standardize.py` wrote
  `data/quarantine/strategy_b/2026-07-24/error.json` containing the exact
  `SchemaDriftError` message (missing column `ProfitLossUSD`) and a full
  traceback, so on-call didn't need to reproduce the failure to see what
  broke.

## Triage / Timeline (as it would play out on-call)

1. **T+0** — DAG task `run_quality_checks` completes; `evaluate_slo_and_alert`
   fails, page fires.
2. **T+2min** — On-call opens `data/quarantine/strategy_b/<date>/error.json`,
   sees `missing expected column(s) ['ProfitLossUSD']; got columns=[...,
   'PnLUSD', ...]`. Root cause identified without reading pipeline code.
3. **T+10min** — On-call confirms `strategy_a` and `strategy_c` are healthy
   (blast radius contained to one source, per the per-source parser design)
   and that no partition was overwritten with bad data (the failure occurred
   before any write — quarantine, not corruption).
4. **T+20min** — Fix shipped: `parse_strategy_b` updated to accept either
   `ProfitLossUSD` or `PnLUSD` (backward/forward compatible column mapping),
   guarded by `tests/test_pipeline_integration.py::
   test_previously_incident_causing_column_rename_now_handled`, which
   regenerates the exact renamed-column conditions and asserts the pipeline
   no longer quarantines strategy_b.
5. **T+25min** — Backfill: `python -m src.standardize --date 2026-07-24`
   re-run; partition write is idempotent so this is safe to re-run without
   creating duplicates.
6. **T+27min** — `quality_checks` + `health_check` re-run clean.
7. **Ongoing** — strategy_b's team never reverted the rename (they consider
   `PnLUSD` the new normal). 2026-07-25 onward uses that column name and the
   pipeline handles it without incident -- see "Verify the fix" above.

## Root cause

No schema contract existed on the *raw* side — only on the canonical output.
The parser assumed a fixed raw column name with no compatibility window, so
any upstream rename is a hard failure by design (intentional: fail loud, not
silent-wrong). That's the correct tradeoff for a PnL field, but it means
upstream changes need a heads-up channel.

## What worked

- Per-source parser isolation meant one source's break didn't take down the
  other two, or corrupt any existing partition (idempotent writes + fail
  loudly, don't fail silently at the last valid partition).
- The error was actionable without reading code: the quarantine record had
  the exact missing column and the diff against what was present.
- Detection was same-day via the freshness check, not "someone querying
  the table three days later and asking why strategy_b is empty."

## Related follow-up: isolation held for this incident, but not for all

This incident's specific failure (missing column) was already one of the
three exception types `standardize.py` caught, so it quarantined correctly
the first time. Stress-testing other failure shapes afterward turned up two
that were not caught: a malformed CSV row (pandas either raises `ParserError`
or silently misaligns columns and fails downstream with a confusing,
unrelated-looking error) and a pandera strict-mode column violation (raises
the *plural* `SchemaErrors`, a different, non-subclass exception from the
singular `SchemaError` that was being caught). Both used to crash the entire
run instead of quarantining just the broken source, contradicting the
isolation guarantee this runbook and the README both claim. The except
clause is now broad (catches any exception per source), with a regression
test: `tests/test_pipeline_integration.py::
test_malformed_csv_row_quarantines_source_instead_of_crashing_run`.

## Follow-ups / prevention (postmortem action items)

- [ ] Add a lightweight raw-schema contract per source (not just canonical)
      so drift is caught with a clearer message and can optionally *warn*
      instead of hard-fail for non-critical columns.
- [ ] Add a Slack/email notification channel with the upstream strategy_b
      team for planned schema changes (process fix, not just code).
- [ ] Extend `check_row_counts` with a week-over-week trend check, not just
      an absolute floor, to catch quieter forms of drift (e.g. half the
      expected tickers silently missing rather than the whole source).
- [ ] Track quarantine rate per source over time as a reliability metric —
      a source that drifts monthly needs a different integration contract
      than one that's been stable for a year.
