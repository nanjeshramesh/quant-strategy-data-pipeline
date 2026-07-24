.PHONY: install run incident test clean

install:
	pip install -r requirements.txt

# Full happy-path demo for a given date (defaults to today).
run:
	python -m src.generate_raw_data --date $(DATE)
	python -m src.standardize --date $(DATE)
	python -m src.quality_checks --date $(DATE)
	python -m monitoring.health_check --date $(DATE) || true
	python -m src.query_examples --date $(DATE)

# Reproduces the schema-drift incident documented in
# runbooks/schema_drift_incident.md end to end.
incident:
	python -m src.generate_raw_data --date $(DATE) --inject-drift
	python -m src.standardize --date $(DATE)
	python -m src.quality_checks --date $(DATE)
	python -m monitoring.health_check --date $(DATE) || true

test:
	pytest tests/ -v

DATE ?= $(shell date +%Y-%m-%d)
