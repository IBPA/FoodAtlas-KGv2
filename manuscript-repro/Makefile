.PHONY: help setup run collect check audit all

help:
	@echo "Targets:"
	@echo "  make setup    - install python dependencies"
	@echo "  make run      - run preprocessing, clustering, substitution, viz bundle"
	@echo "  make collect  - collect manuscript artifacts into artifacts/"
	@echo "  make check    - compare outputs against manuscript claims"
	@echo "  make audit    - collect + manuscript check + paper support audit"
	@echo "  make all      - run + collect"

setup:
	bash scripts/setup_env.sh

run:
	bash scripts/run_pipeline.sh

collect:
	python3 scripts/collect_artifacts.py

check:
	python3 scripts/manuscript_check.py

audit:
	python3 scripts/collect_artifacts.py
	python3 scripts/manuscript_check.py
	python3 scripts/paper_support_audit.py

all: run collect
