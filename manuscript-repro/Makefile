.PHONY: help setup run fig2 collect test revalidate clean all

help:
	@echo "Targets:"
	@echo "  make setup    - pip install -r ../requirements.txt"
	@echo "  make run      - KG sanity check + instructions (see scripts/run_pipeline.sh)"
	@echo "  make fig2     - generate Fig.2 circos/sunburst assets into analysis_outputs/visualization"
	@echo "  make collect  - collect manuscript artifacts into artifacts/"
	@echo "  make test     - unittest: fixture reproducibility + optional paper scale test"
	@echo "  make revalidate - make test + scripts/revalidate_paper.py (full KG vs paper)"
	@echo "  make clean    - remove generated logs and copied artifacts"
	@echo "  make all      - run + collect"

setup:
	bash scripts/setup_env.sh

run:
	bash scripts/run_pipeline.sh

fig2:
	PYTHONPATH=scripts python3 scripts/generate_fig2_assets.py

collect:
	python3 scripts/collect_artifacts.py

test:
	PYTHONPATH=scripts python3 -m unittest discover -s tests -v

revalidate: test
	PYTHONPATH=scripts python3 scripts/revalidate_paper.py

clean:
	mkdir -p artifacts logs
	find artifacts -mindepth 1 ! -name README.md -exec rm -rf {} +
	find logs -mindepth 1 ! -name README.md -exec rm -rf {} +

all: run collect
