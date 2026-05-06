.PHONY: help setup run fig2 collect revalidate clean all

help:
	@echo "Targets:"
	@echo "  make setup    - pip install -r ../requirements.txt"
	@echo "  make run      - KG sanity check + instructions (see scripts/run_pipeline.sh)"
	@echo "  make fig2     - generate Fig.2 circos/sunburst assets into analysis_outputs/visualization"
	@echo "  make collect  - collect manuscript artifacts into artifacts/"
	@echo "  make revalidate - scripts/revalidate_paper.py (Fig. 2 scale metrics vs manuscript)"
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

revalidate:
	PYTHONPATH=scripts python3 scripts/revalidate_paper.py

clean:
	mkdir -p artifacts logs
	find artifacts -mindepth 1 ! -name README.md -exec rm -rf {} +
	find logs -mindepth 1 ! -name README.md -exec rm -rf {} +

all: run collect
