#!/bin/bash

set -e

# Create empty files.
mkdir -p outputs/data_processing

python -m food_atlas.data_processing.run_processing_cdno
python -m food_atlas.data_processing.run_processing_chebi
python -m food_atlas.data_processing.run_processing_foodon
python -m food_atlas.data_processing.run_processing_mesh
python -m food_atlas.data_processing.run_processing_pubchem
