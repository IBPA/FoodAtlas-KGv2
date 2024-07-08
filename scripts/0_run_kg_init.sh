#!/bin/bash

set -e

# Create empty files.
python -m food_atlas.kg.initialization.create_empty_files

# Initialize food and chemical entities.
python -m food_atlas.kg.initialization.food.init_entities
python -m food_atlas.kg.initialization.chemical.init_entities

python -m food_atlas.kg.initialization.food.init_onto
python -m food_atlas.kg.initialization.chemical.init_onto

python -m food_atlas.kg.merge_dbs.merge_fdc
