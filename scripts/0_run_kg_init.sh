#!/bin/bash

set -e

# Create empty files.
python -m food_atlas.kg.initialization.create_empty_files

# Initialize food entities and taxonomical tripltes.
python -m food_atlas.kg.initialization.food.init_entities
python -m food_atlas.kg.initialization.food.init_triplets

# Initialize chemical entities.
python -m food_atlas.kg.initialization.chemical.init_entities

python -m food_atlas.kg.merge_dbs.merge_fdc
