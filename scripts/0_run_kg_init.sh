#!/bin/bash

set -e

python -m food_atlas.kg.initialization.init_kg.create_empty_files
python -m food_atlas.kg.initialization.init_kg.init_food_entities
python -m food_atlas.kg.initialization.init_kg.init_chemical_entities

python -m food_atlas.kg.merge_dbs.merge_fdc
