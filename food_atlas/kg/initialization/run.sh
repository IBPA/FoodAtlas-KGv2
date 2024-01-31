#!/bin/bash

python -m food_atlas.kg.initialization.extract_ncbi_taxon_ids
python -m food_atlas.kg.initialization.retrieve_ncbi_taxonomy
python -m food_atlas.kg.initialization.initialize_food_entities
python -m food_atlas.kg.initialization.initialize_food_lookup_table
python -m food_atlas.kg.initialization.merge_foodb
python -m food_atlas.kg.initialization.merge_fdc
