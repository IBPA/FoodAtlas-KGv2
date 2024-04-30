#!/bin/bash

python -m food_atlas.kg.postprocessing.map_chebi_ids

python -m food_atlas.kg.postprocessing.generate_entity_group

python -m food_atlas.kg.postprocessing.update_entity_common_name
