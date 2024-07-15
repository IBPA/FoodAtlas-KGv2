#!/bin/bash

# python -m food_atlas.kg.postprocessing.map_chebi_ids  # Deprecated
# python -m food_atlas.kg.postprocessing.update_entity_common_name  # Deprecated

python -m food_atlas.kg.postprocessing.group_entities.group_chemicals
python -m food_atlas.kg.postprocessing.group_entities.group_foods

python -m food_atlas.kg.postprocessing.convert_unit
