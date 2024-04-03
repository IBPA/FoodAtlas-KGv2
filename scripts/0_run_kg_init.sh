#!/bin/bash

PATH_WORKSPACE=outputs/kg/initialization

python -m food_atlas.kg.initialization.init_kg.create_empty_files

# python -m food_atlas.kg.initialization.get_primary_ids.get_ncbi_taxon_ids
# python -m food_atlas.kg.initialization.get_primary_ids.get_pubchem_cids

# python -m food_atlas.kg.initialization.retrieve_dbs.retrieve_ncbi_taxonomy
# python -m food_atlas.kg.initialization.retrieve_dbs.retrieve_pubchem_compound

python -m food_atlas.kg.initialization.init_kg.init_food_entities
python -m food_atlas.kg.initialization.init_kg.init_chemical_entities
# python -m food_atlas.kg.initialization.init_kg.append_lut_with_ids

# python -m food_atlas.kg.run_kg_expansion \
#     outputs/kg/merge_dbs/fdc
