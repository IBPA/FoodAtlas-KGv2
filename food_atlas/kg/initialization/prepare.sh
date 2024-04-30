#!/bin/bash

set -e

python -m food_atlas.kg.initialization.get_primary_ids.get_ncbi_taxon_ids
python -m food_atlas.kg.initialization.get_primary_ids.get_pubchem_cids

python -m food_atlas.kg.initialization.retrieve_dbs.retrieve_ncbi_taxonomy
python -m food_atlas.kg.initialization.retrieve_dbs.retrieve_pubchem_compound
