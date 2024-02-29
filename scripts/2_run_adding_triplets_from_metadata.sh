#!/bin/bash

# CURR_TIME=$(date "+%Y%m%d%H%M%S")
# mkdir -p outputs/kg/$CURR_TIME
# echo "Created directory: outputs/kg/$CURR_TIME"

# PATH_WORKSPACE=outputs/kg/$CURR_TIME
# PATH_WORKSPACE=outputs/kg/20240222190741
PATH_WORKSPACE=outputs/kg/merge_dbs/fa_v1

# PATH_DATA_LIT2KG=data/Lit2KG/tests/text_parser_predicted.pkl
# python -m food_atlas.kg.run_data_preparation \
#     $PATH_DATA_LIT2KG \
#     $PATH_WORKSPACE

python -m food_atlas.kg.run_entity_linkage \
    $PATH_WORKSPACE

# python -m food_atlas.kg.retrieve_ncbi_taxonomy \
#     $PATH_WORKSPACE

# python -m food_atlas.kg.retrieve_pubchem_compound \
#     $PATH_WORKSPACE
