#!/bin/bash

PATH_NAMES_NOT_IN_LUT_NAME="outputs/kg/20240212162043/_names_not_in_lut_food.txt"
PATH_OUTPUT="outputs/kg/20240212162043/ncbi_taxonomy_ids.txt"
PATH_INPUT_DIR="outputs/kg/20240212162043"

python -m food_atlas.kg.retrieve_ncbi_taxonomy \
    ${PATH_NAMES_NOT_IN_LUT_NAME} \
    $PATH_OUTPUT
python -m foodatlas.kg.run_entity_creation \
    $PATH_INPUT_DIR
