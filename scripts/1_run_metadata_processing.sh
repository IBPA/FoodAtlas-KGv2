#!/bin/bash

# CURR_TIME=$(date "+%Y%m%d%H%M%S")
# mkdir -p outputs/kg/$CURR_TIME
# echo "Created directory: outputs/kg/$CURR_TIME"

# PATH_WORKSPACE=outputs/kg/$CURR_TIME
# PATH_WORKSPACE=outputs/kg/20240222190741

PATH_INPUT=data/Lit2KG/text_parser_predicted_2024_02_25.pkl
PATH_OUTPUT_DIR=outputs/kg/20240401

python -m food_atlas.kg.run_metadata_processing \
    $PATH_INPUT \
    $PATH_OUTPUT_DIR
