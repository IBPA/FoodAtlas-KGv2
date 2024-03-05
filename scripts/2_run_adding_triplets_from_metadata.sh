#!/bin/bash

# CURR_TIME=$(date "+%Y%m%d%H%M%S")
# mkdir -p outputs/kg/$CURR_TIME
# echo "Created directory: outputs/kg/$CURR_TIME"

# PATH_WORKSPACE=outputs/kg/$CURR_TIME

PATH_OUTPUT_DIR=outputs/kg/20240315

python -m food_atlas.kg.run_kg_expansion \
    $PATH_OUTPUT_DIR
