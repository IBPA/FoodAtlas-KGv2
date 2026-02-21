#!/bin/bash

PATH_INPUT=outputs/kg/_metadata_new.tsv
PATH_INPUT_KG=outputs/kg
PATH_OUTPUT_DIR=outputs/kg

python -m food_atlas.kg.run_kg_expansion \
    $PATH_INPUT \
    --path-input-kg $PATH_INPUT_KG \
    --path-output-dir $PATH_OUTPUT_DIR \

python -m food_atlas.tests.test_kg \
    $PATH_OUTPUT_DIR
