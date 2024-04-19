#!/bin/bash

PATH_OUTPUT_DIR=outputs/kg/_test/kg_new

python -m food_atlas.kg.run_kg_expansion \
    $PATH_OUTPUT_DIR \
    --path-kg outputs/kg/_test

python -m food_atlas.tests.test_kg \
    $PATH_OUTPUT_DIR
