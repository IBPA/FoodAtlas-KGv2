#!/bin/bash

PATH_INPUT=data/Lit2KG/text_parser_predicted_2024_02_25.pkl
PATH_OUTPUT_DIR=outputs/kg/20240613

python -m food_atlas.kg.run_metadata_processing \
    $PATH_INPUT \
    $PATH_OUTPUT_DIR
