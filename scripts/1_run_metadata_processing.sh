#!/bin/bash

PATH_INPUT=data/Lit2KG/2024_12_31/text_parser_predicted_gpt3.tsv
PATH_OUTPUT_DIR=outputs/kg

python -m food_atlas.kg.run_metadata_processing \
    $PATH_INPUT \
    $PATH_OUTPUT_DIR \
    --model-name gpt-3.5-ft
