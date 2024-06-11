#!/bin/bash

# # Download the data from USDA FDC.
# https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_foundation_food_csv_2024-04-18.zip
# https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_sr_legacy_food_csv_2018-04.zip

# # Download the data from FoodAtlas zip.

# # Download the data from FooDB.
# https://foodb.ca/public/system/downloads/foodb_2020_04_07_json.zip

# ChEBI.
echo 'Downloading ChEBI.zip...'
curl -L  https://ucdavis.box.com/shared/static/u6fbnqwc40qjvzhl3wozvjqcvuzndwzf --output ChEBI.zip

echo 'Downloading FDC.zip...'
curl -L https://ucdavis.box.com/shared/static/lqab5272bsvch6qq3mkbejzryp9itvzn.zip --output FDC.zip

echo 'Downloading FoodOn.zip...'
curl -L https://ucdavis.box.com/shared/static/5qflnk36xeyvnmfokwzbiedbik1tpjdy.zip --output FoodOn.zip

echo 'Downloading Lit2KG.zip...'
curl -L https://ucdavis.box.com/shared/static/3jtujehzeefxrztyhz9pmksbg1w8cxja.zip --output Lit2KG.zip

echo 'Unzipping all files...'
unzip "*.zip"

echo 'Deleting zipped files...'
rm *.zip
