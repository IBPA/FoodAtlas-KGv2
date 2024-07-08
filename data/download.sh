#!/bin/bash

# # Download the data from USDA FDC.
# https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_foundation_food_csv_2023-10-26.zip
# https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_sr_legacy_food_csv_2018-04.zip

# # Download the data from FoodAtlas zip.

# # Download the data from FooDB.
# https://foodb.ca/public/system/downloads/foodb_2020_04_07_json.zip

echo 'Downloading CDNO.zip...'
curl -L https://ucdavis.box.com/shared/static/o0hlh3p27a7hsn936v5y2421xmsvja92.zip --output CDNO.zip

echo 'Downloading ChEBI.zip...'
curl -L  https://ucdavis.box.com/shared/static/u6fbnqwc40qjvzhl3wozvjqcvuzndwzf --output ChEBI.zip

echo 'Downloading FDC.zip...'
curl -L https://ucdavis.box.com/shared/static/lqab5272bsvch6qq3mkbejzryp9itvzn.zip --output FDC.zip

echo 'Downloading FoodOn.zip...'
curl -L https://ucdavis.box.com/shared/static/5qflnk36xeyvnmfokwzbiedbik1tpjdy.zip --output FoodOn.zip

echo 'Downloading Lit2KG.zip...'
curl -L https://ucdavis.box.com/shared/static/3jtujehzeefxrztyhz9pmksbg1w8cxja.zip --output Lit2KG.zip

echo 'Downloading MeSH.zip...'
curl -L https://ucdavis.box.com/shared/static/ec2wlws59y46rewh9y1hrht3tluehmdq.zip --output MeSH.zip

echo 'Downloading PubChem.zip...'
curl -L https://ucdavis.box.com/shared/static/2l8phdwcd6zjkm94urskqo33w76o35fv.zip --output PubChem.zip

echo 'Unzipping all files...'
unzip "*.zip"

echo 'Deleting zipped files...'
rm *.zip
