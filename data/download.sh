#!/bin/bash

echo 'Downloading CDNO.zip...'
curl -L https://ucdavis.box.com/shared/static/o0hlh3p27a7hsn936v5y2421xmsvja92.zip --output CDNO.zip

echo 'Downloading ChEBI.zip...'
curl -L  https://ucdavis.box.com/shared/static/u6fbnqwc40qjvzhl3wozvjqcvuzndwzf --output ChEBI.zip

echo 'Downloading CTD.zip...'
curl -L  https://ucdavis.box.com/shared/static/26ks5gtsyzixkz36h2abe6enseuj251u.zip --output CTD.zip

echo 'Downloading FDC.zip...'
curl -L https://ucdavis.box.com/shared/static/lqab5272bsvch6qq3mkbejzryp9itvzn.zip --output FDC.zip

echo 'Downloading FlavorDB.zip...'
curl -L https://ucdavis.box.com/shared/static/ucrnph5uwdkbdn55t7onamdfcf981zee.zip --output FlavorDB.zip

echo 'Downloading FoodOn.zip...'
curl -L https://ucdavis.box.com/shared/static/5qflnk36xeyvnmfokwzbiedbik1tpjdy.zip --output FoodOn.zip

echo 'Downloading HSDB.zip...'
curl -L https://ucdavis.box.com/shared/static/n71t5k1rs8xcwjojtzvehxng2euxvpm0.zip --output HSDB.zip

echo 'Downloading Lit2KG.zip...'
curl -L https://ucdavis.box.com/shared/static/3jtujehzeefxrztyhz9pmksbg1w8cxja.zip --output Lit2KG.zip

echo 'Downloading MeSH.zip...'
curl -L https://ucdavis.box.com/shared/static/ec2wlws59y46rewh9y1hrht3tluehmdq.zip --output MeSH.zip

echo 'Downloading PubChem.zip...'
curl -L https://ucdavis.box.com/shared/static/2l8phdwcd6zjkm94urskqo33w76o35fv.zip --output PubChem.zip

echo 'Downloading FlavorGraph.zip...'
curl -L https://ucdavis.box.com/s/yvhb00n2l5jzdr2uiz5jkehvpugw3z35 --output FlavorGraph.zip

echo 'Unzipping all files...'
unzip "*.zip"

echo 'Deleting zipped files...'
rm *.zip
