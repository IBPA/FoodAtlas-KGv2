# Description: Download the OpenFoodTox data from the Zenodo repository 
wget https://zenodo.org/records/8120114/files/ReferencePoints_KJ_2023.xlsx -P data/
wget https://zenodo.org/records/8120114/files/SubstanceCharacterisation_KJ_2023.xlsx -P data/

# Description: Download the The Toxicity Value Database from Clowder
# Download ToxRefDB data
wget https://clowder.edap-cluster.com/files/645a5c53e4b08a6b39438c7b/blob/ -O data/toxval_all_res_toxval_v94_ToxRefDB.xlsx
