# FoodAtlasDisease
CTD Disease Data reformatting for FoodAtlas

## Getting Started
Install
* Anaconda
* Python 3.10

## Please run the following command to install the required packages
```
git clone https://github.com/IBPA/FoodAtlasDisease.git
cd FoodAtlasDisease

conda create -n {your env} python=3.10
conda activate {your env}
pip install -r requirements.txt
```

## Run scripts in number order
<!---
## Download data from CTD and put in data folder
```
./scripts/download_ctd.sh
```

## Download data from FoodAtlas and put in data folder
### Current version of FoodAtlas data is 2.0

## Run read_ctd_chemdis.py
```
python -u -m fadisease.read_ctd_chemdis
```

## Run compare_factd_chem.py
```
python -u -m fadisease.compare_factd_chem
```
### Put FA_chemicals_pubchem_cid.csv into PubChem Identifier Exchange Service (https://pubchem.ncbi.nlm.nih.gov/idexchange/idexchange.cgi)
#### Select the following:
* Input ID List: CIDs
* Choose File: FA_chemicals_pubchem_cid.csv
* Operator Type: Same CID
* Output IDs: Registry IDs, Comparative Toxicogenomics Database (CTD) Chemicals
* Output Methods: Two Column file
* Compression: No compression
#### Rename output file as PubChemToCTD.txt and place in output folder
### Rerun compare_factd_chem.py
```
python -u -m fadisease.compare_factd_chem
```

## Run make_pmid_to_pmcid.py
```
python -u -m fadisease.make_pmid_to_pmcid <email>
```

## Run create_factd_data.py
```
python -u -m fadisease.create_factd_data
```
-->
