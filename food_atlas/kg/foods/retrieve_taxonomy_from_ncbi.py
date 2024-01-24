import pandas as pd
from Bio import Entrez

Entrez.email = "fzli@ucdavis.edu"
Entrez.api_key = "7631dcfb0fa03378db8adae415c8fee60309"


if __name__ == '__main__':
    data_fdc = pd.read_csv("outputs/kg/foods/fdc.csv")
    data_foodb = pd.read_csv("outputs/kg/foods/foodb.csv")
    data = pd.concat([data_fdc, data_foodb], axis=0)
    data = data.drop(columns=['Unnamed: 0'])
    data = data[[
        'orig_name',
        'scientific_name',
        'ncbi_taxon_id',
        'itis_id',
        'fdc_id',
        'foodb_id',
        'food_group',
        'food_subgroup',
    ]]
    data = data.astype({
        'ncbi_taxon_id': 'Int64'
    })

    # Get scientific name from NCBI
    ncbi_taxon_ids = data['ncbi_taxon_id'].dropna().unique().tolist()
    handle = Entrez.efetch(db='taxonomy', id=ncbi_taxon_ids, retmode='xml')
    records = Entrez.read(handle)
    data = pd.DataFrame(records)

    data.to_csv("outputs/kg/foods/ncbi_taxonomy.csv")
