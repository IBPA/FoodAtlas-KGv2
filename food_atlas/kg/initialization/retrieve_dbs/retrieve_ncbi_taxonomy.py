import pandas as pd
from Bio import Entrez

with open("food_atlas/kg/api_key.txt") as f:
    Entrez.email = f.readline().strip()
    Entrez.api_key = f.readline().strip()


if __name__ == '__main__':
    with open("outputs/kg/initialization/_ncbi_taxon_ids.txt", 'r') as f:
        ncbi_taxon_ids = f.read().split('\n')

    handle = Entrez.efetch(db='taxonomy', id=ncbi_taxon_ids, retmode='xml')
    records = Entrez.read(handle)
    data = pd.DataFrame(records)

    data.to_csv("outputs/kg/initialization/_ncbi_taxonomy.tsv", sep='\t')
