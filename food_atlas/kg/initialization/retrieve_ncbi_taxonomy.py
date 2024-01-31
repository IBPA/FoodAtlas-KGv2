import pandas as pd
from Bio import Entrez

Entrez.email = "fzli@ucdavis.edu"
Entrez.api_key = "7631dcfb0fa03378db8adae415c8fee60309"


if __name__ == '__main__':
    with open("outputs/kg/initialization/ncbi_taxon_ids.txt", 'r') as f:
        ncbi_taxon_ids = f.read().split('\n')

    handle = Entrez.efetch(db='taxonomy', id=ncbi_taxon_ids, retmode='xml')
    records = Entrez.read(handle)
    data = pd.DataFrame(records)

    data.to_csv("outputs/kg/initialization/ncbi_taxonomy.tsv", sep='\t')
