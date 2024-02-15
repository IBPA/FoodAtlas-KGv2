import os
from ast import literal_eval

import numpy as np
import pandas as pd
from Bio import Entrez
from tqdm import tqdm
import click

from .utils import load_lookup_tables, constants

with open("food_atlas/kg/api_key.txt") as f:
    Entrez.email = f.readline().strip()
    Entrez.api_key = f.readline().strip()


@click.command()
@click.argument('path-output-dir', type=click.Path(exists=False))
def main(
    path_output_dir: str,
):
    # Step 1: Retrieve NCBI taxon IDs.
    if os.path.exists(f"{path_output_dir}/_query_entrez_taxonomy.tsv"):
        records_search = pd.read_csv(
            f"{path_output_dir}/_query_entrez_taxonomy.tsv",
            sep='\t',
            converters={
                'IdList': literal_eval,
                'WarningList': lambda x: literal_eval(x) if x else np.nan,
            },
        )
    else:
        with open(f"{path_output_dir}/_names_not_in_lut_food.txt", 'r') as f:
            names = [x for x in f.read().split('\n') if x]

        records_search = []
        for name in tqdm(names):
            handle_search = Entrez.esearch(db='taxonomy', term=f"\"{name}\"")
            record_search = Entrez.read(handle_search)
            records_search += [record_search]
        records_search = pd.DataFrame(records_search)
        records_search['search_term'] = names
        records_search.to_csv(
            f"{path_output_dir}/_query_entrez_taxonomy.tsv",
            sep='\t',
            index=False,
        )

    # This is done because the API will automatically change the queries to the closest
    # match. However, we only want to keep the exact matches to avoid any potential
    # issues.
    records_search_matched = records_search.query("WarningList.isna()")
    ncbi_taxon_ids = list(set([
        item for items in records_search_matched['IdList'].tolist() for item in items
    ]))

    # Skip the NCBI taxon IDs already in the knowledge graph.
    lut_food, _ = load_lookup_tables()
    ncbi_taxon_ids = [
        x for x in ncbi_taxon_ids
        if constants.get_lookup_key_by_id('ncbi_taxon_id', x) not in lut_food
    ]
    print(f"Retrieving {len(ncbi_taxon_ids)} new NCBI Taxonomy IDs.")

    handle_fetch = Entrez.efetch(db='taxonomy', id=ncbi_taxon_ids, retmode='xml')
    records_fetch = Entrez.read(handle_fetch)
    records_fetch = pd.DataFrame(records_fetch)
    records_fetch.to_csv(
        f"{path_output_dir}/_id_ncbi_taxonomy.tsv",
        sep='\t',
        index=False,
    )
    # records_not_matched = records_search.query("WarningList.notna()")
    # print(records_not_matched)



if __name__ == '__main__':
    main()
