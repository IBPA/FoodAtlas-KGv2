import os
from ast import literal_eval

import numpy as np
import pandas as pd
from Bio import Entrez
from tqdm import tqdm

from .utils import constants

if os.path.exists("food_atlas/kg/api_key.txt"):
    with open("food_atlas/kg/api_key.txt") as f:
        Entrez.email = f.readline().strip()
        Entrez.api_key = f.readline().strip()
else:
    print(
        "NCBI API key not found. Please create a file named 'api_key.txt' in the"
        "'food_atlas/kg' directory."
    )


def load_lookup_tables(
    path_dir: str = "outputs/kg"
) -> list[dict]:
    """Load the lookup tables.

    Args:
        path_dir (str, optional): The path to the directory having the lookup tables.
            Defaults to "outputs/kg".

    Returns:
        list[dict]: The list of lookup tables.

    """
    luts = []
    for suffix in ['food', 'chemical']:
        lut_df = pd.read_csv(
            f"{path_dir}/lookup_table_{suffix}.tsv",
            sep='\t',
            converters={
                'foodatlas_id': literal_eval,
                'name': str,
            },
        )
        lut = dict(zip(lut_df['name'], lut_df['foodatlas_id']))
        luts += [lut]

    return luts


def _search_ncbi_taxonomy(
    food_names: list[str],
    path_cache_dir: str,
):
    """
    """
    if os.path.exists(f"{path_cache_dir}/_cached_search_ncbi_taxonomy.tsv"):
        records_search = pd.read_csv(
            f"{path_cache_dir}/_cached_search_ncbi_taxonomy.tsv",
            sep='\t',
            converters={
                'IdList': literal_eval,
                'WarningList': lambda x: literal_eval(x) if x else np.nan,
            },
        )
        names_searched = records_search['search_term'].tolist()
        food_names = [x for x in food_names if x not in names_searched]
    else:
        records_search = pd.DataFrame()

    if food_names:
        records_search_new_rows = []
        for i, name in enumerate(tqdm(food_names)):
            handle_search = Entrez.esearch(db='taxonomy', term=f"\"{name}\"")
            record_search = Entrez.read(handle_search)
            records_search_new_rows += [record_search]

            # Cache the result per 100 queries to avoid network issues.
            if i % 100 == 99 or i == len(food_names) - 1:
                records_search_new = pd.DataFrame(records_search_new_rows)
                records_search_new['search_term'] \
                    = food_names[i - (len(records_search_new_rows) - 1):i + 1]
                records_search = pd.concat([records_search, records_search_new])
                records_search.to_csv(
                    f"{path_cache_dir}/_cached_search_ncbi_taxonomy.tsv",
                    sep='\t',
                    index=False,
                )
                records_search_new_rows = []

    return records_search


def _fetch_ncbi_taxonomy(
    ncbi_taxon_ids: list[int],
    path_cache_dir: str,
):
    """
    """
    # Skip the NCBI taxon IDs already in the knowledge graph.
    lut_food, _ = load_lookup_tables()
    ncbi_taxon_ids = [
        int(x) for x in ncbi_taxon_ids
        if constants.get_lookup_key_by_id('ncbi_taxon_id', x) not in lut_food
    ]

    if os.path.exists(f"{path_cache_dir}/_cached_fetch_ncbi_taxonomy.tsv"):
        records_fetch = pd.read_csv(
            f"{path_cache_dir}/_cached_fetch_ncbi_taxonomy.tsv",
            sep='\t',
            converters={
                'OtherNames': lambda x: literal_eval(x) if x != '' else None,
            },
        )
        ncbi_taxon_ids_fetched = records_fetch['TaxId'].tolist()
        ncbi_taxon_ids = [x for x in ncbi_taxon_ids if x not in ncbi_taxon_ids_fetched]

    if ncbi_taxon_ids:
        print(f"Retrieving {len(ncbi_taxon_ids)} new NCBI Taxonomy IDs.")
        handle_fetch = Entrez.efetch(db='taxonomy', id=ncbi_taxon_ids, retmode='xml')
        records_fetch_new = Entrez.read(handle_fetch)
        records_fetch_new = pd.DataFrame(records_fetch_new)
        records_fetch = pd.concat([records_fetch, records_fetch_new])
        records_fetch.to_csv(
            f"{path_cache_dir}/_cached_fetch_ncbi_taxonomy.tsv",
            sep='\t',
            index=False,
        )

    # Drop bacteria and viruses.
    records_fetch = records_fetch.query(
        "Division not in ['Bacteria', 'Viruses', 'Unassigned']"
    )

    return records_fetch


def query_ncbi_taxonomy(
    food_names: list[str],
    path_cache_dir: str,
) -> pd.DataFrame:
    """Query the NCBI taxonomy database.

    Args:
        food_names (list[str]): The list of food names.
        path_cache_dir (str): The path to the cache directory.

    Returns:
        pd.DataFrame: The retrieved NCBI taxonomy entries.

    """
    # Step 1: Retrieve NCBI taxon IDs.
    records_search = _search_ncbi_taxonomy(food_names, path_cache_dir)

    ncbi_taxon_ids = list(set([
        item
        for items in records_search['IdList'].tolist()
        for item in items
    ]))

    # Step 2: Retrieve NCBI entries.
    records_fetch = _fetch_ncbi_taxonomy(ncbi_taxon_ids, path_cache_dir)

    return records_fetch


def query_pubchem_compound(
    chemical_names: list[str],
    path_cache_dir: str,
) -> pd.DataFrame:
    """Query the PubChem compound database.

    Args:
        chemical_names (list[str]): The list of chemical names.
        path_cache_dir (str): The path to the cache directory.

    Returns:
        pd.DataFrame: The retrieved PubChem compound entries.

    """
    # Step 1: Retrieve PubChem compound IDs.
    if not os.path.exists(f"{path_cache_dir}/_cached_search_pubchem_compound.txt"):
        with open("chemical_names_for_pies.txt", 'w') as f:
            f.write('\n'.join(chemical_names))

        print(
            f"WARNING: The file `{path_cache_dir}/_cached_search_pubchem_compound.txt`"
            "could not be found. Please follow the instruction below:\n"
            "1. Go to https://pubchem.ncbi.nlm.nih.gov/idexchange/.\n"
            f"2. Upload the file `./chemical_names_for_pies.txt`.\n"
            "3. Make sure the following fields are correct and click `Submit Job`:\n"
            "   - `Input ID List`: `Synonyms`\n"
            "   - `Operator Type`: `Same CID`\n"
            "   - `Output IDs`: `CIDs`\n"
            "   - `Output Method`: `Two column file...`\n"
            "   - `Compression`: `No compression`\n"
            "4. Once the download is finished, rename the file and move to "
            f"`{path_cache_dir}/_cached_search_pubchem_compound.txt`.\n"
            "5. Click `Enter` to continue once all steps are finished..."
        )
        input()

    records_search = pd.read_csv(
        f"{path_cache_dir}/_cached_search_pubchem_compound.txt",
        sep='\t',
        header=None,
        names=['name', 'cid'],
        dtype={'cid': 'Int64'},
    )
    _, lut_chemical = load_lookup_tables()
    cids = records_search['cid'].dropna().unique().tolist()
    cids = [
        x for x in cids
        if constants.get_lookup_key_by_id('pubchem_cid', x) not in lut_chemical
    ]

    # Step 2: Retrieve PubChem compound entries.
    if os.path.exists(f"{path_cache_dir}/_cached_fetch_pubchem_compound.tsv"):
        records_fetch = pd.read_csv(
            f"{path_cache_dir}/_cached_fetch_pubchem_compound.tsv",
            sep='\t',
            converters={
                'SynonymList': literal_eval,
            },
        )
        cids_fetched = records_fetch['CID'].tolist()
        cids = [x for x in cids if x not in cids_fetched]
    else:
        records_fetch = pd.DataFrame()

    if cids:
        print(f"Retrieving {len(cids)} new PubChem CIDs.")
        cids_str = ','.join([str(x) for x in cids])
        handler = Entrez.esummary(db='pccompound', id=cids_str)
        records_fetch_new = Entrez.read(handler)
        records_fetch_new = pd.DataFrame(records_fetch_new)
        records_fetch = pd.concat([records_fetch, records_fetch_new])
        records_fetch.to_csv(
            f"{path_cache_dir}/_cached_fetch_pubchem_compound.tsv",
            sep='\t',
            index=False,
        )

    return records_fetch
