import os
from ast import literal_eval
import json
from multiprocessing import Process, Queue

import pandas as pd
import requests
from tqdm import tqdm

tqdm.pandas()


def timeout(seconds, action=None):
    """
    Calls any function with timeout after 'seconds'. If a timeout occurs, 'action' will
    be returned or called if it is a function-like object.

    """
    def handler(queue, func, args, kwargs):
        queue.put(func(*args, **kwargs))

    def decorator(func):

        def wraps(*args, **kwargs):
            q = Queue()
            p = Process(target=handler, args=(q, func, args, kwargs))
            p.start()
            p.join(timeout=seconds)
            if p.is_alive():
                p.terminate()
                p.join()
                if hasattr(action, '__call__'):
                    return action()
                else:
                    return action
            else:
                return q.get()

        return wraps

    return decorator


@timeout(3)
def download_pug_view_ids(cid, path_cache_dir="food_atlas/hotfixes/data/pug_view"):
    if pd.isna(cid):
        return

    path_save_dir = f"{path_cache_dir}/{cid}"
    if not os.path.exists(path_save_dir):
        os.makedirs(path_save_dir)

    if not os.path.exists(f"{path_save_dir}/other_identifiers.json"):
        print(cid)
        url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/"\
            f"{cid}/JSON?heading=Other+Identifiers"
        res = requests.get(url)
        print(res.headers)

        try:
            text = res.json()
            with open(f"{path_save_dir}/other_identifiers.json", 'w') as f:
                json.dump(text, f)
        except ValueError:
            pass

    if not os.path.exists(f"{path_save_dir}/mesh_entry_terms.json"):
        print(cid)
        url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/"\
            f"{cid}/JSON?heading=MeSH+Entry+Terms"
        res = requests.get(url)

        try:
            text = res.json()
            with open(f"{path_save_dir}/mesh_entry_terms.json", 'w') as f:
                json.dump(text, f)
        except ValueError:
            pass


def get_chebi_id_from_cid(cid, path_cache_dir="food_atlas/hotfixes/data/pug_view"):
    if not os.path.exists(f"{path_cache_dir}/{cid}"):
        download_pug_view_ids(cid)

    # Other identifiers.
    with open(f"{path_cache_dir}/{cid}/other_identifiers.json") as f:
        text = json.load(f)

    chebi_ids = []
    if 'Record' in text:
        for ref in text['Record']['Reference']:
            if ref['SourceName'] == 'ChEBI':
                if not ref['URL'].startswith(
                    "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:"
                ):
                    # Check.
                    print(ref['URL'])
                    raise ValueError("ChEBI URL not uniform.")

                chebi_ids += [int(ref['URL'].split(
                    "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:"
                )[-1])]

    assert len(chebi_ids) <= 1

    return chebi_ids


def get_map_name_to_chebi_ids():
    names_chebi = pd.read_csv(
        "data/ChEBI/names.tsv",
        sep='\t',
    )
    names_chebi_official = pd.read_csv(
        "data/ChEBI/compounds.tsv",
        sep='\t',
        encoding='unicode_escape',
    ).astype({'PARENT_ID': 'Int64'})

    # Check ChEBI synonyms vs. PubChem sysnonyms.
    id_to_names = {}
    names_chebi.groupby('COMPOUND_ID').progress_apply(
        lambda group: id_to_names.update({
            group.name: list(set(
                [x.strip().lower() for x in group['NAME'].dropna().tolist()]
            ))
        })
    )

    def add_id_to_names(row):
        if pd.notna(row['PARENT_ID']):
            return

        if row['ID'] not in id_to_names:
            id_to_names[row['ID']] = []
        id_to_names[row['ID']] += [row['NAME'].strip().lower()]
        id_to_names[row['ID']] = list(set(id_to_names[row['ID']]))

    names_chebi_official.progress_apply(add_id_to_names, axis=1)

    name_to_ids = {}
    for chebi_id, names in id_to_names.items():
        for name in names:
            if name not in name_to_ids:
                name_to_ids[name] = []
            name_to_ids[name] += [chebi_id]

    return name_to_ids


def add_chebi_to_chemical_entity(row):
    global name_to_ids

    if row['entity_type'] != 'chemical':
        return row

    # From PubChem CID.
    if 'pubchem_cid' in row['external_ids']:
        chebi_ids = get_chebi_id_from_cid(row['external_ids']['pubchem_cid'])
    elif '_placeholder_to' not in row['external_ids']:
        # From name.
        chebi_ids = set()
        for name in row['synonyms']:
            if name in name_to_ids:
                chebi_ids.update(name_to_ids[name])
        chebi_ids = list(chebi_ids)
    else:
        chebi_ids = []

    if chebi_ids:
        row['external_ids']['chebi_ids'] = chebi_ids

    return row


if __name__ == '__main__':
    entities = pd.read_csv(
        "outputs/kg/entities.tsv",
        sep='\t',
        converters={
            'synonyms': literal_eval,
            'external_ids': literal_eval,
        },
    ).set_index('foodatlas_id')

    name_to_ids = get_map_name_to_chebi_ids()

    entities = entities.apply(add_chebi_to_chemical_entity, axis=1)
    entities.to_csv("outputs/kg/entities.tsv", sep='\t')
