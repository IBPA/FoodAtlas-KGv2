import os
from ast import literal_eval
import json
from multiprocessing import Process, Queue

import pandas as pd
import requests
from tqdm import tqdm

tqdm.pandas()


def timeout(seconds, action=None):
    """Calls any function with timeout after 'seconds'.
       If a timeout occurs, 'action' will be returned or called if
       it is a function-like object.
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
def download_pug_view_ids(row, path_cache_dir="outputs/pug_view"):
    if pd.isna(row['pubchem_cid']):
        return

    path_save_dir = f"{path_cache_dir}/{row['pubchem_cid']}"
    if not os.path.exists(path_save_dir):
        os.makedirs(path_save_dir)

    if not os.path.exists(f"{path_save_dir}/other_identifiers.json"):
        print(row['pubchem_cid'])
        url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/"\
            f"{row['pubchem_cid']}/JSON?heading=Other+Identifiers"
        res = requests.get(url)
        # print(res.headers)

        try:
            text = res.json()
            with open(f"{path_save_dir}/other_identifiers.json", 'w') as f:
                json.dump(text, f)
        except ValueError:
            pass

    if not os.path.exists(f"{path_save_dir}/mesh_entry_terms.json"):
        print(row['pubchem_cid'])
        url = "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/"\
            f"{row['pubchem_cid']}/JSON?heading=MeSH+Entry+Terms"
        res = requests.get(url)

        try:
            text = res.json()
            with open(f"{path_save_dir}/mesh_entry_terms.json", 'w') as f:
                json.dump(text, f)
        except ValueError:
            pass


def parse_ids(row, path_cache_dir="food_atlas/hotfixes/data/pug_view"):
    """
    """
    if pd.isna(row['pubchem_cid']):
        return row

    # MeSH IDs.
    with open(f"{path_cache_dir}/{row['pubchem_cid']}/mesh_entry_terms.json") as f:
        text = json.load(f)
    if 'Record' in text:
        for ref in text['Record']['Reference']:
            if ref['SourceName'] == 'Medical Subject Headings (MeSH)':
                row['mesh_ids'] += [ref['SourceID']]

    # Other identifiers.
    with open(f"{path_cache_dir}/{row['pubchem_cid']}/other_identifiers.json") as f:
        text = json.load(f)
    if 'Record' in text:
        for ref in text['Record']['Reference']:
            if ref['SourceName'] == 'ChEBI':
                if not ref['URL'].startswith(
                    "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:"
                ):
                    # Check.
                    print(ref['URL'])
                    raise ValueError("ChEBI URL not uniform.")

                row['chebi_ids'] += [ref['URL'].split(
                    "https://www.ebi.ac.uk/chebi/searchId.do?chebiId="
                )[-1]]

            elif ref['SourceName'] == 'KEGG':
                row['kegg_ids'] += [ref['SourceID']]
            elif ref['SourceName'] == 'NCI Thesaurus (NCIt)':
                row['ncit_ids'] += [ref['SourceID']]

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

    entities_chemical = entities[entities['entity_type'] == 'chemical'].copy()
    entities_chemical['pubchem_cid'] = entities_chemical['external_ids'].apply(
        lambda x: x.get('pubchem_cid', None)
    ).astype('Int64')

    # # Download.
    # while True:
    #     try:
    #         entities_chemical.progress_apply(
    #             lambda row: download_pug_view_ids(row), axis=1
    #         )
    #         break
    #     except TimeoutError:
    #         print("TimeoutError: Retrying...")
    #         continue

    # for id_type in ['mesh_ids', 'chebi_ids', 'kegg_ids', 'ncit_ids']:
    #     entities_chemical[id_type] = [[] for _ in range(len(entities_chemical))]
    # entities_chemical = entities_chemical.progress_apply(
    #     lambda row: parse_ids(row), axis=1
    # )
    # entities_chemical.to_csv("entities_chemical_with_ids.csv")

    entities_chemical = pd.read_csv(
        "entities_chemical_with_ids.csv",
        converters={
            'chebi_ids': literal_eval,
        },
    )
    entities_chemical['chebi_ids'] = entities_chemical['chebi_ids'].apply(
        lambda x: int(x[0].split("CHEBI:")[-1]) if len(x) > 0 else None
    ).astype('Int64')

    chebi_chemicals = pd.read_csv(
        "data/ChEBI/compounds.tsv",
        sep='\t',
        encoding='unicode_escape',
    ).set_index('ID')
    chebi_relationships = pd.read_csv(
        "data/ChEBI/relation.tsv",
        sep='\t',
        encoding='unicode_escape',
    ).set_index(['FINAL_ID', 'TYPE', 'INIT_ID'])

    def get_parent(chebi_id):
        if pd.isna(chebi_id):
            return

        parents = chebi_relationships.loc[(chebi_id, 'is_a', slice(None))]

        if chebi_id not in ht_is_a:
            ht_is_a[chebi_id] = set()
        ht_is_a[chebi_id].update(parents.index.tolist())

        for parent in parents.index.tolist():
            if parent not in ht_is_a:
                get_parent(parent)

    ht_is_a = {}
    ht_is_a[24431] = set()
    entities_chemical['chebi_ids'].progress_apply(get_parent)
    chebi_chemicals.loc[ht_is_a.keys()].to_csv("chebi.tsv", sep='\t')

    ht_has_child = {}
    for k, v in ht_is_a.items():
        for parent in v:
            if parent not in ht_has_child:
                ht_has_child[parent] = set()
            ht_has_child[parent].add(k)

    # Get groups.
    def get_groups(level=2):
        queue = [24431] # level = 0
        while queue and level > 0:
            groups_new = []
            for current in queue:
                if current in ht_has_child:
                    groups_new.extend(ht_has_child[current])
                else:
                    groups_new.append(current)

            queue = list(set(groups_new))
            level -= 1

        return queue

    groups = get_groups()
    chebi_groups = chebi_chemicals.loc[groups]

    def map_group(chebi_id):
        if pd.isna(chebi_id):
            return 'Unclassified'

        groups_matched = []
        queue = [chebi_id]
        while queue:
            current = queue.pop(0)
            if current in groups:
                groups_matched.append(current)
            else:
                queue.extend(ht_is_a[current])

        return list(set(
            chebi_groups.loc[groups_matched]['NAME'].str.capitalize().tolist()
        ))

    entities_chemical['chebi_groups'] = entities_chemical['chebi_ids'].progress_apply(
        lambda x: map_group(x)
    )
    # entities_chemical.to_csv("entities_chemical_with_groups.csv")
    chemical_groups = entities_chemical.set_index('foodatlas_id')[['chebi_groups']]
    chemical_groups.to_csv("chemical_groups.tsv", sep='\t')
    print(chemical_groups)